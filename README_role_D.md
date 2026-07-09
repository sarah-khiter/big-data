# Rôle D — Gold état courant & Power BI

Ce document explique le livrable D : second usage de `MERGE INTO` (Gold 2,
état courant par capteur) et restitution BI (Power BI Desktop).

> ⚠️ Ceci **n'est pas le README final du projet**. Doc de travail interne au
> rôle D — le `README.md` à la racine reste le seul livrable évalué.

## Ce qui a été livré

```
big-data/
├── jobs/
│   ├── gold_etat_courant.py    # Silver (stream) -> dernière valeur/statut par capteur -> foreachBatch(MERGE INTO + upsert Postgres)
│   └── describe_gold_etat.py   # utilitaire de preuve (DESCRIBE HISTORY + contenu table)
└── lakehouse/                  # généré à l'exécution
    ├── gold/etat_courant_capteur/            # table Delta Gold 2
    └── checkpoints/gold_etat_courant_capteur/
```

## Logique Gold 2 (`jobs/gold_etat_courant.py`)

1. Lecture stream **Silver** (pas Bronze, pour bénéficier de `is_anomalie`
   déjà calculé par C).
2. **Pas de `groupBy`/watermark sur le stream lui-même.** La déduplication
   "dernière valeur par capteur" se fait dans le DataFrame *statique* reçu
   par `foreachBatch`, via une fonction fenêtre
   (`row_number()` partitionné par `capteur_id`, ordonné par `event_ts`
   desc, on garde `rn == 1`). L'état "dernière valeur connue" est de toute
   façon porté par la table cible (Delta + Postgres), pas par un state
   store Spark — contrairement à Gold 1 (rôle C), ce job n'est donc pas
   stateful au sens Structured Streaming.
3. `foreachBatch` :
   - `DeltaTable.forPath(...).merge(...)` sur `capteur_id` →
     `whenMatchedUpdateAll(condition="s.derniere_maj >= t.derniere_maj")` +
     `whenNotMatchedInsertAll` (condition de garde pour ne jamais écraser une
     valeur plus récente par une plus ancienne si deux micro-batchs arrivent
     dans le désordre).
   - upsert Postgres `gold.etat_courant_capteur` via `psycopg2`
     `ON CONFLICT (capteur_id) DO UPDATE ... WHERE EXCLUDED.derniere_maj >= ...`
     (même garde de fraîcheur côté Postgres).

## Prérequis (rôles A/B/C déjà faits)

```bash
docker compose up -d
docker compose ps   # 4 conteneurs Up

docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql
python3 -m venv ~/venv-projet && source ~/venv-projet/bin/activate
pip install psycopg2-binary
python3 scripts/load_referentiel.py
```
Bronze et Silver doivent avoir déjà tourné au moins une fois (Gold 2 lit le
stream Silver).

## Étape 1 — Lancer Gold 2

```bash
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /jobs/gold_etat_courant.py
```

⚠️ **Contrainte 2 cœurs (1 seul worker)**, cf. `README_role_C.md` : si
Bronze/Silver/Gold 1 tournent déjà, ce job restera `WAITING` faute de cœurs
libres. Le lancer par vagues (arrêter un job en cours, lancer celui-ci,
capturer la preuve), comme fait pour Gold 1.

Si l'erreur `mkdir ... state/... failed` (Incident 1, rôle C) apparaît malgré
l'absence de `groupBy` streaming, ajouter par précaution :
```bash
  --conf spark.hadoop.fs.permissions.umask-mode=000
```

## Preuves — *à capturer à l'exécution*

### Table Gold 2 peuplée et mise à jour (Postgres)
```sql
SELECT capteur_id, derniere_valeur, dernier_statut_anomalie, derniere_maj
FROM gold.etat_courant_capteur ORDER BY capteur_id;
```
Attendu : 24 lignes (une par capteur, cardinalité fixe — contrairement à
Gold 1 qui accumule des fenêtres dans le temps), `derniere_maj` qui avance à
chaque nouvelle vague d'injection.

### `DESCRIBE HISTORY` (preuve obligatoire)
```bash
docker exec capteurs-spark-master /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /jobs/describe_gold_etat.py
```
Attendu : plusieurs versions `MERGE`, une par vague de données non vide.

### Spark UI
Capture `localhost:8080` montrant `gold_etat_courant_capteur` en `RUNNING`.

### Power BI
Voir section suivante — capture du dashboard avec KPIs à jour.

## Power BI Desktop

1. **Connexion** : `Obtenir les données` → `PostgreSQL` → serveur
   `localhost:5432`, base `capteurs`, mode **DirectQuery** (les données
   Postgres évoluent en continu ; Import figerait un snapshot).
   Identifiants : type "Base de données", `capteurs` / `capteurs`.
2. **Tables chargées** : `gold.dim_capteur`, `gold.dim_machine`,
   `gold.dim_site`, `gold.agg_fenetre_machine`, `gold.etat_courant_capteur`.
3. **Modèle** (relations créées manuellement, vue "Modèle") :
   - `dim_site.site_id` → `dim_machine.site_id` (1:*)
   - `dim_machine.machine_id` → `agg_fenetre_machine.machine_id` (1:*)
   - `dim_machine.machine_id` → `etat_courant_capteur.machine_id` (1:*)
   - `dim_capteur.capteur_id` → `etat_courant_capteur.capteur_id` (1:1)
   - `dim_site.site_id` → `etat_courant_capteur.site_id` (1:*)
4. **KPIs** :
   - Anomalies par machine/site : `SUM(nb_anomalies)` (Gold 1) par machine,
     filtrable par site
   - Valeur moyenne glissante : courbe `window_start` × `valeur_moyenne`
     (Gold 1) par machine
   - Batterie faible : table/carte filtrée `dernier_batterie_pct < 20`
     (Gold 2)
   - Dernier statut par capteur : table `etat_courant_capteur` avec mise en
     forme conditionnelle sur `dernier_statut_anomalie`

## Incidents — *à documenter au fil de l'eau*

*(compléter ici tout problème réel rencontré lors du lancement du job ou de
la connexion Power BI, sur le modèle des Incidents 1-3 de `README_role_C.md`)*
