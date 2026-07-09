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
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  /jobs/gold_etat_courant.py
```

⚠️ **Contrainte 2 cœurs (1 seul worker)**, cf. `README_role_C.md` : si
Bronze/Silver/Gold 1 tournent déjà, ce job restera `WAITING` faute de cœurs
libres. Le lancer par vagues (arrêter un job en cours, lancer celui-ci,
capturer la preuve), comme fait pour Gold 1.

⚠️ Le `--conf spark.hadoop.fs.permissions.umask-mode=000` est **obligatoire**
ici — voir Incident 1 ci-dessous : le problème touche cette fois l'écriture
de la table Gold elle-même (pas seulement un state store stateful).

## Preuves (capturées le 09/07/2026, deux vagues de test 400 puis +300
événements injectés via `kafka-console-producer`, cf. `README_role_C.md`
Incident 3 pour le contournement Windows)

### Table Gold 2 peuplée et mise à jour (Postgres)
```sql
SELECT capteur_id, derniere_valeur, dernier_statut_anomalie, derniere_maj
FROM gold.etat_courant_capteur ORDER BY capteur_id;
```
```
 nb_lignes | derniere_maj_la_plus_recente
-----------+------------------------------
        24 | 2026-07-09 15:33:45.991
```
24 lignes après la 1ère vague **et** après la 2ᵉ (cardinalité fixe — à la
différence de Gold 1 qui accumule des fenêtres). `derniere_maj` avance bien
de la vague 1 (15:03:43) à la vague 2 (15:33:45) et les valeurs changent
(ex. `cpt-001` : 41.6 → 57.7), confirmant que le `MERGE INTO` met à jour les
lignes existantes plutôt que d'accumuler.

### `DESCRIBE HISTORY` (preuve obligatoire)
```bash
docker exec capteurs-spark-master /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /jobs/describe_gold_etat.py
```
```
version | timestamp              | operation | operationParameters
2       | 2026-07-09 21:55:19.911| MERGE     | matchedPredicates=[update: derniere_maj >= ...], notMatchedPredicates=[insert]
1       | 2026-07-09 15:58:34.627| MERGE     | matchedPredicates=[update: derniere_maj >= ...], notMatchedPredicates=[insert]
0       | 2026-07-09 15:57:05.409| WRITE     | mode=Overwrite (création table vide au démarrage du job)
COUNT= 24
```
Version 1 = insert des 24 capteurs (vague 1, table vide au départ) ; version 2
= update des 24 mêmes capteurs (vague 2) — confirme le comportement upsert
réel côté Delta, avec la garde de fraîcheur (`derniere_maj >= ...`) visible
dans `operationParameters`, en plus de l'upsert Postgres (`ON CONFLICT
(capteur_id) DO UPDATE ... WHERE EXCLUDED.derniere_maj >= ...`).

### Spark UI
Capture `localhost:8080` (ou `8081` en local si conflit de port avec un autre
TP, cf. Incident 2) montrant `gold_etat_courant_capteur` en `RUNNING`.

### Power BI
Voir section suivante — capture du dashboard avec KPIs à jour, *à faire*.

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

## Incidents

### Incident 1 — `Permission denied` à l'écriture de la table Gold elle-même

**Problème** : au tout premier lancement, `gold_etat_courant.py` plantait
dès le premier micro-batch avec :
```
java.io.FileNotFoundException: /lakehouse/gold/etat_courant_capteur/part-00000-....snappy.parquet (Permission denied)
```
alors que le job n'a **aucun** opérateur stateful (pas de `groupBy`/watermark
sur le stream) — je pensais donc, à tort, être à l'abri du problème documenté
par C (Incident 1 de `README_role_C.md`, spécifique au state store).

**Diagnostic** : le même mécanisme s'applique en fait à **toute** création de
répertoire Delta, pas seulement au state store. `ensure_gold_table()` crée le
répertoire `/lakehouse/gold/etat_courant_capteur` (mode `755`, propriétaire
`root`) depuis le driver (conteneur `spark-master`, process `root`), mais
l'écriture effective des fichiers Parquet lors du `MERGE INTO` est faite par
l'**executor** (conteneur `spark-worker-1`, utilisateur différent) — qui n'a
pas le droit d'écrire dans un dossier `root:root 755`.

**Résolution** : ajout de `--conf spark.hadoop.fs.permissions.umask-mode=000`
au `spark-submit`, comme pour Silver/Gold 1. Après nettoyage du répertoire
Gold partiellement créé (`rm -rf /lakehouse/gold/etat_courant_capteur` et
`.../checkpoints/gold_etat_courant_capteur`) et relance avec cette conf, le
job a démarré sans erreur.

**Leçon** : la conf `umask-mode=000` doit être ajoutée par défaut à **tout**
job Spark qui écrit une nouvelle table Delta dans ce projet (bind mount
Docker + driver root / executor non-root), que le job soit stateful ou non
au sens Structured Streaming — pas seulement pour le state store comme
documenté initialement par C.

### Incident 2 — Conflit de port 8080 avec un autre TP (environnement Windows)

**Problème** : `docker compose up -d` a échoué sur `Bind for 0.0.0.0:8080
failed: port is already allocated` — un conteneur Airflow d'un autre TP
(`lakehouse-fx-tp-airflow-webserver-1`) tournait déjà sur ce port depuis
plusieurs jours.

**Résolution** : remapping local du port du Spark Master UI vers `8081`
(`127.0.0.1:8081:8080` dans `docker-compose.yml`) plutôt que d'arrêter le
conteneur de l'autre TP. Pas de changement fonctionnel côté pipeline, juste
un port d'accès différent pour l'UI.

**Leçon** : même leçon que l'Incident 1 du rôle A (`docker ps -a` avant tout
premier lancement) — particulièrement vrai sur une machine qui fait tourner
plusieurs projets Docker en parallèle.

### Incident 3 — Kill du job via `docker exec -d` / `TaskStop` insuffisant

**Problème** : arrêter le processus qui a lancé `docker exec -d ... spark-submit`
(ou tuer le process client local) ne stoppe **pas** le driver Spark à
l'intérieur du conteneur — l'application restait `RUNNING` côté Spark
Master UI même après l'arrêt du client.

**Résolution** : tuer explicitement le process driver *dans* le conteneur :
```bash
docker exec capteurs-spark-master bash -c "kill \$(pgrep -f SparkSubmit)"
```

**Leçon** : `docker exec -d` détache complètement la commande du process
client — un `kill` côté client ou côté outil d'orchestration n'atteint jamais
le process réellement exécuté dans le conteneur.
