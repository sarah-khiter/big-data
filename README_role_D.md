# Rôle D — Gold état courant & Power BI

Ce document explique le livrable D : état courant par capteur (2ᵉ usage de
`MERGE INTO`) et la restitution Power BI.

> ⚠️ Ceci **n'est pas le README final du projet**. Doc de travail interne au
> rôle D — le `README.md` à la racine reste le seul livrable évalué.

## Ce qui a été livré

```
big-data/
└── jobs/
    ├── gold_etat_courant.py    # Silver (stream) -> dernière valeur/statut par capteur -> foreachBatch(MERGE INTO + upsert Postgres)
    └── describe_gold_etat.py   # utilitaire de preuve (DESCRIBE HISTORY + contenu table)
```

## Logique Gold 2 (`jobs/gold_etat_courant.py`)

1. Lecture stream Silver (`file:///lakehouse/silver/mesures`)
2. `foreachBatch` (pas de `groupBy`/`window` streaming — donc **pas
   d'opérateur stateful Spark**, contrairement à Silver/Gold 1) :
   - à l'intérieur du micro-batch, `Window.partitionBy("capteur_id")
     .orderBy(event_ts desc)` + `row_number() == 1` pour ne garder que la
     mesure la plus récente par capteur (un micro-batch peut contenir
     plusieurs mesures du même capteur)
   - `DeltaTable.merge(...)` sur `capteur_id`, avec
     `whenMatchedUpdateAll(condition="s.derniere_maj >= t.derniere_maj")` —
     protège contre un micro-batch en retard qui écraserait un état plus
     récent
   - upsert Postgres `gold.etat_courant_capteur` via `psycopg2`
     `ON CONFLICT (capteur_id) DO UPDATE ... WHERE EXCLUDED.derniere_maj >=
     gold.etat_courant_capteur.derniere_maj` (même garde-fou côté SQL)

## Prérequis (rôles A + B + C déjà faits)

```bash
docker compose up -d --build
docker compose ps
docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql
python scripts/load_referentiel.py
chmod -R 777 lakehouse
```
Bronze puis Silver doivent avoir tourné au moins une fois (Gold 2 lit le
stream Silver).

## Lancer Gold 2

```bash
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  /jobs/gold_etat_courant.py
```
⚠️ Le `--conf spark.hadoop.fs.permissions.umask-mode=000` est nécessaire :
même sans opérateur stateful *streaming*, la toute première écriture de la
table Delta vide (`ensure_gold_table`) est faite par le **driver** (root) —
sans cette option, l'**executor** (uid `spark`) ne peut pas relire ces
fichiers lors du premier `MERGE INTO` (`Permission denied`). Voir Incident 1
ci-dessous.

## Preuves (capturées le 10/07/2026)

### Table peuplée, une ligne par capteur
```sql
SELECT capteur_id, machine_id, site_id, derniere_valeur, derniere_unite,
       dernier_statut_anomalie, dernier_batterie_pct, derniere_maj
FROM gold.etat_courant_capteur ORDER BY capteur_id;
```
→ **24 lignes** (= 24 capteurs du référentiel), une seule par `capteur_id`.

### Mise à jour dans le temps (2ᵉ vague d'événements)
Avant 2ᵉ vague : `derniere_maj = 2026-07-09 13:39:46`
Après 2ᵉ vague : `derniere_maj = 2026-07-10 08:16:00` (même `capteur_id`)
→ la ligne est bien **mise à jour en place** (UPDATE), pas dupliquée.

### `DESCRIBE HISTORY` (preuve obligatoire)
```
version | operation | operationParameters
2       | MERGE     | matchedPredicates=[{"predicate":"derniere_maj >= t.derniere_maj","actionType":"update"}], notMatchedPredicates=[insert]
1       | MERGE     | matchedPredicates=[{"predicate":"derniere_maj >= t.derniere_maj","actionType":"update"}], notMatchedPredicates=[insert]
0       | WRITE     | mode=Overwrite (création table vide)
```
Le prédicat conditionnel `derniere_maj >= t.derniere_maj` apparaît bien dans
les métadonnées de version — preuve que le garde-fou anti-régression est
actif, pas juste écrit dans le code.

## Power BI Desktop — connexion et modèle

> ⚠️ **Power BI Desktop n'est pas installé sur la machine utilisée pour
> développer/tester ce job** (Windows, vérifié via `Get-StartApps` — aucune
> app Power BI trouvée). Cette section documente la marche à suivre **pas à
> pas**, prête à exécuter par la première personne de l'équipe qui l'a
> installé, mais **n'a pas pu être exécutée ni capturée ici**. C'est le seul
> livrable du projet qui reste réellement `TODO` à ce stade.

### 1. Connexion
`Obtenir les données` → `Base de données` → `PostgreSQL` → :
- Serveur : `localhost:5432` (ou `127.0.0.1:5432`)
- Base de données : `capteurs`
- Mode : **DirectQuery** (les données Gold sont mises à jour en continu par
  les jobs Spark ; DirectQuery évite d'avoir à rafraîchir manuellement)
- Identifiants : utilisateur `capteurs`, mot de passe `capteurs`

Tables à importer : `gold.dim_capteur`, `gold.dim_machine`, `gold.dim_site`,
`gold.agg_fenetre_machine`, `gold.etat_courant_capteur`.

### 2. Modèle (relations faits/dimensions)
```
dim_site (site_id) ──1───N── dim_machine (site_id, machine_id)
dim_machine (machine_id) ──1───N── agg_fenetre_machine (machine_id)
dim_machine (machine_id) ──1───N── etat_courant_capteur (machine_id)   [redondant, dénormalisé pour filtrage direct]
dim_capteur (capteur_id) ──1───1── etat_courant_capteur (capteur_id)
```
Dans Power BI : onglet **Modèle**, glisser-déposer pour créer les relations
ci-dessus (cardinalité 1-N sauf `dim_capteur`↔`etat_courant_capteur` qui est
1-1), sens de filtrage **unique** (dimension → fait).

### 3. KPIs à construire (mesures DAX)

**Anomalies par machine/site**
```dax
Nb Anomalies (fenêtre) = SUM(agg_fenetre_machine[nb_anomalies])
```
Visuel : histogramme empilé par `dim_machine[type_machine]` ou
`dim_site[nom]`, axe = `Nb Anomalies (fenêtre)`.

**Valeur moyenne glissante**
```dax
Valeur Moyenne = AVERAGE(agg_fenetre_machine[valeur_moyenne])
```
Visuel : courbe temporelle, axe X = `agg_fenetre_machine[window_start]`,
axe Y = `Valeur Moyenne`, légende = `machine_id`.

**Batterie faible**
```dax
Nb Capteurs Batterie Faible =
    CALCULATE(
        COUNTROWS(etat_courant_capteur),
        etat_courant_capteur[dernier_batterie_pct] < 20
    )
```
Visuel : carte (KPI tile) + table détail filtrée sur ce seuil.

**Dernier statut par capteur**
Visuel : table/matrice sur `etat_courant_capteur` (`capteur_id`,
`derniere_valeur`, `dernier_statut_anomalie`, `derniere_maj`), avec mise en
forme conditionnelle (rouge si `dernier_statut_anomalie = true`).

### 4. Preuve attendue (à fournir par la personne qui exécute cette section)
- Capture du dashboard avec les 4 KPIs ci-dessus, données à jour
- Capture du modèle (onglet Power BI **Modèle**) montrant les relations

## Incident 1 (résolu) — même permission `mkdir`/lecture que Silver/Gold 1

**Problème** : `gold_etat_courant.py` plantait au tout premier micro-batch
avec `java.io.FileNotFoundException: ... (Permission denied)` en lisant les
fichiers Parquet de la table Delta vide qu'il venait de créer.

**Cause** : identique à l'Incident 1 du rôle C (voir `README_role_C.md`) —
la table vide est créée par le **driver** (root, conteneur `spark-master`),
mais lue/mergée par l'**executor** (`spark`, conteneur `spark-worker-1`).
Nouveau ici : ce n'est **pas** un opérateur stateful (Gold 2 n'a ni
`watermark` ni `groupBy` sur le stream), donc ce n'est pas le state store qui
pose problème mais bien les fichiers de données Delta eux-mêmes écrits par
`ensure_gold_table()` avant le démarrage du stream.

**Résolution** : même fix, `--conf
spark.hadoop.fs.permissions.umask-mode=000` au `spark-submit`.

**Leçon consolidée pour toute future table Gold créée vide avant un
`foreachBatch`** : ce `--conf` doit être systématique dès qu'un job écrit
depuis le driver puis relit/merge depuis l'executor — indépendamment de la
présence ou non d'un state store streaming.

## Incident 2 (résolu, à signaler à A) — Kafka n'a pas de volume persistant

**Problème** : après un redémarrage de Docker Desktop (VM relancée), tous
les conteneurs du projet ont été recréés. Kafka (`capteurs-kafka`) est
reparti avec un topic `mesures` **vide** (offset 0), alors que le checkpoint
Bronze pointait encore vers l'ancien offset 700. Résultat :
`WARN KafkaMicroBatchStream: Partition mesures-0's offset was changed from
700 to 200, some data may have been missed` puis le job Bronze restait bloqué
sans consommer les nouveaux messages.

**Cause racine** : `docker-compose.yml` (rôle A) ne déclare **aucun volume**
pour le service `kafka` — contrairement à `postgres` (`pgdata`, named
volume) et à Spark/`lakehouse` (bind mount). Toute recréation du conteneur
Kafka perd donc l'intégralité des topics.

**Contournement appliqué** : suppression du checkpoint Bronze
(`rm -rf lakehouse/checkpoints/bronze_mesures`) pour forcer une relecture
propre depuis `earliest` sur le topic (re-vidé) — acceptable ici car Bronze
est un append pur (les doublons éventuels sont filtrés en Silver par
`event_id`), mais **ce n'est pas une vraie solution** : en production, perdre
tout Kafka au moindre restart de conteneur est inacceptable.

**À signaler à A / traiter collectivement** : ajouter un volume nommé au
service `kafka` dans `docker-compose.yml` (le mode KRaft stocke ses logs dans
`/var/lib/kafka/data` par défaut sur l'image `apache/kafka`), pour que les
topics survivent à un `docker compose down`/`up` ou à un restart de Docker
Desktop.

## À transmettre pour la soutenance

- Toutes les tables Gold sont peuplées et vérifiables : `gold.dim_*`,
  `gold.agg_fenetre_machine`, `gold.etat_courant_capteur`
- 2 usages distincts de `MERGE INTO` (Gold 1 par C, Gold 2 par D), chacun
  avec sa preuve `DESCRIBE HISTORY`
- Seul point réellement manquant : construction effective du dashboard Power
  BI (nécessite Power BI Desktop, non disponible sur ce poste) — la
  connexion, le modèle et les mesures DAX sont documentés ci-dessus, prêts à
  être suivis pas à pas
