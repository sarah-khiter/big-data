# Rôle C — Silver & Gold agrégation temporelle

Ce document explique le livrable C : nettoyage Silver (dédoublonnage +
détection d'anomalie) et premier usage de `MERGE INTO` (Gold 1, agrégation
fenêtre glissante par machine).

> ⚠️ Ceci **n'est pas le README final du projet**. Doc de travail interne au
> rôle C — le `README.md` à la racine reste le seul livrable évalué.

## Ce qui a été livré

```
big-data/
├── jobs/
│   ├── silver.py               # Bronze (stream) -> dédup + is_anomalie -> Delta Silver (append)
│   ├── gold_agg_fenetre.py     # Silver (stream) -> fenêtre glissante -> foreachBatch(MERGE INTO + upsert Postgres)
│   ├── count_silver.py         # utilitaire de preuve (count + échantillon)
│   └── describe_gold_agg.py    # utilitaire de preuve (DESCRIBE HISTORY + contenu table)
└── lakehouse/                  # généré à l'exécution
    ├── silver/mesures/                       # table Delta Silver (partition event_date)
    ├── gold/agg_fenetre_machine/             # table Delta Gold 1
    └── checkpoints/silver_mesures/
        checkpoints/gold_agg_fenetre_machine/
```

## Logique Silver (`jobs/silver.py`)

1. Lecture stream Bronze (`file:///lakehouse/bronze/mesures`)
2. **Dédoublonnage** : `withWatermark("event_ts", "10 minutes")` +
   `dropDuplicates(["event_id"])`
3. **Détection d'anomalie par seuil** : jointure `broadcast` avec
   `referentiel/capteurs.csv` (plage nominale min/max par capteur) → colonne
   `is_anomalie` (booléen). **Aucune ligne n'est supprimée**, même les
   anomalies sont écrites.
4. Écriture Delta append, partitionnée par `event_date`, checkpoint dédié
   `checkpoints/silver_mesures`, trigger 10s.

## Logique Gold 1 (`jobs/gold_agg_fenetre.py`)

1. Lecture stream Silver
2. Watermark 2 min + `window(event_ts, "2 minutes", "1 minute")` (fenêtre
   glissante) groupé par `machine_id` → `valeur_moyenne`, `valeur_max`,
   `nb_mesures`, `nb_anomalies`
3. `outputMode("update")` + `foreachBatch` :
   - `DeltaTable.forPath(...).merge(...)` sur
     `(machine_id, window_start)` → `whenMatchedUpdateAll` +
     `whenNotMatchedInsertAll` (table Delta Gold pré-créée vide au démarrage
     du job pour que même le premier micro-batch passe par `MERGE INTO`)
   - upsert Postgres `gold.agg_fenetre_machine` via `psycopg2`
     `ON CONFLICT (machine_id, window_start) DO UPDATE`

## Prérequis (rôles A + B déjà faits)

```bash
docker compose up -d --build
docker compose ps   # 4 conteneurs Up

docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql
python3 -m venv ~/venv-projet && source ~/venv-projet/bin/activate
pip install psycopg2-binary
python3 scripts/load_referentiel.py

chmod 777 lakehouse
pip install -r requirements-generator.txt
```

## Étape 1 — Bronze doit tourner (rôle B)

```bash
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /jobs/bronze.py
```

## Étape 2 — Lancer Silver

```bash
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  /jobs/silver.py
```
⚠️ Le `--conf spark.hadoop.fs.permissions.umask-mode=000` est **obligatoire**
ici (contrairement à Bronze) — voir Incident 1 ci-dessous.

## Étape 3 — Lancer Gold 1 (agrégation fenêtre)

```bash
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  /jobs/gold_agg_fenetre.py
```

## ⚠️ Contrainte : 1 seul worker, 2 cœurs

Le worker Spark n'a que 2 cœurs (`SPARK_WORKER_CORES=2`, cf. rôle A). Chaque
job streaming (Bronze/Silver/Gold) consomme par défaut **tous** les cœurs
disponibles au démarrage. Sur cette machine, il n'a donc pas été possible de
faire tourner Bronze + Silver + Gold 1 **simultanément** — la preuve
ci-dessous a été obtenue en excutant les jobs par vagues successives
(Bronze → arrêt → Silver → arrêt → Gold 1), chaque job reprenant sur son
propre checkpoint là où il s'était arrêté (aucune perte, aucun doublon).

En production/avec plus de RAM : augmenter `SPARK_WORKER_CORES` (et
soumettre chaque job avec `--total-executor-cores 1`) pour les faire tourner
en continu et en parallèle, comme le montre le schéma d'architecture du
README principal.

## Preuves (capturées le 09/07/2026)

### Silver — dédoublonnage + anomalies, rien de supprimé
```bash
docker exec capteurs-spark-master /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /jobs/count_silver.py
```
```
COUNT= 700
COUNT anomalies= 100
```
700 = même count que Bronze après 2 vagues d'injection (400 puis 300
messages) → aucune perte, aucun doublon introduit par le job Silver.
100 anomalies / 700 ≈ 14,3 %, cohérent avec le taux ~15 % injecté par le
générateur mock.

### Gold 1 — agrégation qui grossit dans le temps (Postgres)
```sql
SELECT machine_id, window_start, nb_mesures, nb_anomalies
FROM gold.agg_fenetre_machine ORDER BY machine_id, window_start;
```
12 lignes après la 1ère vague (6 machines × 2 fenêtres glissantes) → **24
lignes** après la 2ᵉ vague (nouvelles fenêtres insérées, anciennes
conservées) : la table grossit bien dans le temps.

### Gold 1 — preuve `DESCRIBE HISTORY` (obligatoire)
```bash
docker exec capteurs-spark-master /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /jobs/describe_gold_agg.py
```
```
version | timestamp            | operation | operationParameters
2       | 2026-07-09 13:47:36  | MERGE     | matchedPredicates=[update], notMatchedPredicates=[insert]
1       | 2026-07-09 13:37:47  | MERGE     | matchedPredicates=[update], notMatchedPredicates=[insert]
0       | 2026-07-09 13:36:53  | WRITE     | mode=Overwrite (création table vide)
```
Confirme le comportement upsert réel : `MERGE INTO` exécuté à chaque
micro-batch non vide (le batch avec 0 nouvelle ligne, entre les versions 1 et
2, n'a délibérément généré aucune nouvelle version Delta — cf. code
`gold_agg_fenetre.py`, on court-circuite `upsert_batch` si `n == 0`).

## Incident 1 (résolu) — `mkdir` refusé sur le state store Silver/Gold

**Problème** : `silver.py` (et par extension `gold_agg_fenetre.py`, tous
deux utilisant un opérateur stateful — `dropDuplicates`+watermark pour l'un,
`groupBy(window(...))` pour l'autre) plantait systématiquement après le
premier micro-batch avec :
```
java.io.IOException: mkdir of file:/lakehouse/checkpoints/silver_mesures/state/0/1 failed
```
alors que `bronze.py` (rôle B), lui, tournait sans problème avec le même
volume `lakehouse` et le même `chmod 777` préalable.

**Diagnostic** : le **driver** (conteneur `spark-master`, process `root`)
crée les dossiers de checkpoint de haut niveau (`offsets/`, `commits/`,
`metadata`) en `755`, propriétaire `root`. `chmod -R 777` sur le volume ne
change rien car ces dossiers sont **recréés** à chaque nouveau run (le
`chmod` fait avant coup n'a plus d'effet une fois le dossier supprimé/
recréé). Le sous-dossier `state/` en revanche est écrit directement par
l'**executor** (conteneur `spark-worker-1`, process `spark`, uid 185) — un
utilisateur différent du driver. Hadoop's `RawLocalFileSystem.mkdirs()`
applique en plus une permission fixe (`755`) **indépendamment de l'`umask`
du process appelant**, donc même un `umask 000` sur le conteneur ne suffit
pas : seul le propriétaire (`root`, le driver) peut écrire dans les dossiers
qu'il a créés — l'executor (`spark`) en est exclu.

Bronze n'a jamais ce problème car il n'a **aucun opérateur stateful** (pas de
`dropDuplicates`/`groupBy` avec watermark) : tous ses écrits de checkpoint
sont faits par le driver seul, jamais par l'executor.

**Résolution** : forcer Hadoop à créer les dossiers de checkpoint avec des
permissions permissives dès la création, via une conf Spark dédiée :
```bash
--conf spark.hadoop.fs.permissions.umask-mode=000
```
(contrairement à `umask 000` au niveau shell, celle-ci est bien respectée
par `RawLocalFileSystem`, car elle configure directement
`fs.permissions.umask-mode` côté Hadoop plutôt que l'umask OS). Vérifié :
`state/` créé en `drwxrwxrwx spark:spark`, plus aucune erreur.

**Leçon pour D** : si le job Gold 2 (état courant par capteur) utilise lui
aussi un opérateur stateful (agrégat/dernière valeur avec watermark), ajouter
systématiquement ce `--conf` dès le premier lancement.

## Incident 2 (résolu) — `list[dict]` incompatible avec le Python du conteneur

**Problème** : `gold_agg_fenetre.py` plantait immédiatement avec
`TypeError: 'type' object is not subscriptable` à l'import.

**Cause** : le conteneur `spark-master` embarque **Python 3.8.10**, qui ne
supporte pas la syntaxe de generics `list[dict]` sans
`from __future__ import annotations` (disponible nativement seulement à
partir de Python 3.9). Cette syntaxe est utilisée sans le `__future__` import
dans le générateur (rôle B, exécuté côté hôte en Python 3.11 — jamais testée
dans le conteneur).

**Résolution** : ajout de `from __future__ import annotations` en tête de
`jobs/gold_agg_fenetre.py`.

**Leçon** : tout script exécuté via `spark-submit` doit être compatible
Python 3.8 (ou utiliser systématiquement `from __future__ import
annotations`), même si le code est écrit/testé sur un poste avec un Python
plus récent.

## Incident 3 (contournement, non résolu côté B) — `kafka-python` inutilisable sous Windows

**Problème** : en testant le pipeline de bout en bout sur une machine
Windows, `scripts/generateur_mesures.py` (rôle B) échoue systématiquement,
avec `kafka-python` en 3.0.7 (`KafkaTimeoutError: Unable to bootstrap`), en
2.1.5 et en 2.0.2 (`NoBrokersAvailable` / `NodeNotReadyError`) — alors que
Kafka est bien joignable en TCP sur `localhost:9092` (vérifié par un simple
`socket.create_connection`). Semble être un problème connu de compatibilité
de `kafka-python` avec la boucle non-bloquante sous Windows.

**Contournement utilisé pour les preuves de ce document** : injection des
événements directement via `kafka-console-producer.sh` du conteneur Kafka
(mêmes IDs/schéma JSON que le générateur, script `gen_events.py` non versionné
généré à la volée) — valide pour prouver Bronze/Silver/Gold, mais **ne
remplace pas** le générateur officiel du rôle B.

**À signaler à B / traiter collectivement** : si des membres de l'équipe
développent sous Windows, prévoir soit de lancer le générateur **depuis un
conteneur Linux** (ex. un petit service `generator` dans
`docker-compose.yml`), soit de documenter ce contournement dans le README
principal.

## À transmettre à D

- Chemin Silver : `file:///lakehouse/silver/mesures` — colonnes : mêmes
  champs que Bronze + `is_anomalie` (boolean)
- Table `gold.agg_fenetre_machine` déjà peuplée en Postgres (upsert `ON
  CONFLICT (machine_id, window_start)`)
- Pour l'état courant par capteur (Gold 2), lire aussi le stream **Silver**
  (pas Bronze) pour bénéficier de `is_anomalie` déjà calculé
- **Important** : ajouter `--conf spark.hadoop.fs.permissions.umask-mode=000`
  dès le premier lancement du job Gold 2 s'il utilise un opérateur stateful
  (cf. Incident 1)
- Contrainte 2 cœurs partagée : si Gold 2 doit tourner en même temps que
  Bronze/Silver/Gold 1, coordonner avec l'équipe sur l'augmentation de
  `SPARK_WORKER_CORES` dans `docker-compose.yml` (Rôle A)
