# Rôle B — Générateur & Bronze

Ce document explique le livrable B : générateur mock + job Bronze Kafka → Delta.

> Doc de travail interne au rôle B. Reporter les commandes et preuves dans `README.md`.

## Ce qui a été livré

```
big-data/
├── scripts/
│   └── generateur_mesures.py   # mock générateur → Kafka topic "mesures"
├── jobs/
│   └── bronze.py               # Structured Streaming → Delta Bronze
├── requirements-generator.txt  # dépendances hôte (kafka-python)
└── lakehouse/                  # généré à l'exécution
    ├── bronze/mesures/         # table Delta Bronze (partition event_date)
    └── checkpoints/bronze_mesures/
```

## Prérequis (rôle A déjà fait)

```bash
cd big-data
docker compose up -d --build
docker compose ps   # 4 conteneurs Up

docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql
python3 -m venv ~/venv-projet && source ~/venv-projet/bin/activate
pip install psycopg2-binary
python3 scripts/load_referentiel.py
```

### 0. Préparer le volume lakehouse (une fois)

Le worker Spark (uid `spark`) doit pouvoir écrire dans `./lakehouse` :

```bash
chmod 777 lakehouse
```

Si des jobs ont déjà tourné en root et planté, nettoyer puis relancer :

```bash
docker exec -u root capteurs-spark-master rm -rf /lakehouse/checkpoints/bronze_mesures /lakehouse/bronze
docker exec -u root capteurs-spark-master chmod -R 777 /lakehouse
```

## Étape 1 — Installer le générateur (sur l'hôte)

Le générateur tourne **sur l'hôte** et publie vers `localhost:9092` (listener PLAINTEXT Kafka).

```bash
source ~/venv-projet/bin/activate
pip install -r requirements-generator.txt
```

## Étape 2 — Lancer le job Bronze (dans Spark)

Dans un terminal dédié (le job reste actif) :

```bash
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /jobs/bronze.py
```

Spark UI du job : http://127.0.0.1:4040 (onglet **Structured Streaming**).

## Étape 3 — Lancer le générateur

Dans un autre terminal :

```bash
source ~/venv-projet/bin/activate
python3 scripts/generateur_mesures.py
# options utiles :
#   --taux-anomalie 0.15
#   --max-events 200   # pour un test fini
```

## Étape 4 — Preuves

### Messages Kafka
```bash
docker exec capteurs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic mesures \
  --from-beginning \
  --max-messages 3
```

### Count Bronze qui grossit
```bash
docker exec capteurs-spark-master /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /jobs/count_bronze.py
```

Relancer la commande après 30s : le count doit augmenter.

### Spark UI
Ouvrir http://127.0.0.1:4040 → onglet **Structured Streaming** : micro-batches en cours.

## Schéma événement (conforme énoncé)

```json
{
  "event_id": "evt-9f3a1c2d",
  "capteur_id": "cpt-001",
  "machine_id": "m-01",
  "site_id": "site-lyon",
  "type_mesure": "temperature",
  "valeur": 78.4,
  "unite": "celsius",
  "qualite_signal": 0.97,
  "batterie_pourcentage": 63,
  "timestamp": "2026-07-08T10:15:32.104Z"
}
```

IDs alignés sur `referentiel/*.csv` (24 capteurs, 6 machines, 2 sites).

## Choix techniques

| Choix | Justification |
|---|---|
| Topic `mesures` | Nom unique, partagé par tout le pipeline |
| Chemin `file:///lakehouse/...` | Évite l'erreur Hadoop `Mkdirs failed` sur le worker Spark |
| Checkpoint `file:///lakehouse/checkpoints/bronze_mesures` | Dédié au job Bronze, jamais partagé |
| Partition `event_date` | Facilite les lectures par jour et la rétention |
| `startingOffsets=earliest` | Ne perd aucun message au premier démarrage |
| Taux anomalie ~12% (défaut) | Permet à C de tester `is_anomalie` sans attendre le générateur officiel |

## À transmettre à C

- Chemin Bronze : `file:///lakehouse/bronze/mesures` (ou `/lakehouse/bronze/mesures` depuis un shell dans le conteneur)
- Schéma typé identique à l'événement Kafka + `event_ts`, `event_date`
- Les anomalies sont dans les valeurs (hors plage nominale du référentiel), pas encore flaggées

## Incident 1 (résolu) — Écriture Delta impossible sur le worker

**Problème** : le job Bronze démarrait mais le micro-batch échouait avec
`Mkdirs failed to create file:/lakehouse/bronze/mesures/event_date=...`.

**Cause** : Hadoop interprétait mal le chemin relatif `file:/lakehouse` depuis
le répertoire de travail du worker (`/opt/spark/work/...`). Droits lakehouse
insuffisants si le premier lancement se fait en root.

**Résolution** :
1. Chemins explicites `file:///lakehouse/...` dans `jobs/bronze.py`
2. `chmod 777 lakehouse` avant le premier run
3. Nettoyer checkpoint + bronze si un run précédent a échoué à mi-chemin

**Preuve** : `COUNT= 75` via `jobs/count_bronze.py`, fichiers Parquet dans
`lakehouse/bronze/mesures/event_date=2026-07-09/`.
