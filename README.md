# Pipeline Big Data temps réel — Monitoring flotte de capteurs industriels

> Statut : 🔴 à faire · 🟡 en cours · 🟢 fait

| Section | Responsable(s) principal(aux) | Statut |
|---|---|---|
| 1. Architecture | A (infra) + C/D (modélisation Gold) | 🟡 |
| 2. Lancement | A + B/C/D (chacun ajoute son job) | 🟡 |
| 3. Preuves | Chacun pour son étage | 🔴 |
| 4. Justifications | Collectif | 🔴 |
| 5. Incident | Tout le monde, au fil de l'eau | 🟡 |

---

## 1. Architecture

*Schéma ou description de l'implémentation réelle (pas celle du sujet), avec
les choix de partitionnement et de format des tables.*

### Vue d'ensemble

```
Générateur capteurs (Python, hôte)
        │  publie JSON
        ▼
   Kafka (topic "mesures")
        │  Spark Structured Streaming (readStream)
        ▼
  [BRONZE]  Delta — capture brute, append, sans perte
        │  nettoyage + dédoublonnage + détection anomalie (seuil)
        ▼
  [SILVER]  Delta — typé, dédupliqué, colonne is_anomalie (rien n'est supprimé)
        │
        ├──> [GOLD 1] Agrégation fenêtre glissante par machine (moyenne/max)
        │       foreachBatch + MERGE INTO (Delta) + upsert Postgres
        │
        └──> [GOLD 2] État courant par capteur (dernière valeur/statut)
                foreachBatch + MERGE INTO (Delta) + upsert Postgres

  Postgres (schéma gold) ──> Power BI Desktop (DirectQuery)
```

### Infra (rôle A)
- Kafka en mode KRaft (sans ZooKeeper), 2 listeners : `PLAINTEXT` (hôte,
  `localhost:9092`) et `INTERNAL` (conteneurs Spark, `kafka:29092`)
- Spark standalone : 1 master + 1 worker (2 cœurs / 2 Go — contrainte laptop
  16 Go)
- Stockage Delta sur volume partagé (`./lakehouse` bind-mount entre
  spark-master et spark-worker-1) — **pas de vrai HDFS**, choix assumé pour
  un environnement local, cf. Justifications
- Postgres comme couche de service BI (Gold Delta reste la source de vérité
  versionnée ; Postgres est une réplique dénormalisée pour Power BI, alimentée
  en upsert)

### Référentiel statique
2 sites (`site-lyon`, `site-nantes`), 6 machines (`m-01` à `m-06`), 24 capteurs
(`cpt-001` à `cpt-024`, 4 par machine : 2 température, 1 vibration, 1
pression). Voir `referentiel/*.csv` et `scripts/generate_referentiel.py`.

### Modélisation Gold (schéma en étoile) — *à compléter par C/D*
- Table de faits : `TODO` (grain, mesures, clés étrangères)
- Dimensions : `dim_capteur`, `dim_machine`, `dim_site` (déjà en place),
  `dim_temps` ? *(à décider)*
- Partitionnement des tables Delta : `TODO` (ex. par date sur Bronze/Silver)
- Format des tables : Delta partout (Parquet + `_delta_log/`)

---

## 2. Lancement

*Commandes exactes et reproductibles, de zéro à pipeline qui tourne.*

### Prérequis
- Docker + Docker Compose
- Python 3.x + venv

### 1. Infra
```bash
git clone https://github.com/sarah-khiter/big-data.git
cd big-data
docker compose up -d
docker compose ps   # vérifier que les 4 conteneurs sont Up
```
⚠️ En cas d'erreur "port already allocated", voir section Incident —
probablement un conteneur d'un TP précédent à arrêter (`docker ps -a`).

### 2. Schéma Postgres + référentiel
```bash
docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql

python3 -m venv ~/venv-projet
source ~/venv-projet/bin/activate
pip install psycopg2-binary
python3 scripts/load_referentiel.py
```

### 3. Dépendances Spark
```bash
docker exec -u root -it capteurs-spark-master pip install delta-spark==3.2.0 psycopg2-binary
```

### 4. Lancer le générateur — *TODO (rôle B)*
```bash
# TODO : commande exacte du générateur (mock ou officiel une fois fourni)
```

### 5. Soumettre les jobs Spark — *TODO (rôles B/C/D)*
```bash
# Bronze (B)
docker exec -u root -it capteurs-spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /jobs/TODO_bronze.py

# Silver + Gold agrégation (C)
# TODO

# Gold état courant (D)
# TODO
```

### 6. Power BI — *TODO (rôle D)*
```
Obtenir les données → PostgreSQL → localhost:5432 → base "capteurs"
Mode DirectQuery recommandé
```

---

## 3. Preuves

*Pour chaque étage (Kafka → Bronze → Silver → Gold), une capture ou une
sortie de commande montrant que les données transitent réellement, plus une
requête `DESCRIBE HISTORY`/`VERSION AS OF` exécutée avec succès, plus une
capture du dashboard Power BI avec les KPIs à jour.*

### Infra (rôle A) — fait
**Conteneurs up :**
```
NAME                      STATUS
capteurs-kafka            Up (127.0.0.1:9092)
capteurs-postgres         Up (127.0.0.1:5432)
capteurs-spark-master     Up (127.0.0.1:4040, 7077, 8080)
capteurs-spark-worker-1   Up
```

**Schéma Postgres (`\dt gold.*`) :**
```
 Schema |         Name         | Type  |  Owner
--------+----------------------+-------+----------
 gold   | agg_fenetre_machine  | table | capteurs
 gold   | dim_capteur          | table | capteurs
 gold   | dim_machine          | table | capteurs
 gold   | dim_site             | table | capteurs
 gold   | etat_courant_capteur | table | capteurs
(5 rows)
```

**Référentiel chargé (`SELECT * FROM gold.dim_machine;`) :** 6 lignes
vérifiées, cohérentes avec les sites (`site-lyon`/`site-nantes`).

### Kafka → Bronze — *TODO (rôle B)*
- [ ] Capture montrant les messages qui arrivent sur le topic
- [ ] Count Bronze qui grossit dans le temps
- [ ] Capture Spark UI (`localhost:4040`, onglet Structured Streaming)

### Silver — *TODO (rôle C)*
- [ ] Lignes avec `is_anomalie = true` présentes (pas supprimées)
- [ ] Preuve du dédoublonnage (compter les doublons avant/après)

### Gold — *TODO (rôle C/D)*
- [ ] Table agrégation fenêtre glissante peuplée
- [ ] Table état courant peuplée, mise à jour visible dans le temps
- [ ] `DESCRIBE HISTORY` ou `VERSION AS OF` exécutée avec succès sur une
      table Gold Delta *(preuve obligatoire, à ne pas oublier)*

### Power BI — *TODO (rôle D)*
- [ ] Capture du dashboard avec KPIs à jour

---

## 4. Justifications

*Pourquoi Kappa dans ce contexte, comment le checkpoint est géré, ce que vous
feriez pour la compaction des petits fichiers si vous aviez plus de temps.*

### Pourquoi Kappa ici — *TODO (collectif)*
Le cas d'usage (capteurs industriels émettant en continu, 1 mesure/1-3s) est
un vrai flux, contrairement à un clickstream ou des transactions qui peuvent
être simulés par lots. Un pipeline Lambda dupliquerait inutilement la logique
batch/streaming pour une donnée dont la valeur est avant tout temps réel.
*(à développer/nuancer par l'équipe)*

### Gestion du checkpoint — *TODO*
- Un `checkpointLocation` par `writeStream`, jamais partagé entre deux jobs
- Emplacement : `TODO` (chemin exact utilisé)
- *(préciser ce qui a été testé : redémarrage d'un job, reprise sans perte/doublon ?)*

### Compaction des petits fichiers — *TODO*
Chaque micro-batch écrit de nouveaux fichiers Parquet en streaming ; sans
`OPTIMIZE` périodique, la table Delta se fragmente. Non fait faute de temps —
*(préciser ce qui serait fait : fréquence d'un job `OPTIMIZE`, ou VACUUM)*.

### Pourquoi Postgres en plus de Delta (remplacement Metabase → Power BI)
Power BI Desktop n'a pas de connecteur natif vers des tables Delta (fichiers
Parquet + `_delta_log/`). Le Gold Delta reste la source de vérité versionnée
(time travel, `MERGE INTO`) ; les mêmes `foreachBatch` upsertent en parallèle
vers Postgres, qui sert uniquement de couche de restitution BI.

### Choix d'architecture infra (rôle A)
- Volume bind-mount (`./lakehouse`) plutôt qu'un vrai HDFS distribué : choix
  assumé pour tenir sur un laptop 16 Go, cohérent avec la contrainte du sujet.
- 2 listeners Kafka nécessaires pour que Spark (dans son conteneur) et le
  générateur (sur l'hôte) atteignent Kafka chacun par le bon chemin réseau.

---

## 5. Incident

*Un problème réellement rencontré et comment vous l'avez résolu — noté au
fil de l'eau, pas reconstitué après coup.*

### Incident 1 — Conflits de port au premier lancement (rôle A)
**Problème** : `docker compose up -d` a échoué deux fois de suite sur
`Bind for 0.0.0.0:7077 failed: port is already allocated`, puis `8080`.

**Diagnostic** : `lsof -i :<port>` montrait que c'était Docker qui tenait le
port. `docker ps -a` a révélé plusieurs conteneurs d'anciens TP du semestre
encore actifs en arrière-plan (`spark-master` d'un TP HDFS/Spark depuis 21h,
`tp-lakehouse-airflow-webserver-1`, etc.).

**Résolution** :
```bash
docker stop spark-master tp-lakehouse-airflow-webserver-1
docker compose up -d
```
Puis arrêt (pas suppression) des autres conteneurs orphelins pour libérer de
la RAM sur la contrainte 16 Go.

**Leçon** : toujours vérifier `docker ps -a` avant un premier lancement sur
une machine ayant servi à plusieurs TP du semestre.

### Incident 2 — *TODO (rôle B/C/D, à compléter au fil du projet)*

---

## Notation — rappel des critères de l'énoncé
- Pipeline fonctionnel bout en bout
- Fonctionnement correct de Kafka et Spark
- Usage correct de Delta
- Pertinence de la modélisation en étoile et des KPIs Power BI
- Qualité de la preuve et de la justification dans ce README
- Clarté de la soutenance et solidité des réponses aux questions
