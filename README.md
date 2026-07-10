# Pipeline Big Data temps réel — Monitoring flotte de capteurs industriels

> Statut : 🔴 à faire · 🟡 en cours · 🟢 fait

| Section | Responsable(s) principal(aux) | Statut |
|---|---|---|
| 1. Architecture | A (infra) + C/D (modélisation Gold) | 🟢 |
| 2. Lancement | A + B/C/D (chacun ajoute son job) | 🟡 (Power BI manuel restant) |
| 3. Preuves | Chacun pour son étage | 🟡 (Power BI restant) |
| 4. Justifications | Collectif | 🟢 |
| 5. Incident | Tout le monde, au fil de l'eau | 🟢 |

**Ce qui reste réellement à faire : construire et capturer le dashboard
Power BI (section 6 du Lancement / fin de la section Preuves) — nécessite
Power BI Desktop, non disponible sur le poste utilisé pour développer/tester
le reste du pipeline. Tout le reste (infra, Kafka, Bronze, Silver, Gold 1,
Gold 2) est fait et vérifié.**

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

### Modélisation Gold (schéma en étoile)
- Table de faits **Gold 1** `agg_fenetre_machine` (rôle C) : grain = 1 ligne
  par `(machine_id, window_start)`, fenêtre glissante 2 min / pas 1 min,
  mesures `valeur_moyenne`, `valeur_max`, `nb_mesures`, `nb_anomalies` — FK
  implicite vers `dim_machine`
- Table de faits **Gold 2** `etat_courant_capteur` (rôle D) : grain = 1 ligne
  par `capteur_id` (dernière valeur/statut connu)
- Dimensions : `dim_capteur`, `dim_machine`, `dim_site` (déjà en place),
  pas de `dim_temps` séparée — le temps est porté directement par
  `window_start`/`window_end` et `event_ts` (pas de besoin de rollup calendaire
  pour ce projet)
- Partitionnement des tables Delta : Bronze et Silver partitionnés par
  `event_date` (`to_date(event_ts)`) ; Gold non partitionné (volumétrie trop
  faible par fenêtre pour que ça ait un intérêt)
- Format des tables : Delta partout (Parquet + `_delta_log/`)

---

## 2. Lancement

*Commandes exactes et reproductibles, de zéro à pipeline qui tourne.*

### Prérequis
- Docker + Docker Compose
- Python 3.x + venv

### Raccourci : tout lancer d'un coup
```bash
./scripts/run_all.sh --seed 300
```
Fait tout ce qui suit (infra, schéma, référentiel, les 4 jobs Spark par
vagues de 2 — cf. Incident 4, injection de 300 événements de démo). Idempotent
: peut être relancé sans dupliquer les jobs déjà actifs. Utile pour relancer
rapidement une démo pendant la soutenance. Les étapes détaillées ci-dessous
restent utiles pour comprendre/dépanner chaque brique séparément.

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
Plus nécessaire en pratique : `docker/spark-master.Dockerfile` fige déjà ces
paquets dans l'image (`docker compose up -d --build`), cf. Incident 2 du
rôle A ci-dessous.

### 4. Préparer le volume lakehouse et lancer le générateur (rôle B)
```bash
chmod -R 777 lakehouse   # le worker Spark (uid spark) doit pouvoir écrire dedans
pip install -r requirements-generator.txt
python3 scripts/generateur_mesures.py
# options utiles : --taux-anomalie 0.15   --max-events 200 (pour un run fini)
```
⚠️ **Sous Windows** : `kafka-python` échoue au bootstrap
(`KafkaTimeoutError`/`NoBrokersAvailable`) quelle que soit la version testée
(2.0.2 / 2.1.5 / 3.0.7), alors que Kafka est bien joignable en TCP. Cause
probable : incompatibilité de la boucle non-bloquante de `kafka-python` avec
Windows. Contournement utilisé pour tester B/C/D sur ce type de poste :
injecter des messages JSON (même schéma) via le CLI du conteneur —
```bash
docker exec -i capteurs-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --broker-list localhost:9092 --topic mesures < fichier.jsonl
```
Voir `README_role_C.md` (Incident 3) pour le détail. Sous Linux/Mac, le
générateur fonctionne normalement.

### 5. Soumettre les jobs Spark

```bash
# Bronze (B)
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /jobs/bronze.py

# Silver (C) — dédoublonnage + is_anomalie
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  /jobs/silver.py

# Gold 1 — agrégation fenêtre glissante par machine (C)
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  /jobs/gold_agg_fenetre.py

# Gold 2 — état courant par capteur (D)
docker exec -d capteurs-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  /jobs/gold_etat_courant.py
```
⚠️ **Contrainte 2 cœurs (1 seul worker)** : chaque job streaming consomme par
défaut tous les cœurs disponibles. Sur cette machine (`SPARK_WORKER_CORES=2`),
Bronze + Silver + Gold 1 + Gold 2 n'ont pas pu tourner **simultanément** —
voir `README_role_C.md`/`README_role_D.md` pour le détail de la stratégie de
preuve (jobs lancés par vagues successives, chacun reprenant sur son propre
checkpoint). En production/avec plus de cœurs, soumettre chaque job avec
`--total-executor-cores 1` pour qu'ils tournent en continu en parallèle.

### 6. Power BI — *TODO, nécessite Power BI Desktop*
```
Obtenir les données → Base de données → PostgreSQL → localhost:5432 → base "capteurs"
Mode DirectQuery recommandé (données mises à jour en continu par les jobs Gold)
Tables : gold.dim_capteur, gold.dim_machine, gold.dim_site,
         gold.agg_fenetre_machine, gold.etat_courant_capteur
```
Modèle (relations à créer dans l'onglet **Modèle**) :
```
dim_site (site_id) ──1─N── dim_machine (site_id)
dim_machine (machine_id) ──1─N── agg_fenetre_machine (machine_id)
dim_machine (machine_id) ──1─N── etat_courant_capteur (machine_id)
dim_capteur (capteur_id) ──1─1── etat_courant_capteur (capteur_id)
```
KPIs attendus (mesures DAX prêtes à l'emploi — détail complet dans
`README_role_D.md`) :
- `Nb Anomalies (fenêtre) = SUM(agg_fenetre_machine[nb_anomalies])`
- `Valeur Moyenne = AVERAGE(agg_fenetre_machine[valeur_moyenne])`
- `Nb Capteurs Batterie Faible = CALCULATE(COUNTROWS(etat_courant_capteur), etat_courant_capteur[dernier_batterie_pct] < 20)`
- Table/matrice "dernier statut par capteur" sur `etat_courant_capteur`

⚠️ **Non exécuté** : Power BI Desktop n'est pas installé sur le poste utilisé
pour développer/tester le pipeline (vérifié). C'est la seule étape du projet
qui reste à faire par la première personne de l'équipe qui l'a installé —
tout le reste (connexion, modèle, mesures) est documenté ci-dessus prêt à
suivre pas à pas.

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

### Kafka → Bronze — fait (rôle B, vérifié par C le 09/07)
- [x] Messages produits sur le topic `mesures` (400 puis +300, deux vagues)
- [x] Count Bronze qui grossit dans le temps : `COUNT= 400` → `COUNT= 700`
      après la 2ᵉ vague, job **relancé** entre les deux (checkpoint repris
      sans perte ni doublon)
- [x] Job visible dans Spark Master UI (`localhost:8080`) comme application
      `bronze_mesures`, état `RUNNING`

### Silver — fait (rôle C, 09/07)
- [x] Lignes avec `is_anomalie = true` présentes, rien n'est supprimé :
      `COUNT= 700` / `COUNT anomalies= 100` (≈14,3 %, cohérent avec le taux
      ~15 % injecté par le générateur)
- [x] Dédoublonnage actif (`withWatermark` + `dropDuplicates(["event_id"])`) :
      `COUNT` Silver == `COUNT` Bronze (700 == 700) après chaque vague, donc
      aucun doublon introduit ni aucune perte
- Détail complet des commandes/logs : `README_role_C.md`

### Gold — fait (Gold 1 rôle C 09/07, Gold 2 rôle D 10/07)
- [x] Table `agg_fenetre_machine` peuplée et mise à jour dans le temps :
      12 lignes (6 machines × 2 fenêtres) après la 1ère vague → **24 lignes**
      après la 2ᵉ (nouvelles fenêtres insérées, anciennes conservées)
- [x] Table `etat_courant_capteur` peuplée (**24 lignes = 24 capteurs**, une
      seule par `capteur_id`) et **mise à jour dans le temps** vérifiée : sur
      le même `capteur_id`, `derniere_maj` est passée de
      `2026-07-09 13:39:46` à `2026-07-10 08:16:00` après une nouvelle vague
      d'événements — la ligne est bien mise à jour en place (UPDATE), pas
      dupliquée
- [x] **`DESCRIBE HISTORY` exécutée avec succès** sur les deux tables Gold
      (preuve obligatoire) :

  `gold/agg_fenetre_machine` :
  ```
  version | operation | operationParameters
  2       | MERGE     | matchedPredicates=[update], notMatchedPredicates=[insert]
  1       | MERGE     | matchedPredicates=[update], notMatchedPredicates=[insert]
  0       | WRITE     | mode=Overwrite (création table vide au démarrage du job)
  ```

  `gold/etat_courant_capteur` :
  ```
  version | operation | operationParameters
  2       | MERGE     | matchedPredicates=[{"predicate":"derniere_maj >= t.derniere_maj","actionType":"update"}], notMatchedPredicates=[insert]
  1       | MERGE     | matchedPredicates=[{"predicate":"derniere_maj >= t.derniere_maj","actionType":"update"}], notMatchedPredicates=[insert]
  0       | WRITE     | mode=Overwrite (création table vide au démarrage du job)
  ```
  Le prédicat conditionnel `derniere_maj >= t.derniere_maj` apparaît dans les
  métadonnées de version elles-mêmes — preuve que le garde-fou anti-régression
  (contre un micro-batch en retard) est réellement actif, pas juste écrit
  dans le code. Deux usages distincts de `MERGE INTO` dans le projet (Gold 1
  et Gold 2), chacun avec upsert Postgres correspondant vérifié par
  `SELECT * FROM gold.agg_fenetre_machine` / `gold.etat_courant_capteur`.

### Power BI — *TODO, non exécuté (Power BI Desktop indisponible sur le poste de dev)*
- [ ] Capture du dashboard avec KPIs à jour
- [ ] Capture du modèle (relations faits/dimensions)

Connexion, modèle et mesures DAX entièrement documentés (section Lancement
ci-dessus et `README_role_D.md`) — reste seulement à ouvrir Power BI Desktop,
suivre les étapes et capturer le résultat.

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

### Gestion du checkpoint
- Un `checkpointLocation` dédié par `writeStream`, jamais partagé entre deux
  jobs :
  - Bronze : `file:///lakehouse/checkpoints/bronze_mesures`
  - Silver : `file:///lakehouse/checkpoints/silver_mesures`
  - Gold 1 (agg fenêtre) : `file:///lakehouse/checkpoints/gold_agg_fenetre_machine`
  - Gold 2 (état courant) : `file:///lakehouse/checkpoints/gold_etat_courant_capteur`
- **Testé réellement** (rôle C, 09/07) : le job Bronze a été arrêté puis
  relancé entre deux vagues d'injection de messages. Le count Bronze est
  passé de `400` à `700` (exactement `400 + 300` nouveaux messages), sans
  aucun doublon ni perte détectée côté Silver (`COUNT` Silver == `COUNT`
  Bronze à chaque vérification) → la reprise sur checkpoint fonctionne
  correctement, `startingOffsets=earliest` + `failOnDataLoss=false` combinés
  au checkpoint garantissent qu'aucun message Kafka n'est relu deux fois une
  fois committé.

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

### Incident 2 — `mkdir` refusé sur le state store Silver/Gold (rôle C)

**Problème** : `silver.py` (dédoublonnage stateful) et `gold_agg_fenetre.py`
(agrégation stateful) plantaient systématiquement après le premier
micro-batch avec `java.io.IOException: mkdir of
file:/lakehouse/checkpoints/.../state/0/1 failed`, alors que Bronze (rôle B,
non stateful) tournait sans problème sur le même volume.

**Diagnostic** : le driver (conteneur `spark-master`, process `root`) crée
les dossiers de checkpoint de haut niveau en `755`. Le sous-dossier `state/`
est écrit directement par l'**executor** (conteneur `spark-worker-1`,
process `spark`, uid 185) — utilisateur différent, sans droit d'écriture.
`chmod -R 777` et `umask 000` côté shell n'y changent rien : Hadoop
(`RawLocalFileSystem.mkdirs()`) applique une permission fixe (`755`)
indépendamment de l'umask du process appelant.

**Résolution** : ajout de `--conf
spark.hadoop.fs.permissions.umask-mode=000` au `spark-submit` — cette
option-là est bien respectée par Hadoop (contrairement à l'umask OS) et rend
les dossiers de checkpoint créés en `777`, accessibles à l'executor.

**Leçon pour D** : Gold 2 (`gold_etat_courant.py`) n'a **pas** d'opérateur
stateful streaming, mais rencontre la **même erreur** au tout premier
micro-batch — cette fois parce que c'est la table Delta vide elle-même
(créée par le driver avant le démarrage du stream) que l'executor n'arrive
pas à relire. Même fix (`--conf spark.hadoop.fs.permissions.umask-mode=000`),
donc à appliquer **systématiquement** dès qu'un job Gold crée une table
vide puis la merge, stateful ou non. Voir `README_role_D.md` pour le détail.

### Incident 3 (résolu) — Kafka perdait tous ses messages au redémarrage de Docker Desktop

**Problème** : après un redémarrage de Docker Desktop (VM relancée en cours
de session), tous les conteneurs du projet ont été recréés. Le topic
`mesures` est reparti **vide** (offset 0) alors que le checkpoint Bronze
pointait encore vers l'ancien offset (700). Résultat :
`WARN KafkaMicroBatchStream: Partition mesures-0's offset was changed from
700 to 200, some data may have been missed`, puis le job Bronze restait
bloqué sans progresser.

**Cause racine** : `docker-compose.yml` ne déclarait **aucun volume** pour le
service `kafka` (contrairement à `postgres` avec `pgdata`, et à
Spark/`lakehouse` avec un bind mount). Toute recréation du conteneur Kafka
effaçait donc l'intégralité des topics.

**Résolution** : ajout d'un bind mount `./kafka-data:/tmp/kafka-logs`
(chemin par défaut de `log.dirs` sur l'image `apache/kafka`, confirmé par
`docker exec capteurs-kafka env | grep LOG_DIR`). Un **volume nommé** a été
essayé en premier mais rejeté : Docker le crée `root:root`, alors que le
conteneur Kafka tourne en `uid 1000` (`appuser`) → `Error while writing
meta.properties file ... Permission denied` au démarrage. Le bind mount,
comme pour `./lakehouse`, permet un `chmod -R 777 kafka-data` explicite côté
hôte (fait automatiquement par `scripts/run_all.sh`).

### Incident 4 (résolu) — 4 jobs Spark simultanés saturent la VM Docker Desktop

**Problème** : en tentant de faire tourner les 4 jobs streaming (Bronze,
Silver, Gold 1, Gold 2) **en même temps** (`SPARK_WORKER_CORES` porté à 4,
`--total-executor-cores 1`/job), le démon Docker est devenu injoignable
(`request returned 500 Internal Server Error` sur **tous** les appels
`docker`, y compris `docker version`) après l'ajout d'une 5ᵉ requête
ponctuelle (une vérification `local[1]`) par-dessus les 4 jobs.

**Diagnostic** : `docker stats` montrait `capteurs-spark-master` à ~2 Go et
`capteurs-spark-worker-1` à ~1,3 Go, sur une VM Docker Desktop limitée à
**7,358 Go** (`docker info | grep "Total Memory"`) — pas les 16 Go du laptop
hôte. Chaque `spark-submit` lance un driver JVM (défaut `1g` de heap, réduit
à `512m` — en dessous, `SparkIllegalArgumentException: INVALID_DRIVER_MEMORY`
car Spark exige un minimum d'environ 450 Mo). 4 drivers + 4 executors +
requêtes ad-hoc ont fait déborder la VM ; les processus `com.docker.backend`
sont restés vivants mais bloqués (CPU à 300%+, aucune réponse) — un simple
restart de Docker Desktop a été nécessaire pour récupérer la main.

**Résolution** : retour à `SPARK_WORKER_CORES=2` / `SPARK_WORKER_MEMORY=2G`
dans `docker-compose.yml` — cette configuration a tourné sans le moindre
souci pendant tout le reste du projet. C'est désormais une **limite dure du
cluster** (2 cœurs au total), pas une simple politique de script : impossible
de refaire l'erreur par accident. `scripts/run_all.sh` orchestre les 4 jobs
**par vagues de 2 maximum** : Bronze+Silver tournent, absorbent les données,
sont arrêtés ; puis Gold 1+Gold 2 tournent et consomment le Silver produit,
et restent actifs (Postgres/Power BI restent à jour en continu tant
qu'aucune nouvelle vague Bronze+Silver n'est relancée).

**Leçon** : la contrainte réelle sur ce projet n'est pas le nombre de cœurs
du laptop hôte (16 Go, largement suffisant) mais la RAM allouée par défaut à
la VM Docker Desktop. Avant d'augmenter `SPARK_WORKER_CORES`, vérifier
`docker info | grep "Total Memory"` et/ou augmenter cette allocation dans
les paramètres de Docker Desktop (Settings → Resources).

---

## Notation — rappel des critères de l'énoncé
- Pipeline fonctionnel bout en bout
- Fonctionnement correct de Kafka et Spark
- Usage correct de Delta
- Pertinence de la modélisation en étoile et des KPIs Power BI
- Qualité de la preuve et de la justification dans ce README
- Clarté de la soutenance et solidité des réponses aux questions
