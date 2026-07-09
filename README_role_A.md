Voici le contenu complet à copier-coller directement dans README_role_A.md (remplace tout le contenu existant du fichier) :
markdown# Rôle A — Infra & Référentiel

Ce document explique ce qui a été mis en place côté infra, comment le relancer,
et ce qu'il faut savoir avant de brancher B, C ou D dessus.

> ⚠️ Ceci **n'est pas le README final du projet**. C'est un doc de travail
> interne au rôle A. Le README final unique (`README.md` à la racine) est le
> seul livrable évalué — chacun doit y reporter sa partie.

## Ce qui a été livré
big-data/
├── docker-compose.yml            # Kafka (KRaft), Spark standalone (master+worker), Postgres
├── docker/
│   └── spark-master.Dockerfile   # image custom : delta-spark + psycopg2 figés
├── sql/init_schema.sql           # schéma gold : 3 dimensions + 2 tables de faits/agrégats
├── scripts/
│   ├── generate_referentiel.py   # a produit les 3 CSV (rejouable si les IDs changent)
│   └── load_referentiel.py       # charge les CSV dans Postgres (upsert)
├── referentiel/
│   ├── sites.csv       # 2 sites : site-lyon, site-nantes
│   ├── machines.csv    # 6 machines : m-01 à m-06
│   └── capteurs.csv    # 24 capteurs : cpt-001 à cpt-024 (4 par machine)
├── jobs/       # <- vide, à remplir par B (bronze.py), C (silver.py, gold_agg.py), D (gold_etat.py)
└── lakehouse/  # <- stockage des tables Delta, généré à l'exécution (ignoré par git)

## Commandes pour relancer l'infra depuis zéro

```bash
cd big-data
docker compose up -d --build
docker compose ps   # vérifier que les 4 conteneurs sont "Up"
```

### Créer le schéma Postgres (une seule fois, ou après un volume reset)
```bash
docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql
```

### Charger le référentiel (une seule fois, ou si le référentiel change)
```bash
python3 -m venv ~/venv-projet
source ~/venv-projet/bin/activate
pip install psycopg2-binary
python3 scripts/load_referentiel.py
```

### Spark master : delta-spark + psycopg2 déjà inclus
Plus besoin d'installer quoi que ce soit à la main : `spark-master` est
construit depuis `docker/spark-master.Dockerfile`, qui fige `delta-spark==3.2.0`
et `psycopg2-binary` dans l'image. Ça survit à un `docker compose down`/`up`
complet (contrairement à un `docker exec -u root pip install`, qui se perdait
à chaque recréation du conteneur — voir Incident).

Si tu modifies ce Dockerfile, reconstruis l'image avec :
```bash
docker compose build spark-master
docker compose up -d
```

Vérification que les paquets sont bien présents :
```bash
docker exec -it capteurs-spark-master python3 -c "import delta; import psycopg2; print('OK')"
```

## Preuves que l'infra fonctionne (capturées le 09/07)

**Conteneurs up :**
NAME                      STATUS
capteurs-kafka            Up (127.0.0.1:9092)
capteurs-postgres         Up (127.0.0.1:5432)
capteurs-spark-master     Up (127.0.0.1:4040, 7077, 8080)
capteurs-spark-worker-1   Up

**Schéma Postgres créé (`\dt gold.*`) :**
Schema |         Name         | Type  |  Owner
--------+----------------------+-------+----------
gold   | agg_fenetre_machine  | table | capteurs
gold   | dim_capteur          | table | capteurs
gold   | dim_machine          | table | capteurs
gold   | dim_site             | table | capteurs
gold   | etat_courant_capteur | table | capteurs
(5 rows)

**Référentiel chargé (`SELECT * FROM gold.dim_machine;`) :**
machine_id |    type_machine    | ligne_production | criticite | ... | site_id
------------+--------------------+------------------+-----------+-----+-------------
m-01       | presse hydraulique | ligne-A          | haute     | ... | site-lyon
m-02       | convoyeur          | ligne-A          | moyenne   | ... | site-lyon
m-03       | compresseur        | ligne-B          | haute     | ... | site-lyon
m-04       | presse hydraulique | ligne-C          | haute     | ... | site-nantes
m-05       | convoyeur          | ligne-C          | basse     | ... | site-nantes
m-06       | compresseur        | ligne-D          | moyenne   | ... | site-nantes
(6 rows)

**Dépendances Spark confirmées (`import delta; import psycopg2`) :** `OK`

## À transmettre à B, C, D

- **Réseau Kafka** : `localhost:9092` depuis l'hôte (générateur), `kafka:29092`
  depuis les conteneurs Spark.
- **IDs disponibles** : sites `site-lyon`/`site-nantes` ; machines `m-01` à
  `m-06` ; capteurs `cpt-001` à `cpt-024` (2 température + 1 vibration + 1
  pression par machine).
- **Tables Postgres prêtes** pour les upserts Gold :
  - `gold.agg_fenetre_machine` — PK `(machine_id, window_start)` → pour C,
    `ON CONFLICT (machine_id, window_start) DO UPDATE`
  - `gold.etat_courant_capteur` — PK `capteur_id` → pour D,
    `ON CONFLICT (capteur_id) DO UPDATE`
  - Dimensions déjà peuplées : `dim_capteur`, `dim_machine`, `dim_site`
- **Chemin Delta** : `/lakehouse/...` dans les conteneurs Spark (volume
  partagé bind mount `./lakehouse`), pas de vrai HDFS — choix assumé pour un
  environnement laptop 16 Go.
- **`delta-spark`/`psycopg2` déjà dans l'image `spark-master`** — rien à
  installer manuellement.
- **⚠️ Conflits de port fréquents** : si vous avez d'anciens TP qui tournent
  encore (Spark/Airflow/HDFS d'un TP précédent), `docker compose up` peut
  échouer sur les ports `7077`/`8080`/`9092`/`5432`. Voir Incident ci-dessous
  pour la marche à suivre.

## Incident 1 (résolu) — Conflits de port au premier lancement

**Problème** : au premier `docker compose up -d`, deux conflits de port
successifs :
- `7077` déjà utilisé → conteneur `spark-master` d'un ancien TP (HDFS/Spark)
  encore actif depuis 21h
- `8080` déjà utilisé → conteneur `tp-lakehouse-airflow-webserver-1` d'un
  autre TP encore actif

**Diagnostic** : `lsof -i :<port>` confirmait que c'était Docker qui tenait le
port (pas un process Mac natif), puis `docker ps -a` a permis d'identifier les
conteneurs orphelins d'anciens TP qui tournaient toujours en parallèle.

**Résolution** :
```bash
docker stop spark-master tp-lakehouse-airflow-webserver-1
docker compose up -d
```
Puis nettoyage plus large des anciens TP pour éviter d'autres conflits
(`namenode`, `datanode1`, `datanode3`, `spark-worker`, `tp-lakehouse-*`) —
arrêtés (`docker stop`, pas supprimés) pour libérer la RAM sur la contrainte
16 Go du laptop.

**Leçon** : sur un environnement partagé/laptop perso où plusieurs TP du
semestre ont pu laisser des conteneurs actifs en arrière-plan, toujours
vérifier `docker ps -a` avant un premier `docker compose up` sur un nouveau
projet.

## Incident 2 (résolu) — dépendances Spark non persistées

**Problème** : `delta-spark`/`psycopg2-binary` installés via
`docker exec -u root -it capteurs-spark-master pip install ...` ne survivaient
pas à une recréation du conteneur (`docker compose down` puis `up`) — seul un
`restart` simple les conservait. Risque que B/C/D voient leurs jobs planter
avec une erreur d'import si le conteneur est un jour recréé sans repasser par
cette commande.

**Résolution** : création de `docker/spark-master.Dockerfile` qui fige ces
dépendances dans l'image (`docker compose build spark-master`). Le service
`spark-master` du `docker-compose.yml` pointe maintenant sur ce Dockerfile
plutôt que sur l'image `apache/spark:3.5.1-python3` brute. Vérifié avec
`docker exec -it capteurs-spark-master python3 -c "import delta; import psycopg2"`.