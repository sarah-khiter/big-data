# Rôle A — Infra & Référentiel

Ce dossier contient tout ce qu'il faut pour démarrer l'infra du projet.
À faire en premier : B, C et D dépendent de ça pour commencer.

## Structure livrée

```
projet/
├── docker-compose.yml
├── referentiel/
│   ├── sites.csv
│   ├── machines.csv
│   └── capteurs.csv
├── sql/
│   └── init_schema.sql
├── scripts/
│   ├── generate_referentiel.py   # a produit les 3 CSV ci-dessus (rejouable)
│   └── load_referentiel.py       # charge les CSV dans Postgres
├── jobs/          # <- vide, à remplir par B/C/D (bronze.py, silver.py, gold_*.py)
└── lakehouse/      # <- stockage des tables Delta (bronze/silver/gold)
```

## Commandes, dans l'ordre

### 1. Lancer l'infra

```bash
cd projet
docker compose up -d
docker compose ps
```

Vérifier que les 4 services sont up : `capteurs-kafka`, `capteurs-spark-master`,
`capteurs-spark-worker-1`, `capteurs-postgres`.

### 2. Créer le schéma Postgres (gold : dimensions + faits)

```bash
docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql
```

Vérification :
```bash
docker exec -it capteurs-postgres psql -U capteurs -d capteurs -c "\dt gold.*"
```

### 3. Charger le référentiel dans Postgres

```bash
python3 -m venv ~/venv-projet
source ~/venv-projet/bin/activate
pip install psycopg2-binary
python3 scripts/load_referentiel.py
```

Vérification :
```bash
docker exec -it capteurs-postgres psql -U capteurs -d capteurs -c "SELECT * FROM gold.dim_machine;"
```

### 4. Préparer le conteneur Spark master (delta-spark + psycopg2)

```bash
docker exec -u root -it capteurs-spark-master pip install delta-spark==3.2.0 psycopg2-binary
```

(delta-spark seulement sur spark-master : c'est là que tourne le driver au
`spark-submit`. Les JARs Java sont ensuite distribués automatiquement au worker
via `spark.jars.packages`.)

### 5. Vérifier l'accessibilité Kafka / Spark UI / Postgres

- Spark master UI : http://localhost:8080
- Spark job UI (une fois un job lancé) : http://localhost:4040
- Kafka : accessible sur `localhost:9092` depuis l'hôte (générateur), sur
  `kafka:29092` depuis les conteneurs Spark
- Postgres : accessible sur `localhost:5432` depuis l'hôte (Power BI Desktop
  tourne hors Docker, donc directement dessus)

## Points à transmettre à B / C / D

- Les IDs du référentiel sont : sites `site-lyon`, `site-nantes` ; machines
  `m-01` à `m-06` ; capteurs `cpt-001` à `cpt-024` (4 par machine : 2
  température, 1 vibration, 1 pression).
- **⚠️ Quand le générateur officiel sera fourni**, comparer ses IDs à ceux-ci.
  S'ils diffèrent, ajuster les listes `SITES` / `MACHINES` / `CAPTEURS` dans
  `scripts/generate_referentiel.py`, relancer le script, puis rejouer
  `load_referentiel.py`.
- Table `gold.agg_fenetre_machine` : clé primaire `(machine_id, window_start)`
  → pour C, upsert avec `ON CONFLICT (machine_id, window_start) DO UPDATE`.
- Table `gold.etat_courant_capteur` : clé primaire `capteur_id` → pour D,
  upsert avec `ON CONFLICT (capteur_id) DO UPDATE`.
- Le chemin `/lakehouse/...` est un volume partagé entre spark-master et
  spark-worker-1 (bind mount `./lakehouse`) — pas de vrai HDFS ici, choix
  assumé pour un environnement laptop 16 Go (à justifier dans le README final,
  section Architecture).

## Incident rencontré (à compléter au fil du projet)

*(Section à tenir à jour par toute l'équipe — noter ici tout problème réel
rencontré côté infra : conflit de port, volume non accessible en écriture,
conteneur qui ne démarre pas, etc.)*
