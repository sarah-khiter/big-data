#!/usr/bin/env bash
# Lance tout le pipeline de zéro : infra -> schéma -> référentiel -> les 4 jobs
# Spark (Bronze/Silver/Gold1/Gold2), au maximum 2 à la fois (voir Incident 4,
# README.md) -> seed de données de démo.
#
# Vague 1 : Bronze + Silver tournent, absorbent les données, sont arrêtés.
# Vague 2 : Gold 1 + Gold 2 tournent, consomment le Silver produit, et
#           restent actifs à la fin (Postgres/Power BI restent à jour).
#
# Usage :
#   ./scripts/run_all.sh              # infra + jobs, pas de données
#   ./scripts/run_all.sh --seed 300   # + injecte 300 événements de démo
#   ./scripts/run_all.sh --seed 300 --taux-anomalie 0.2
#
# Conçu pour Git Bash (Windows) et bash (Linux/Mac). Idempotent : peut être
# relancé (ex. après avoir généré plus de données) sans dupliquer les jobs.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Sous Git Bash, désactive la conversion automatique des chemins Unix (/opt/...)
# en chemins Windows dans les arguments passés à `docker exec` (cf. README Incident).
export MSYS_NO_PATHCONV=1

SEED_COUNT=0
TAUX_ANOMALIE=0.15
while [[ $# -gt 0 ]]; do
  case "$1" in
    --seed) SEED_COUNT="$2"; shift 2 ;;
    --taux-anomalie) TAUX_ANOMALIE="$2"; shift 2 ;;
    *) echo "Option inconnue: $1"; exit 1 ;;
  esac
done

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "$PYTHON_BIN" ]]; then
  echo "Erreur: ni python3 ni python trouvé dans le PATH." >&2
  exit 1
fi

log() { echo -e "\n\033[1;34m>>> $*\033[0m"; }
warn() { echo -e "\033[1;33m!!! $*\033[0m"; }

# ---------------------------------------------------------------------------
log "1/8 Vérification des conflits de port (9092/5432/4040/7077/8080)"
# ---------------------------------------------------------------------------
CONFLICT=0
for p in 9092 5432 4040 7077 8080; do
  owner=$(docker ps --format '{{.Names}} {{.Ports}}' | grep ":$p->" | grep -v "capteurs-" || true)
  if [[ -n "$owner" ]]; then
    warn "Port $p déjà utilisé par un conteneur qui n'appartient pas au projet : $owner"
    CONFLICT=1
  fi
done
if [[ "$CONFLICT" -eq 1 ]]; then
  echo
  warn "Arrêtez ces conteneurs avant de continuer, par exemple :"
  echo "    docker stop <nom_du_conteneur>"
  echo "(voir README.md, Incident 1, pour le cas déjà rencontré sur ce projet)"
  exit 1
fi

# ---------------------------------------------------------------------------
log "2/8 Démarrage de l'infra (docker compose up -d --build)"
# ---------------------------------------------------------------------------
mkdir -p kafka-data
chmod -R 777 kafka-data
docker compose up -d --build

log "Attente que les 4 conteneurs soient sains"
for i in $(seq 1 30); do
  up=$(docker compose ps --format '{{.Names}} {{.State}}' | grep -c "running" || true)
  [[ "$up" -ge 4 ]] && break
  sleep 2
done
docker compose ps

log "Attente que Postgres accepte les connexions"
for i in $(seq 1 30); do
  docker exec capteurs-postgres pg_isready -U capteurs >/dev/null 2>&1 && break
  sleep 2
done

log "Attente que Spark Master réponde (localhost:8080)"
for i in $(seq 1 30); do
  curl -sf http://127.0.0.1:8080/json/ >/dev/null 2>&1 && break
  sleep 2
done

# ---------------------------------------------------------------------------
log "3/8 Schéma Postgres (idempotent : IF NOT EXISTS)"
# ---------------------------------------------------------------------------
docker exec -i capteurs-postgres psql -U capteurs -d capteurs < sql/init_schema.sql

# ---------------------------------------------------------------------------
log "4/8 Chargement du référentiel (upsert, idempotent)"
# ---------------------------------------------------------------------------
"$PYTHON_BIN" -m pip install --quiet psycopg2-binary
"$PYTHON_BIN" scripts/load_referentiel.py

# ---------------------------------------------------------------------------
log "5/8 Permissions du volume lakehouse"
# ---------------------------------------------------------------------------
chmod -R 777 lakehouse 2>/dev/null || docker exec -u root capteurs-spark-master chmod -R 777 /lakehouse

# ---------------------------------------------------------------------------
log "6/8 Création du topic Kafka 'mesures'"
# ---------------------------------------------------------------------------
docker exec capteurs-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --if-not-exists --topic mesures --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# ---------------------------------------------------------------------------
log "7/8 Jobs Spark — par VAGUES DE 2 max (voir Incident 4, README.md)"
# ---------------------------------------------------------------------------
# La VM Docker Desktop (~7.3 Go) a été observée saturée avec 4 jobs Spark
# simultanés (driver 512m + executor 600m chacun) + une requête ad-hoc en
# plus : le démon Docker devient injoignable (erreurs 500) et nécessite un
# restart. Avec SPARK_WORKER_CORES=2 (docker-compose.yml), le cluster ne
# PEUT de toute façon pas accepter plus de 2 jobs à la fois (1 cœur chacun) —
# c'est une garde-fou dur, pas juste une politique du script.
is_running() {
  curl -s http://127.0.0.1:8080/json/ | "$PYTHON_BIN" -c "
import sys, json
apps = json.load(sys.stdin)['activeapps']
sys.exit(0 if any(a['name'] == sys.argv[1] and a['state'] == 'RUNNING' for a in apps) else 1)
" "$1"
}

submit_job() {
  local app_name="$1" job_path="$2" extra_packages="$3" extra_conf="$4"
  if is_running "$app_name"; then
    echo "  - $app_name déjà RUNNING, on ne resoumet pas."
    return
  fi
  echo "  - soumission de $app_name ..."
  docker exec -d capteurs-spark-master bash -c "
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 512m \
      --total-executor-cores 1 \
      --executor-memory 600m \
      ${extra_packages:+--packages $extra_packages} \
      ${extra_conf:+--conf $extra_conf} \
      $job_path > /tmp/${app_name}.spark-submit.log 2>&1
  "
  sleep 3
}

stop_job() {
  # $1 = motif du chemin du job (ex: bronze.py) pour retrouver le PID du driver
  local pattern="$1"
  local pid
  pid=$(docker exec capteurs-spark-master ps aux 2>/dev/null | grep "SparkSubmit" | grep "$pattern" | grep -v grep | awk '{print $2}' | head -1)
  if [[ -n "$pid" ]]; then
    echo "  - arrêt de $pattern (pid $pid)"
    docker exec capteurs-spark-master kill "$pid" 2>/dev/null || true
    sleep 3
  fi
}

wait_apps() {
  # Attend que $1 applications soient RUNNING (jusqu'à 60s)
  local target="$1"
  for i in $(seq 1 30); do
    n=$(curl -s http://127.0.0.1:8080/json/ | "$PYTHON_BIN" -c "
import sys, json
apps = json.load(sys.stdin)['activeapps']
print(sum(1 for a in apps if a['state'] == 'RUNNING'))
")
    [[ "$n" -ge "$target" ]] && break
    sleep 2
  done
  curl -s http://127.0.0.1:8080/json/ | "$PYTHON_BIN" -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  cœurs utilisés: {d['coresused']}/{d['cores']}\")
for a in d['activeapps']:
    print(f\"  - {a['name']}: {a['state']}\")
"
}

log "Vague 1/2 : Bronze + Silver (2 cœurs)"
submit_job "bronze_mesures" "/jobs/bronze.py" \
  "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" ""
submit_job "silver_mesures" "/jobs/silver.py" \
  "io.delta:delta-spark_2.12:3.2.0" "spark.hadoop.fs.permissions.umask-mode=000"
wait_apps 2

# ---------------------------------------------------------------------------
if [[ "$SEED_COUNT" -gt 0 ]]; then
  log "8/8 Injection de $SEED_COUNT événements de démo (taux anomalie=$TAUX_ANOMALIE)"
  "$PYTHON_BIN" scripts/seed_kafka.py "$SEED_COUNT" "$TAUX_ANOMALIE" \
    | docker exec -i capteurs-kafka /opt/kafka/bin/kafka-console-producer.sh \
        --broker-list localhost:9092 --topic mesures
  echo "  Fait. Attente ~40s pour laisser Bronze puis Silver rattraper (trigger 10s chacun)."
  sleep 40
else
  echo "  Pas de seed demandé (utilisez --seed N pour injecter des données de démo)."
  sleep 15
fi

log "Fin de la vague 1 : arrêt de Bronze + Silver pour libérer les 2 cœurs"
stop_job "bronze.py"
stop_job "silver.py"
sleep 3

log "Vague 2/2 : Gold 1 + Gold 2 (2 cœurs) — consomment le Silver déjà écrit"
submit_job "gold_agg_fenetre_machine" "/jobs/gold_agg_fenetre.py" \
  "io.delta:delta-spark_2.12:3.2.0" "spark.hadoop.fs.permissions.umask-mode=000"
submit_job "gold_etat_courant_capteur" "/jobs/gold_etat_courant.py" \
  "io.delta:delta-spark_2.12:3.2.0" "spark.hadoop.fs.permissions.umask-mode=000"
wait_apps 2
echo "  Laissés en marche : ils mettront à jour Postgres/Power BI en continu"
echo "  dès que vous relancerez une vague Bronze+Silver (relancez ce script)."

echo
log "Terminé. Vérifications utiles :"
cat <<'EOF'
  curl -s http://127.0.0.1:8080/json/                                  # apps actives, cœurs utilisés
  docker exec capteurs-postgres psql -U capteurs -d capteurs -c \
    "SELECT count(*) FROM gold.etat_courant_capteur;"
  docker exec capteurs-postgres psql -U capteurs -d capteurs -c \
    "SELECT count(*) FROM gold.agg_fenetre_machine;"
EOF
