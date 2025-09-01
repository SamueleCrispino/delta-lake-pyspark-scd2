#!/usr/bin/env bash
set -euo pipefail

###########################
# scale_horizontal_runner.sh
# Versione semplificata (workers file contiene solo IP per riga)
###########################

### CONFIGURA QUI (modifica secondo ambiente) ###
SPARK_HOME=/opt/spark
WORKERS_FILE=/opt/spark/conf/workers
MASTER_HOST=10.0.1.7
MASTER_URL="spark://${MASTER_HOST}:7077"
BASE_PROJECT_DIR=/data/delta-lake-pyspark-scd2
LOGDIR=${BASE_PROJECT_DIR}/infra/scale_logs
DATA_DIR=/data
WRITE_BASE=/data/delta/
BATCH1_IN=${DATA_DIR}/crm_with_event_time/header/header_20230127.csv
BATCH2_IN=${DATA_DIR}/crm_with_event_time/header/header_20230228.csv

# spark-submit common options (modifica se serve)
SPARK_SUBMIT_COMMON=(
  "${SPARK_HOME}/bin/spark-submit"
  --master "${MASTER_URL}"
  --deploy-mode client
  --conf "spark.sql.legacy.timeParserPolicy=CORRECTED"
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  --packages io.delta:delta-spark_2.12:3.1.0
  --conf "spark.local.dir=/tmp/spark_local"
)

# timeout / waits
WAIT_AFTER_START=10   # secondi per aspettare che i cluster si avviino (aggiusta se necessario)
WAIT_AFTER_STOP=5

# end config
#################################################

mkdir -p "${LOGDIR}"
echo "Logs -> ${LOGDIR}"
echo "Workers file: ${WORKERS_FILE}"
echo

confirm() {
  read -r -p "$1 [y/N] " resp
  case "$resp" in
    [yY][eE][sS]|[yY]) true ;;
    *) false ;;
  esac
}

if ! confirm "Vuoi procedere? (modifica le variabili nello script se necessario)"; then
  echo "Aborted by user."
  exit 1
fi

iteration=0

# safety: make a timestamped backup of workers file
sudo cp "${WORKERS_FILE}" "${WORKERS_FILE}.bak.$(date +%Y%m%dT%H%M%S)"
echo "Backup saved to ${WORKERS_FILE}.bak.*"

while true; do
  iteration=$((iteration+1))
  echo
  echo "=== ITERATION ${iteration} === $(date -u +%FT%TZ)"

  # read active (non-empty) workers (strip whitespace)
  mapfile -t active_workers < <(awk 'NF{print $1}' "${WORKERS_FILE}" || true)
  if [ "${#active_workers[@]}" -eq 0 ]; then
    echo "No active workers left -> finished."
    break
  fi

  echo "Active workers (${#active_workers[@]}):"
  for w in "${active_workers[@]}"; do echo "  - $w"; done

  # --- 1) CLEAN target dirs (run data)
  echo "Cleaning previous run data (landing/discarded partitions)..."
  if [ -d "${WRITE_BASE}/landing/header" ]; then
    sudo rm -rf "${WRITE_BASE}/landing/header/"* || true
  fi
  if [ -d "${WRITE_BASE}/discarded/header" ]; then
    sudo rm -rf "${WRITE_BASE}/discarded/header/"* || true
  fi

  # --- 2) ensure workers file contains exactly active_workers (we already have it)
  # rewrite canonical workers file (this is idempotent)
  tmp_workers=$(mktemp)
  for w in "${active_workers[@]}"; do
    printf "%s\n" "${w}" >> "${tmp_workers}"
  done
  echo "Installing workers file (current active workers)..."
  sudo cp "${tmp_workers}" "${WORKERS_FILE}"
  sudo chown root:root "${WORKERS_FILE}" || true
  sudo chmod 644 "${WORKERS_FILE}" || true
  rm -f "${tmp_workers}"

  # --- 3) start cluster (ensure a clean start)
  echo "Stopping any running cluster (safe)..."
  sudo "${SPARK_HOME}/sbin/stop-all.sh" || true
  sleep 2
  echo "Starting Spark cluster..."
  sudo "${SPARK_HOME}/sbin/start-all.sh"
  echo "Waiting ${WAIT_AFTER_START}s for cluster to settle..."
  sleep "${WAIT_AFTER_START}"

  # iteration logdir
  LOGDIR_ITER="${LOGDIR}/iter_${iteration}"
  mkdir -p "${LOGDIR_ITER}"

  echo "Running batch1 (initial load)..."
  # run batch1
  "${SPARK_SUBMIT_COMMON[@]}" \
    "${BASE_PROJECT_DIR}/header_etl.py" \
    "${BATCH1_IN}" \
    "${WRITE_BASE}" \
    > "${LOGDIR_ITER}/batch1.stdout.log" 2> "${LOGDIR_ITER}/batch1.stderr.log" || {
      echo "batch1 failed; see ${LOGDIR_ITER}/batch1.stderr.log"
  }

  echo "Running batch2 (merge)..."
  "${SPARK_SUBMIT_COMMON[@]}" \
    "${BASE_PROJECT_DIR}/header_etl.py" \
    "${BATCH2_IN}" \
    "${WRITE_BASE}" \
    > "${LOGDIR_ITER}/batch2.stdout.log" 2> "${LOGDIR_ITER}/batch2.stderr.log" || {
      echo "batch2 failed; see ${LOGDIR_ITER}/batch2.stderr.log"
  }

  # collect some logs: list landing files count etc
  {
    echo "Listing landing content (sample):"
    sudo ls -R "${WRITE_BASE}/landing/header" 2>&1 || true
    echo
    echo "discarded files:"
    sudo ls -R "${WRITE_BASE}/discarded/header" 2>&1 || true
  } > "${LOGDIR_ITER}/summary.txt"

  # --- 4) stop cluster
  echo "Stopping Spark cluster..."
  sudo "${SPARK_HOME}/sbin/stop-all.sh"
  sleep "${WAIT_AFTER_STOP}"

  # --- 5) remove last active worker from workers file (so next run has one fewer)
  if [ "${#active_workers[@]}" -le 1 ]; then
    echo "Only one active worker left; will remove it and finish after this iteration."
    new_active=()
  else
    # keep all except last element
    new_active=("${active_workers[@]:0:${#active_workers[@]}-1}")
  fi

  tmp_workers2=$(mktemp)
  for w in "${new_active[@]}"; do
    printf "%s\n" "${w}" >> "${tmp_workers2}"
  done
  echo "Updating workers file to remove last worker for next iteration..."
  sudo cp "${tmp_workers2}" "${WORKERS_FILE}"
  sudo chown root:root "${WORKERS_FILE}" || true
  sudo chmod 644 "${WORKERS_FILE}" || true
  rm -f "${tmp_workers2}"

  echo "Iteration ${iteration} completed. Logs: ${LOGDIR_ITER}"
  echo "Sleeping 3s before next iteration..."
  sleep 3
done

echo "All iterations completed."
