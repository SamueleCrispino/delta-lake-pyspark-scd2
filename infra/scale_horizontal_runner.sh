#!/usr/bin/env bash
set -euo pipefail

###########################
# scale_horizontal_runner.sh
# Automatizza i test di scaling orizzontale:
# - pulisce dati
# - start cluster
# - esegue 2 spark-submit
# - stop cluster
# - rimuove (commenta) ultimo worker dal file /opt/spark/conf/workers
# ripete finché non ci sono più workers attivi
#
# USO: esegui sul Manager (es. 10.0.1.7)
# prima: sudo cp /opt/spark/conf/workers /opt/spark/conf/workers.bak
#
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

  # read active (non-commented, non-empty) workers
  mapfile -t active_workers < <(grep -E -v '^\s*#' "${WORKERS_FILE}" | sed '/^\s*$/d' || true)
  if [ "${#active_workers[@]}" -eq 0 ]; then
    echo "No active workers left -> finished."
    break
  fi

  echo "Active workers (${#active_workers[@]}):"
  for w in "${active_workers[@]}"; do echo "  - $w"; done

  # --- 1) CLEAN target dirs (run data)
  echo "Cleaning previous run data (landing/discarded partitions)..."
  # usa sudo se /data è owned da root/nobody
  sudo rm -rf "${WRITE_BASE}/landing/header/*" || true
  sudo rm -rf "${WRITE_BASE}/discarded/header/*" || true

  # --- 2) ensure workers file contains exactly active_workers + original commented lines
  # keep commented lines at end to preserve history
  commented_lines=$(grep -E '^\s*#' "${WORKERS_FILE}" || true)
  tmp_workers=$(mktemp)
  for w in "${active_workers[@]}"; do
    printf "%s\n" "${w}" >> "${tmp_workers}"
  done
  if [ -n "${commented_lines}" ]; then
    printf "%s\n" "${commented_lines}" >> "${tmp_workers}"
  fi
  echo "Installing workers file (current active workers)..."
  sudo cp "${tmp_workers}" "${WORKERS_FILE}"
  rm -f "${tmp_workers}"

  # --- 3) start cluster
  echo "Starting Spark cluster..."
  sudo "${SPARK_HOME}/sbin/start-all.sh"
  echo "Waiting ${WAIT_AFTER_START}s for cluster to settle..."
  sleep "${WAIT_AFTER_START}"

  # optional: dump list of nodes from master UI (if accessible)
  echo "Master URL: ${MASTER_URL}"
  echo "Running batch1 (initial load)..."
  LOGDIR_ITER="${LOGDIR}/iter_${iteration}"
  mkdir -p "${LOGDIR_ITER}"

  # run batch1
  "${SPARK_SUBMIT_COMMON[@]}" \
    "${BASE_PROJECT_DIR}/header_etl.py" \
    "${BATCH1_IN}" \
    "${WRITE_BASE}" \
    > "${LOGDIR_ITER}/batch1.stdout.log" 2> "${LOGDIR_ITER}/batch1.stderr.log" || {
      echo "batch1 failed; see ${LOGDIR_ITER}/batch1.stderr.log"
      # continue to stop cluster and proceed with next iteration
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
  echo "Listing landing content (sample):" > "${LOGDIR_ITER}/summary.txt"
  echo "landing files:" >> "${LOGDIR_ITER}/summary.txt"
  sudo ls -R "${WRITE_BASE}/landing/header" >> "${LOGDIR_ITER}/summary.txt" 2>&1 || true
  echo >> "${LOGDIR_ITER}/summary.txt"
  echo "discarded files:" >> "${LOGDIR_ITER}/summary.txt"
  sudo ls -R "${WRITE_BASE}/discarded/header" >> "${LOGDIR_ITER}/summary.txt" 2>&1 || true

  # --- 4) stop cluster
  echo "Stopping Spark cluster..."
  sudo "${SPARK_HOME}/sbin/stop-all.sh"
  sleep "${WAIT_AFTER_STOP}"

  # --- 5) comment out last active worker (so next run has one fewer)
  # build new workers file with all active except last; preserve commented lines
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
  # append original commented lines (if any)
  if [ -n "${commented_lines}" ]; then
    printf "%s\n" "${commented_lines}" >> "${tmp_workers2}"
  fi

  echo "Updating workers file to remove last worker for next iteration..."
  sudo cp "${tmp_workers2}" "${WORKERS_FILE}"
  rm -f "${tmp_workers2}"

  echo "Iteration ${iteration} completed. Logs: ${LOGDIR_ITER}"
  echo "Sleeping 3s before next iteration..."
  sleep 3
done

echo "All iterations completed."
