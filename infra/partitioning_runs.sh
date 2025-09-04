#!/usr/bin/env bash
# partitioning_runs.sh
# Esegue i test di partitioning per gli scenari: none, is_current, valid_from_ymd
#
# Usage: ./partitioning_runs.sh
#
set -u
# non attiviamo -e perché vogliamo gestire errori per scenario e continuare

### CONFIGURAZIONE (modifica se serve) ###
SPARK_HOME=/opt/spark
MASTER_HOST=10.0.1.7
MASTER_URL="spark://${MASTER_HOST}:7077"
BASE_PROJECT_DIR=/data/delta-lake-pyspark-scd2
LOGDIR=${BASE_PROJECT_DIR}/infra/partitioning_logs
DATA_DIR=/data
DELTA_BASE=${DATA_DIR}/delta
BATCH1_INPUT=${DATA_DIR}/crm_with_event_time/header/header_20230127.csv

HEADER_ETL=${BASE_PROJECT_DIR}/header_etl.py
PART_TEST=${BASE_PROJECT_DIR}/partitioning_test.py

# spark-submit common options
SPARK_SUBMIT_COMMON=(
  "${SPARK_HOME}/bin/spark-submit"
  --master "${MASTER_URL}"
  --deploy-mode client
  --conf "spark.sql.legacy.timeParserPolicy=CORRECTED"
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  --packages io.delta:delta-spark_2.12:3.1.0
  --conf "spark.local.dir=/tmp/spark_local"
)

# dove partitioning_test scrive le metriche (il tuo script le mette qui)
PART_METRICS_DIR=${DELTA_BASE}/metrics/partitioning_metrics

# scenari: "label:cols" -> cols vuoto significa nessuna colonna passata
SCENARIOS=(
  "none:"
  "is_current:is_current"
  "valid_from_ymd:valid_from_year,valid_from_month,valid_from_day"
)

# pulizia e preparazione logdir
mkdir -p "${LOGDIR}"
echo "Logs -> ${LOGDIR}"
echo

timestamp_now() {
  date -u +"%Y%m%dT%H%M%SZ"
}

for entry in "${SCENARIOS[@]}"; do
  label="${entry%%:*}"
  cols="${entry#*:}"   # se dopo ":" è vuoto -> cols == empty string

  ITER_TS=$(timestamp_now)
  ITER_DIR="${LOGDIR}/${label}_${ITER_TS}"
  mkdir -p "${ITER_DIR}"

  echo "=== SCENARIO: ${label} (ts=${ITER_TS}) ==="
  echo "cols = '${cols}'"
  echo "logs -> ${ITER_DIR}"
  echo

  # 1) CLEAN target delta (landing/discarded partitions for header)
  echo "[${label}] Cleaning delta landing/discarded (header) ..."
  # usa sudo se necessario
  sudo rm -rf "${DELTA_BASE}/landing/header/"* 2>/dev/null || true
  sudo rm -rf "${DELTA_BASE}/discarded/header/"* 2>/dev/null || true

  # 2) Lancia header_etl.py (con o senza colonne di partizione)
  echo "[${label}] Running header_etl.py ..."
  if [ -z "${cols}" ]; then
    "${SPARK_SUBMIT_COMMON[@]}" \
      "${HEADER_ETL}" \
      "${BATCH1_INPUT}" \
      "${DELTA_BASE}/" \
      > "${ITER_DIR}/header_etl.stdout.log" 2> "${ITER_DIR}/header_etl.stderr.log" || {
        echo "[${label}] header_etl FAILED (see ${ITER_DIR}/header_etl.stderr.log)"
        echo "Skipping partitioning_test for this scenario."
        continue
      }
  else
    "${SPARK_SUBMIT_COMMON[@]}" \
      "${HEADER_ETL}" \
      "${BATCH1_INPUT}" \
      "${DELTA_BASE}/" \
      "${cols}" \
      > "${ITER_DIR}/header_etl.stdout.log" 2> "${ITER_DIR}/header_etl.stderr.log" || {
        echo "[${label}] header_etl FAILED (see ${ITER_DIR}/header_etl.stderr.log)"
        echo "Skipping partitioning_test for this scenario."
        continue
      }
  fi

  echo "[${label}] header_etl completed. Logs in ${ITER_DIR}/header_etl.*"

  # 3) Esegui partitioning_test.py
  echo "[${label}] Running partitioning_test.py ..."
  python3 "${PART_TEST}" --part_path "${DELTA_BASE}/landing/header" \
    > "${ITER_DIR}/partitioning_test.stdout.log" 2> "${ITER_DIR}/partitioning_test.stderr.log" || {
      echo "[${label}] partitioning_test FAILED (see ${ITER_DIR}/partitioning_test.stderr.log)"
      # prosegui comunque per raccogliere eventuali metriche residue
  }

  # copia metriche prodotte dallo script partitioning_test (se esistono)
  mkdir -p "${ITER_DIR}/metrics"
  if [ -d "${PART_METRICS_DIR}" ]; then
    # prendi l'ultimo csv creato (ordina per tempo di modifica)
    latest_metric=$(ls -1t "${PART_METRICS_DIR}"/*.csv 2>/dev/null | head -n 1 || true)
    if [ -n "${latest_metric}" ]; then
      cp "${latest_metric}" "${ITER_DIR}/metrics/$(basename "${latest_metric%.*}")__${label}__${ITER_TS}.csv"
      echo "[${label}] Copied metrics: ${latest_metric} -> ${ITER_DIR}/metrics/"
    else
      echo "[${label}] No metrics CSV found in ${PART_METRICS_DIR}"
    fi
  else
    echo "[${label}] Metrics dir ${PART_METRICS_DIR} does not exist"
  fi

  echo "=== SCENARIO ${label} completed. Results in ${ITER_DIR} ==="
  echo
done

echo "All scenarios done."
