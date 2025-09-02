#!/usr/bin/env bash
set -euo pipefail

########## CONFIG ##########
SPARK_HOME=/opt/spark
MASTER_HOST=10.0.1.7
MASTER_URL="spark://${MASTER_HOST}:7077"
BASE_PROJECT_DIR=/data/delta-lake-pyspark-scd2
WRITE_BASE=/data/delta/landing
DELTA_BASE=/data/delta
DATA_DIR=/data/crm_with_event_time/header
METRICS_BASE=/data/delta/metrics/partitioning_metrics
HEADER_ETL=${BASE_PROJECT_DIR}/header_etl.py
PARTITION_TEST=${BASE_PROJECT_DIR}/partitioning_test.py    # aggiorna se path diverso

# file di input batch1 (usato da header_etl)
BATCH1_INPUT=${DATA_DIR}/header_20230127.csv

# spark-submit common
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
WAIT_AFTER_ETL=5

# partitioning scenarios: each element is "label:columns"
# label sarà usato nei nomi delle directory/metrica; columns è ciò che verrà passato come 3° arg
SCENARIOS=(
  "none:" 
  "is_current:is_current"
  "valid_from_ymd:valid_from_year,valid_from_month,valid_from_day"
)

# Where header_etl writes by default:
WRITE_TABLE_PATH="${DELTA_BASE}/landing/header"

mkdir -p "${METRICS_BASE}"

echo "=== Partitioning runs starting ==="
echo "Metrics saved to: ${METRICS_BASE}"
echo

for sc in "${SCENARIOS[@]}"; do
  # split label:cols
  label="${sc%%:*}"
  cols="${sc#*:}"   # empty if none

  ts=$(date -u +"%Y%m%dT%H%M%SZ")
  echo "------"
  echo "Scenario: ${label} (cols='${cols}')  (${ts})"
  echo "Cleaning landing/discarded for header..."
  sudo rm -rf "${WRITE_TABLE_PATH}"/* || true
  sudo rm -rf "${DELTA_BASE}/discarded/header"/* || true

  # run header_etl: pass third arg only if cols non empty
  echo "Running header_etl.py (partition columns: ${cols:-<none>}) ..."
  if [ -z "${cols}" ]; then
    "${SPARK_SUBMIT_COMMON[@]}" "${HEADER_ETL}" "${BATCH1_INPUT}" "${DELTA_BASE}/" || {
      echo "header_etl failed for scenario ${label} (see logs)"
      continue
    }
  else
    "${SPARK_SUBMIT_COMMON[@]}" "${HEADER_ETL}" "${BATCH1_INPUT}" "${DELTA_BASE}/" "${cols}" || {
      echo "header_etl failed for scenario ${label} (see logs)"
      continue
    }
  fi

  echo "Waiting ${WAIT_AFTER_ETL}s..."
  sleep "${WAIT_AFTER_ETL}"

  # names for copies
  PART_PATH="${DELTA_BASE}/landing/header_part_${label}_${ts}"
  NOPART_PATH="${DELTA_BASE}/landing/header_nopart_${label}_${ts}"

  echo "Creating partitioned copy -> ${PART_PATH} (fast copy of folder)"
  # copy the whole delta folder (fast, keeps same physical layout)
  sudo rm -rf "${PART_PATH}" || true
  sudo cp -a "${WRITE_TABLE_PATH}" "${PART_PATH}"

  echo "Creating non-partitioned rewrite -> ${NOPART_PATH} (read and rewrite without partitionBy)"
  sudo rm -rf "${NOPART_PATH}" || true

  # create a small temp python script to rewrite table without partitions
  TMP_PY="/tmp/rewrite_nopart_${label}_${ts}.py"
  cat > "${TMP_PY}" <<PY
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
in_path = sys.argv[1]
out_path = sys.argv[2]
spark = SparkSession.builder \
    .appName("rewrite_nopart") \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
df = spark.read.format("delta").load(in_path)
# optional: coalesce to reasonable number of files
df.coalesce(64).write.format("delta").mode("overwrite").save(out_path)
spark.stop()
PY

  "${SPARK_SUBMIT_COMMON[@]}" "${TMP_PY}" "${PART_PATH}" "${NOPART_PATH}" || {
    echo "Failed to rewrite nopart table for scenario ${label}"
    rm -f "${TMP_PY}"
    continue
  }
  rm -f "${TMP_PY}"

  echo "Running partitioning_test comparing nopart(${NOPART_PATH}) vs part(${PART_PATH}) ..."
  MET_OUT="${METRICS_BASE}/partitioning_${label}_${ts}.csv"
  "${SPARK_SUBMIT_COMMON[@]}" "${PARTITION_TEST}" --nopart_path "${NOPART_PATH}" --part_path "${PART_PATH}" \
    > "${METRICS_BASE}/run_${label}_${ts}.out" 2> "${METRICS_BASE}/run_${label}_${ts}.err" || {
      echo "partitioning_test failed for ${label} (see logs ${METRICS_BASE}/run_${label}_${ts}.err)"
  }

  # move produced csv (partitioning_test uses pandas to save direct CSV in METRICS_BASE)
  # Check if partitioning_test already wrote a csv; if yes do nothing; else try to capture its stdout
  echo "Iteration ${label} done. Metrics (if produced) are under ${METRICS_BASE}"
  echo
done

echo "All scenarios done."
