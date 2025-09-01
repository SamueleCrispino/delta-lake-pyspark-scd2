#!/usr/bin/env bash
set -euo pipefail

###############################
# scale_size_runner.sh
# Automatizza test per dimensioni dataset crescenti
###############################

# === CONFIG ===
SPARK_HOME=/opt/spark
MASTER_HOST=10.0.1.7
MASTER_URL="spark://${MASTER_HOST}:7077"
BASE_PROJECT_DIR=/data/delta-lake-pyspark-scd2
DATA_DIR=/data
WRITE_BASE=/data/delta
LOGDIR=${BASE_PROJECT_DIR}/infra/scale_size_logs
GENERATE_SCRIPT=${BASE_PROJECT_DIR}/utils/generate_header_datasets.py
HEADER_ETL=${BASE_PROJECT_DIR}/header_etl.py

# lista dimensioni (righe)
SIZES=(100000 1000000 5000000 10000000)

# parametri per il generate script (modifica se vuoi)
PARTITIONS=32
PCT_NEW=50
PCT_MULTI_EVENT=5
SEED=42
BATCH1_DATE=20230127
BATCH2_DATE=20230228
OUTDIR="${DATA_DIR}/crm_with_event_time/header"

# spark-submit common options (aggiorna / aggiungi conf come preferisci)
SPARK_SUBMIT_COMMON=( 
  "${SPARK_HOME}/bin/spark-submit"
  --master "${MASTER_URL}"
  --deploy-mode client
  --conf "spark.sql.legacy.timeParserPolicy=CORRECTED"
  --conf "spark.local.dir=/tmp/spark_local"
  --conf "spark.sql.shuffle.partitions=${PARTITIONS}"
  --packages io.delta:delta-spark_2.12:3.1.0
)

# header_etl spark-submit extra conf
HEADER_ETL_CONF=( 
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

# sleeps / timeouts
SLEEP_AFTER_GENERATE=5
SLEEP_AFTER_ETL=5

# cleanup toggles
CLEAN_INPUT_BEFORE_GENERATE=true   # cancella /data/crm_with_event_time/header/* prima di generare
CLEAN_DELTA_BEFORE_RUN=true       # cancella landing/discarded prima di eseguire i job
KEEP_METRICS=true                 # non cancellare /data/delta/metrics

# safety checks
if [ ! -f "${GENERATE_SCRIPT}" ]; then
  echo "ERROR: generate script not found: ${GENERATE_SCRIPT}"
  exit 2
fi
if [ ! -f "${HEADER_ETL}" ]; then
  echo "ERROR: header_etl not found: ${HEADER_ETL}"
  exit 2
fi

mkdir -p "${LOGDIR}"
echo "Logs -> ${LOGDIR}"
echo "START scale_size_runner: $(date -u +%FT%TZ)"
echo

iteration=0
for size in "${SIZES[@]}"; do
  iteration=$((iteration+1))
  ITER_DIR="${LOGDIR}/size_${size}"
  mkdir -p "${ITER_DIR}"

  echo "=== SIZE ${size} (iteration ${iteration}) === $(date -u +%FT%TZ)" | tee "${ITER_DIR}/run.log"

  # --- CLEAN input header files (optional)
  if [ "${CLEAN_INPUT_BEFORE_GENERATE}" = true ]; then
    echo "Cleaning input folder: ${OUTDIR} (preserve metrics)" | tee -a "${ITER_DIR}/run.log"
    # rimuove TUTTO sotto OUTDIR in modo sicuro; l'espansione ${OUTDIR:?} evita rm -rf / se OUTDIR è vuoto
    sudo rm -rf "${OUTDIR:?}/"* || true
    # ricrea la directory (se non esiste ancora)
    sudo mkdir -p "${OUTDIR}"
    # sistema ownership/permessi se necessario (opzionale, decommenta se vuoi)
    # sudo chown -R Crispinoadmin:Crispinoadmin "${OUTDIR}"
    # sudo chmod -R 2775 "${OUTDIR}"
  fi


  # --- CLEAN delta landing/discarded (but keep metrics if configured)
  if [ "${CLEAN_DELTA_BEFORE_RUN}" = true ]; then
    echo "Cleaning delta landing/discarded for header..." | tee -a "${ITER_DIR}/run.log"
    sudo rm -rf "${WRITE_BASE}/landing/header/*" || true
    sudo rm -rf "${WRITE_BASE}/discarded/header/*" || true
    # keep metrics dir untouched when KEEP_METRICS=true
    if [ "${KEEP_METRICS}" = false ]; then
      sudo rm -rf "${WRITE_BASE}/metrics/*" || true
    fi
  fi

  # --- 1) GENERATE datasets (spark-submit)
  echo "Generating datasets (size=${size})..." | tee -a "${ITER_DIR}/run.log"
  "${SPARK_SUBMIT_COMMON[@]}" \
    --conf "spark.sql.shuffle.partitions=${PARTITIONS}" \
    "${GENERATE_SCRIPT}" \
      --size "${size}" \
      --outdir "${OUTDIR}" \
      --partitions "${PARTITIONS}" \
      --pct_new "${PCT_NEW}" \
      --pct_multi_event "${PCT_MULTI_EVENT}" \
      --seed "${SEED}" \
      --batch1_date "${BATCH1_DATE}" \
      --batch2_date "${BATCH2_DATE}" \
    > "${ITER_DIR}/generate.stdout.log" 2> "${ITER_DIR}/generate.stderr.log" || {
      echo "generate failed for size ${size}, see ${ITER_DIR}/generate.stderr.log" | tee -a "${ITER_DIR}/run.log"
      # continue to next size
      continue
    }

  sleep "${SLEEP_AFTER_GENERATE}"

  # --- 2) RUN header_etl on batch1 (initial load)
  echo "Running header_etl (batch1)..." | tee -a "${ITER_DIR}/run.log"
  "${SPARK_SUBMIT_COMMON[@]}" "${HEADER_ETL_CONF[@]}" \
    "${HEADER_ETL}" \
    "${OUTDIR}/header_${BATCH1_DATE}.csv" \
    "${WRITE_BASE}/" \
    > "${ITER_DIR}/batch1.stdout.log" 2> "${ITER_DIR}/batch1.stderr.log" || {
      echo "batch1 failed for size ${size}; see ${ITER_DIR}/batch1.stderr.log" | tee -a "${ITER_DIR}/run.log"
      # continue to try batch2 (or skip) — depends on policy; here we'll continue
  }

  sleep "${SLEEP_AFTER_ETL}"

  # --- 3) RUN header_etl on batch2 (merge)
  echo "Running header_etl (batch2)..." | tee -a "${ITER_DIR}/run.log"
  "${SPARK_SUBMIT_COMMON[@]}" "${HEADER_ETL_CONF[@]}" \
    "${HEADER_ETL}" \
    "${OUTDIR}/header_${BATCH2_DATE}.csv" \
    "${WRITE_BASE}/" \
    > "${ITER_DIR}/batch2.stdout.log" 2> "${ITER_DIR}/batch2.stderr.log" || {
      echo "batch2 failed for size ${size}; see ${ITER_DIR}/batch2.stderr.log" | tee -a "${ITER_DIR}/run.log"
  }

  # --- post-run: collect some quick summaries
  echo "Collecting summaries..." | tee -a "${ITER_DIR}/run.log"
  echo "Listing landing (top-level) -> ${ITER_DIR}/summary_landing.txt" 
  sudo ls -l "${WRITE_BASE}/landing/header" > "${ITER_DIR}/summary_landing.txt" 2>&1 || true
  echo "Listing discarded -> ${ITER_DIR}/summary_discarded.txt"
  sudo ls -l "${WRITE_BASE}/discarded/header" > "${ITER_DIR}/summary_discarded.txt" 2>&1 || true

  # copy the metrics files (if present) for quick reference
  mkdir -p "${ITER_DIR}/metrics_snapshot"
  if [ -d "${WRITE_BASE}/metrics" ]; then
    sudo cp -r "${WRITE_BASE}/metrics" "${ITER_DIR}/metrics_snapshot/" 2>/dev/null || true
  fi

  echo "Iteration for size ${size} completed. Logs in ${ITER_DIR}" | tee -a "${ITER_DIR}/run.log"
  echo
  # small wait before next iteration
  sleep 3
done

echo "All sizes processed. Logs in ${LOGDIR}"
