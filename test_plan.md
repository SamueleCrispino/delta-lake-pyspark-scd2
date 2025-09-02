# Scalability Tests Plan – Spark + Delta (Entità `header`)

## 1. Generazione dei dataset

Gli input devono simulare l’entità `header`. Si usano due batch:

- **Batch 1** → prima versione della tabella (nuovi contratti)
- **Batch 2** → contiene una combinazione di:
  - nuovi contratti mai visti
  - update su contratti già presenti (`event_time` diverso → SCD2 intraday)

Script di generazione (`generate_header_datasets.py`):

```bash
spark-submit generate_header_datasets.py \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2
Lo script crea i file in:
    /data/delta/header_input/batch1
    /data/delta/header_input/batch2
    
Modificare la variabile N dentro lo script per scalare il volume (es. 100k, 1M, 10M righe).

## 2. Test di scalabilità

### 2.1 Scalabilità Orizzontale
Definizione: stesso dataset, numero di worker variabile.

Imposta N fisso (es. 1M righe per batch).

Lancia il cluster Spark variando la lista di nodi in /opt/spark/conf/workers:

1 worker
2 worker
4 worker
8 worker

Dopo ogni modifica:
/opt/spark/sbin/stop-all.sh
/opt/spark/sbin/start-all.sh

Esegui il job ETL su Batch1 e Batch2:


spark-submit header_etl.py \
  --input /data/delta/header_input/batch1 \
  --input /data/delta/header_input/batch2 \
  --output /data/delta/header_output


### 2.2 Scalabilità Verticale
Definizione: stesso cluster, dataset crescente.

Fissa il cluster (es. 4 worker, 4 core, 8GB RAM ciascuno).

Genera dataset di dimensioni diverse:

100k righe
1M righe
10M righe
50M righe

Per ogni dataset:

spark-submit header_etl.py \
  --input /data/delta/header_input/batch1 \
  --input /data/delta/header_input/batch2 \
  --output /data/delta/header_output

Scalabilità temporale vs dimensione dati (idealmente lineare)

Utilizzo risorse (se satura CPU o memoria)

4. Checklist
 Dataset generati correttamente (batch1, batch2)

 Cluster Spark avviato con il numero corretto di worker

 Job header_etl.py eseguito su entrambi i batch

 Metriche raccolte in CSV

 Risultati confrontati (scalabilità orizzontale vs verticale)

 !!!! Eseguire anche valutazione da UI


 ########### SCHEMA EVOLUTION TESTS  #########

[
  {
    "version": 0,
    "timestamp": "2025-09-02 16:46:02.784",
    "userId": null,
    "userName": null,
    "operation": "WRITE",
    "operationParameters": {
      "mode": "ErrorIfExists",
      "partitionBy": [
        "valid_from_year",
        "valid_from_month",
        "valid_from_day"
      ]
    },
    "job": null,
    "notebook": null,
    "clusterId": null,
    "readVersion": null,
    "isolationLevel": "Serializable",
    "isBlindAppend": true,
    "operationMetrics": {
      "numFiles": 67,
      "numOutputRows": 9583937,
      "numOutputBytes": 298645277
    },
    "userMetadata": null,
    "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.1.0"
  }
]


export SPARK_HOME=/opt/spark

$SPARK_HOME/bin/spark-submit \
  --master spark://10.0.1.7:7077 \
  --deploy-mode client \
  --conf "spark.sql.legacy.timeParserPolicy=CORRECTED" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  /data/delta-lake-pyspark-scd2/schema_evolution_step1.py \
    --delta_path /data/delta/landing/header \
    --metrics_base /data/delta/metrics/schema_evolution
