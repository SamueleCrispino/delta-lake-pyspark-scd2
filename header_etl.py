import os, json, traceback
from datetime import datetime
import re
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, concat, input_file_name, current_timestamp, date_format, col, to_date, trim, when, count as spark_count, collect_set, size, expr, to_timestamp, lit, lead, year, month, dayofmonth, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType, TimestampType
from functools import reduce
from operator import or_
from delta.tables import *
from pyspark.sql import Window
import requests

from utils.validations_utils import validation
from utlis.write_metrics import write_run_metrics_spark

# DEBUG:
print("pyspark version: ", pyspark.__version__)

##### CONSTANTS
MAX_TS = "9999-12-31 00:00:00"
# date_regex = r'(.+)_(\d{8})\.csv' TODO: make it dynamic
date_regex = r'header_(\d{8})\.csv'


deduplication_keys = ["contratto_cod", "event_time"]
partition_columns = ["valid_from_year", "valid_from_month", "valid_from_day"]



# for job start's timestamp
timestamp_format = "yyyyMMddHHmmss"

# HEADER SCHEMA
header_schema = StructType([
    StructField("contratto_cod", StringType(), nullable=False),
    StructField("codice_ordine_sap", StringType(), nullable=True),
    StructField("tipo_contratto", StringType(), nullable=False),
    StructField("codice_opec", StringType(), nullable=False),
    StructField("data_firma", StringType(), nullable=True),        # mantieni string per ora
    StructField("net_amount", StringType(), nullable=False),
    StructField("causale_annullamento", StringType(), nullable=True),
    StructField("data_annullamento", StringType(), nullable=True),
    StructField("codice_agente", StringType(), nullable=False),
    StructField("status_quote", StringType(), nullable=True),
    StructField("creazione_dta", StringType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=True)   
])


def run_job_header(spark, read_path, write_path, discarded_path, metrics_path):

    start_ts = datetime.utcnow().isoformat()

    print("READ_PATH: ", read_path)
    print("WRITE_PATH: ", write_path)


    # EXTRACT
    start_ts_extract = datetime.utcnow().isoformat()
    df_extracted = spark.read.option("header", "true").option("sep", "|")\
                    .schema(header_schema)\
                    .csv(read_path)\
                    .withColumn("closed_by_batch", lit(None).cast(StringType()))\
                    .withColumn("source_file", input_file_name())\
                    .withColumn("ingest_ts", current_timestamp())\
                    .withColumn("batch_id", concat(date_format(current_timestamp(), timestamp_format), lit("_"), input_file_name()))

    batch_id_row = df_extracted.select("batch_id").limit(1).collect()
    batch_id = batch_id_row[0]["batch_id"]

    end_ts_extract = datetime.utcnow().isoformat()
    duration_s_extract = (datetime.fromisoformat(end_ts_extract) - datetime.fromisoformat(start_ts_extract)).total_seconds()
    
    ### APPLYING VALIDATIONS RULES:
    start_ts_validation = datetime.utcnow().isoformat()
    validated_df, dq_metrics = validation(df_extracted, deduplication_keys, spark, date_regex, discarded_path, batch_id)
    end_ts_validation = datetime.utcnow().isoformat()
    duration_s_validation = (datetime.fromisoformat(end_ts_validation) - datetime.fromisoformat(start_ts_validation)).total_seconds()
    
    
    ### TRANSFORM and APPLYING INTRA-BATCH VERSIONS
    # 1) Parse event_time in timestamp con fallback 
    # (se parsing fallisce usa mezzanotte della valid_from date)
    start_ts_transform = datetime.utcnow().isoformat()
    df_parsed = validated_df.withColumn(
        "event_time_ts",
        coalesce(
            # ISO with milliseconds and offset, es: 2023-01-21T23:00:00.000+01:00
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            # ISO without ms, with offset: 2023-01-21T23:00:00+01:00
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            # common formats
            to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("event_time"), "yyyy-MM-dd"),
            # ultimate fallback: try Spark default parsing (may parse some ISO strings)
            to_timestamp(col("event_time"))
        )
    )

    # 2) Genera version rows intra-batch con lead()
    
    w = Window.partitionBy("contratto_cod").orderBy(col("event_time_ts").asc())

    df_versions = (
        df_parsed
        .withColumn("valid_from_ts", col("event_time_ts"))
        .withColumn("next_event_time", lead("event_time_ts").over(w))
        .withColumn("valid_to_ts", when(col("next_event_time").isNull(), to_timestamp(lit(MAX_TS)))\
                                    .otherwise(col("next_event_time")))
        .withColumn("is_current", col("next_event_time").isNull())
        .drop("next_event_time")
    )

    df_versions = df_versions.drop("event_time")
    df_versions = df_versions.drop("event_time_ts")

    # 3) Deriva colonne di partizione da valid_from_ts (date)
    df_transformed = df_versions\
                         .withColumn("valid_from_year", year("valid_from_ts")) \
                         .withColumn("valid_from_month", month("valid_from_ts")) \
                         .withColumn("valid_from_day", dayofmonth("valid_from_ts"))
    
    
    df_transformed = df_transformed.withColumn("creazione_dta_raw", trim(col("creazione_dta"))) \
               .withColumn("creazione_dta_raw", when(col("creazione_dta_raw") == "", None).otherwise(col("creazione_dta_raw")))

    df_transformed = df_transformed.withColumn(
        "creazione_dta_parsed",
        # prova MM/dd/yyyy (scritta come 'M/d/yyyy' per coprire 1/2/2022 e 11/25/2022)
        # fallback su ISO yyyy-MM-dd
        expr("coalesce(to_date(creazione_dta_raw, 'M/d/yyyy'), to_date(creazione_dta_raw, 'yyyy-MM-dd'))")
    )
    end_ts_transform = datetime.utcnow().isoformat()
    duration_s_transform = (datetime.fromisoformat(end_ts_transform) - datetime.fromisoformat(start_ts_transform)).total_seconds()

    print("AFTER TRANSFORM PHASE:")
    df_transformed.show(truncate=False)


    #### SLOW CHANGE DATA - II

    # ogni evento viene trasformato in una version row con:
    #  valid_from = event_time and valid_to = next_event_time
    # Per evitare sovrapposizioni e garantire idempotenza applichiamo il merge in 2 fasi:
    ## Fase A (close existing) chiudiamo la riga attiva già presente nella tabella
     #### impostando il suo valid_to al primo evento intrabatch (first_event_time).
     #### Questo deve avvenire una volta sola per contratto_cod e non N volte in base
        #### al numero di contratto_cod duplicati nello stesso batch
    ## Fase B (insert versions) inseriamo tutte le version rows (una per evento intrabatch). 
     #### L’inserimento è idempotente (merge su contratto_cod + valid_from)

    start_ts_merge = datetime.utcnow().isoformat()
    if not DeltaTable.isDeltaTable(spark, write_path):
        print("INIT DELTA TABLE")
        df_transformed.write.partitionBy(partition_columns).format("delta").save(write_path)
        
        return 

    print("FOUND DELTA TABLE APPLYING MERGING")

    delta_table = DeltaTable.forPath(spark, write_path)

    # ------------- FASE A (Opzione B: chiudi only on real change) ----------------
    # 1) prendi le righe "current" dalla tabella esistente: valid_to = MAX_TS o null
    # seleziona solo le colonne di interesse per il confronto (riduce i dati nel join)
    existing_current = delta_table.toDF().filter(
        (col("valid_to_ts").isNull()) | 
        (col("valid_to_ts") == to_timestamp(lit(MAX_TS)))
    ).select(
        "contratto_cod",
        "status_quote",
        "codice_agente",
        "codice_ordine_sap",
        "valid_from_ts"
    )
    
    # 2) join tra tutti gli eventi intrabatch (df_versions) e l'existing_current su contratto_cod
    joined = df_versions.alias("st").join(
        existing_current.alias("ex"), 
        on="contratto_cod", 
        how="inner"
    )
    
    # 3) definisci espressione di differenza NULL-safe sui campi che vuoi considerare
    diff_expr = (
        "NOT (st.status_quote <=> ex.status_quote) OR "
        "NOT (st.codice_agente <=> ex.codice_agente) OR "
        "NOT (st.codice_ordine_sap <=> ex.codice_ordine_sap)"
    )

    # 4) filtra gli eventi che effettivamente introducono un cambiamento rispetto all'existing
    changed_events = joined.filter(expr(diff_expr)).select("st.contratto_cod", "st.valid_from_ts")

    # 5) trova il primo evento (min valid_from_ts) che causa la prima differenza per ogni contratto
    first_change = changed_events.groupBy("contratto_cod").agg(spark_min("valid_from_ts").alias("first_change_ts"))

    # 6) se non ci sono cambiamenti in tutto il batch, first_change sarà vuoto 
    # — in tal caso la merge non ha effetto
    # Merge per chiudere existing solo al primo evento che provoca cambiamento
    delta_table.alias("existing").merge(
            first_change.alias("min_staged"),
            "existing.contratto_cod = min_staged.contratto_cod"
        ).whenMatchedUpdate(
            condition = f"(existing.valid_to_ts = to_timestamp('{MAX_TS}') OR existing.valid_to_ts IS NULL) AND min_staged.first_change_ts > existing.valid_from_ts",
            set = {
                "valid_to_ts": "min_staged.first_change_ts",
                "is_current": "false",
                "closed_by_batch": f"'{batch_id}'"
            }
        ).execute()
 


    # FASE B: inserisci tutte le version rows (idempotent insert)
    # Prepara staged con i campi necessari (assicurati di includere tutte le colonne che vuoi mantenere)
    staged = df_transformed.selectExpr(
        "contratto_cod",
        "codice_ordine_sap",
        "tipo_contratto",
        "codice_opec",
        "data_firma",
        "net_amount",
        "causale_annullamento",
        "data_annullamento",
        "codice_agente",
        "status_quote",
        "creazione_dta",
        "ingest_ts",
        "valid_from_ts",
        "valid_to_ts",
        "valid_from_year",
        "valid_from_month",
        "valid_from_day",
        "is_current",
        "batch_id",
        "source_file",
        "closed_by_batch"
    )

    staged_count = staged.count()

    # Merge su key + valid_from per evitare reinserimenti se lo stesso batch viene rilanciato
    merge_condition = "existing.contratto_cod = staged.contratto_cod AND existing.valid_from_ts = staged.valid_from_ts"

    # This insert all the records that don't match the "merge_condition"
    # So if a record has the same contratto_cod and valid_from, it is discarded
    # --> here I might add a statistics
    delta_table.alias("existing").merge(
        staged.alias("staged"),
        merge_condition
    ).whenNotMatchedInsert(
        values = {
            "contratto_cod": "staged.contratto_cod",
            "codice_ordine_sap": "staged.codice_ordine_sap",
            "tipo_contratto": "staged.tipo_contratto",
            "codice_opec": "staged.codice_opec",
            "data_firma": "staged.data_firma",
            "net_amount": "staged.net_amount",
            "causale_annullamento": "staged.causale_annullamento",
            "data_annullamento": "staged.data_annullamento",
            "codice_agente": "staged.codice_agente",
            "status_quote": "staged.status_quote",
            "creazione_dta": "staged.creazione_dta",
            "ingest_ts": "staged.ingest_ts",
            "valid_from_ts": "staged.valid_from_ts",
            "valid_to_ts": "staged.valid_to_ts",
            "valid_from_year": "staged.valid_from_year",
            "valid_from_month": "staged.valid_from_month",
            "valid_from_day": "staged.valid_from_day",
            "is_current": "staged.is_current",
            "batch_id": "staged.batch_id",
            "source_file": "staged.source_file",
            "closed_by_batch": "staged.closed_by_batch"
        }
    ).execute()

    staged_count = staged.count() if 'staged' in locals() else None

    written_delta_table = spark.read.format("delta").load(write_path)

    written_delta_table.show(truncate=False)
    
    # inserted_count: conta righe con batch_id corrente (quelle effettivamente scritte da questa run)
    inserted_count = written_delta_table.filter(col("batch_id") == batch_id).count()
    print("inserted_count: ", inserted_count)

    # closed_count: righe existing marcate closed_by_batch con batch_id corrente
    closed_count = written_delta_table.filter(col("closed_by_batch") == batch_id).count()
    print("closed_count: ", closed_count)

    end_ts = datetime.utcnow().isoformat()
    duration_s_merge = (datetime.fromisoformat(end_ts) - datetime.fromisoformat(start_ts_merge)).total_seconds()
    duration_s = (datetime.fromisoformat(end_ts) - datetime.fromisoformat(start_ts)).total_seconds()
    

    print("MERGE COMPLETATO (intraday versions gestite).")

    app_id = spark.sparkContext.applicationId
    ui_url = spark.sparkContext.uiWebUrl    # es. http://manager-host:4040

    resp = requests.get(f"{ui_url}/api/v1/applications/{app_id}/executors")
    if resp.ok:
        executors_info = resp.json()
        for exe in executors_info:
            print("Executor ID:", exe.get("id"))
            print("  Memory Used (MB):", exe.get("memoryUsed"))
            print("  Total Cores:", exe.get("totalCores"))
            print("  Host Port:", exe.get("hostPort"))
            print("  isDriver:", exe.get("isDriver"))
            print("---------")
    else:
        print("Errore nel recuperare dati executors")

    run_metrics = {
        "executors_info": executors_info,
        "batch_id": batch_id,
        "duration_s": duration_s,
        "duration_s_extract": duration_s_extract,
        "duration_s_validation": duration_s_validation,
        "duration_s_transform": duration_s_transform,
        "duration_s_merge": duration_s_merge,
        "staged_count": int(staged_count) if staged_count is not None else None,
        "inserted_count": int(inserted_count),
        "closed_count": int(closed_count),
        "spark_app_id": spark.sparkContext.applicationId if spark and spark.sparkContext else None,
        "spark_ui_url": getattr(spark.sparkContext, "uiWebUrl", None) if spark and spark.sparkContext else None
    }

    for k,v in dq_metrics.items():
        run_metrics[f"dq_{k}"] = v

    # WRITING METRICS
    write_run_metrics_spark(spark, run_metrics, metrics_path)


if __name__ == '__main__':

    # Controlla se l'argomento è stato fornito
    if len(sys.argv) > 2:
        read_path = sys.argv[1]
        base_write_path = sys.argv[2]
    else:
        print("Usage: python header_etl.py <read_path> <base_write_path>")
        sys.exit(1) # Esce se l'argomento non è fornito
    
    spark = SparkSession.builder \
        .appName("TestDataContract") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()
    
    print("Spark Version: ", spark.version)

    #### PATH DEFINITIONS:
    table_name = "header/"
    
    ### WRITE:
    write_path = base_write_path + "landing/" + table_name
    discarded_path = base_write_path + "discarded/" + table_name
    metrics_path = base_write_path + "metrics/"
  
    run_job_header(spark, read_path, write_path, discarded_path, metrics_path)