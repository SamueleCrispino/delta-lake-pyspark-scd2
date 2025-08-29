#!/usr/bin/env python3
"""
add_event_time.py

Legge tutti i file header_YYYYMMDD.txt in `crm/header/`, aggiunge la colonna event_time
(formato yyyy-MM-dd HH:00:00 con ora casuale 0-23 per riga) e salva i CSV risultanti in
crm_with_event_time/header/header_YYYYMMDD.txt (pipe-separated, header).
"""

import re
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    trim, when, expr, col, floor, rand, concat, lit, lpad, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType

# --- CONFIGURAZIONE ---
INPUT_DIR = "crm/header"                       # path relativo dove sono i file originali
OUTPUT_DIR = "crm_with_event_time/header"      # path di output
FILE_REGEX = r"header_(\d{8})\.txt"            # estrae YYYYMMDD

# --- SCHEMA: mantieni campi data come string per evitare parsing automatico ---
header_schema_str = StructType([
    StructField("contratto_cod", StringType(), nullable=False),
    StructField("codice_ordine_sap", StringType(), nullable=True),
    StructField("tipo_contratto", StringType(), nullable=True),
    StructField("codice_opec", StringType(), nullable=True),
    StructField("data_firma", StringType(), nullable=True),
    StructField("net_amount", StringType(), nullable=True),
    StructField("causale_annullamento", StringType(), nullable=True),
    StructField("data_annullamento", StringType(), nullable=True),
    StructField("codice_agente", StringType(), nullable=True),
    StructField("status_quote", StringType(), nullable=True),
    StructField("creazione_dta", StringType(), nullable=True)
])

# --- creare Spark session ---
spark = SparkSession.builder \
    .appName("header-dataset-creation") \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Assicuriamoci che la directory di output esista
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Trova i file da processare
input_path = Path(INPUT_DIR)
files = sorted([p for p in input_path.glob("header_*.txt") if p.is_file()])

if not files:
    print(f"Nessun file trovato in {INPUT_DIR} con pattern header_YYYYMMDD.txt")
else:
    print(f"Trovati {len(files)} file. Processando...")

for file_path in files:
    fname = file_path.name
    m = re.match(FILE_REGEX, fname)
    if not m:
        print(f"- Salto file non conforme al pattern: {fname}")
        continue

    file_date_yyyymmdd = m.group(1)   # es. '20230123'
    # format YYYY-MM-DD (stringa)
    file_date = f"{file_date_yyyymmdd[0:4]}-{file_date_yyyymmdd[4:6]}-{file_date_yyyymmdd[6:8]}"

    # read file (pipe-separated) con schema stringhe
    full_input = str(file_path)
    print(f"- Leggo {full_input} (batch date {file_date})")
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "|")
        .schema(header_schema_str)
        .csv(full_input)
    )

    # pulizia base: trim e converti stringhe vuote a null per creazione_dta
    df = df.withColumn("creazione_dta_raw", trim(col("creazione_dta")))
    df = df.withColumn(
        "creazione_dta_raw",
        when(col("creazione_dta_raw") == "", None).otherwise(col("creazione_dta_raw"))
    )

    # Genera event_time:
    # - hour random per riga: floor(rand(seed) * 24)
    # - seed derivato dalla data del file per riproducibilita (es. int(YYYYMMDD))
    seed = int(file_date_yyyymmdd)
    df = df.withColumn("hour_rand", floor(rand(seed) * 24).cast("int"))

    # crea stringa 'YYYY-MM-DD HH:00:00' e parsala in timestamp
    df = df.withColumn(
        "event_time_str",
        concat(lit(file_date), lit(" "), lpad(col("hour_rand").cast("string"), 2, "0"), lit(":00:00"))
    ).withColumn(
        "event_time",
        to_timestamp(col("event_time_str"), "yyyy-MM-dd HH:mm:ss")
    )

    # rimuovi colonne ausiliarie prima di salvare (se non vuoi tenerle)
    df_out = df.drop("creazione_dta_raw", "hour_rand", "event_time_str")

    # salva in output in CSV pipe-separated (manteniamo header)
    out_path = os.path.join(OUTPUT_DIR, fname)
    # Spark CSV salva in cartelle; per avere un singolo file CSV possiamo scrivere in directory e poi
    # l'utente può concatenare o leggere la directory. Qui salviamo in directory per batch:
    out_dir_for_file = os.path.join(OUTPUT_DIR, fname.replace(".txt", ""))  # es. crm_with_event_time/header/header_20230123
    print(f"  -> Scrivo output in {out_dir_for_file} (CSV pipe-separated)")
    df_out.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(out_dir_for_file)

print("Fatto. I file con event_time sono in:", OUTPUT_DIR)
spark.stop()
