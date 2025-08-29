# test_scd2.py
import os
import shutil
import tempfile
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# importa la funzione run_job_header dal tuo file
import header_etl as he

# config Spark (simile a quella che usi nel file principale)
def get_spark():
    return SparkSession.builder \
        .appName("TestSCD2") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[2]") \
        .getOrCreate()

# helper per scrivere file CSV con separatore |
def write_csv(path, header, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("|".join(header) + "\n")
        for r in rows:
            f.write("|".join(r) + "\n")
