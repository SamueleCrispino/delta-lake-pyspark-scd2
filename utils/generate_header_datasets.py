from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, rand
import random
import datetime

"""
export SPARK_HOME=/opt/spark
$SPARK_HOME/bin/spark-submit \
  --master spark://10.0.1.7:7077 \
  --deploy-mode client \
  --conf "spark.sql.legacy.timeParserPolicy=CORRECTED" \
  /data/delta-lake-pyspark-scd2/tools/generate_header_datasets.py \
  --size 1000000 \
  --outdir /data/crm_with_event_time/header \
  --partitions 32 \
  --pct_new 50 \
  --seed 42 \
  --batch1_date 20230127 \
  --batch2_date 20230228



"""
#!/usr/bin/env python3
# /data/delta-lake-pyspark-scd2/tools/generate_header_datasets.py
import argparse
import random
import datetime
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import Row
import pytz

def iso_with_tz(dt: datetime.datetime, tz_offset_hours: int = 1) -> str:
    tz = datetime.timezone(datetime.timedelta(hours=tz_offset_hours))
    dt_tz = dt.replace(tzinfo=tz)
    # millisecond precision with offset +01:00
    return dt_tz.isoformat(timespec='milliseconds')

def make_row(contratto_cod, i, date_for_event: datetime.date, hour: int, tipi_contratto, status_quote):
    ordine_sap = str(3000000000 + i)
    tipo_contratto = random.choice(tipi_contratto)
    codice_opec = f"OPEC{i%1000:04d}"
    data_firma = (date_for_event - datetime.timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    net_amount = f"{round(random.uniform(1000, 50000), 2)}"
    causale_annullamento = ""
    data_annullamento = ""
    codice_agente = str(10000 + (i % 500))
    status = random.choice(status_quote)
    creazione_dta = (date_for_event - datetime.timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
    # event_time with chosen hour and random minute/second; timezone +01:00
    event_dt = datetime.datetime(date_for_event.year, date_for_event.month, date_for_event.day,
                                 hour, random.randint(0, 59), random.randint(0, 59))
    event_time = iso_with_tz(event_dt, tz_offset_hours=1)
    return (
        contratto_cod,
        ordine_sap,
        tipo_contratto,
        codice_opec,
        data_firma,
        net_amount,
        causale_annullamento,
        data_annullamento,
        codice_agente,
        status,
        creazione_dta,
        event_time
    )

def generate_batch1(spark, n_rows: int, batch_date: datetime.date, outpath: str, partitions: int, seed: int,
                    tipi_contratto, status_quote):
    random.seed(seed)
    data = []
    for i in range(n_rows):
        contratto_cod = f"C{i:08d}"
        hour = random.randint(0, 23)
        data.append(make_row(contratto_cod, i, batch_date, hour, tipi_contratto, status_quote))

    cols = [
        "contratto_cod", "codice_ordine_sap", "tipo_contratto", "codice_opec",
        "data_firma", "net_amount", "causale_annullamento", "data_annullamento",
        "codice_agente", "status_quote", "creazione_dta", "event_time"
    ]
    df = spark.createDataFrame([Row(**dict(zip(cols, r))) for r in data])
    # repartition and write to directory named outpath (e.g. .../header_20230127.csv)
    df.repartition(partitions).write.mode("overwrite").option("header", True).csv(outpath)
    print(f"Written batch1 to {outpath} (rows={n_rows})")

def generate_batch2(spark, n_rows: int, batch_date: datetime.date, outpath: str, partitions: int, seed: int,
                    existing_count: int, pct_new: float, pct_multi_event: float, tipi_contratto, status_quote):
    """
    batch2: generates a mix of new and updates.
    - existing_count: number of distinct contratto in batch1 (usually = size of batch1)
    - pct_new: fraction (0..100) of rows that are new contratto (Nxxxxx)
    - pct_multi_event: fraction of rows that create multiple events for same contratto within same batch (simulated by
      emitting multiple rows for same contratto because selection of existing contratto is random)
    """
    random.seed(seed)
    data = []
    for i in range(n_rows):
        r = random.random() * 100
        if r < pct_new:
            contratto_cod = f"N{i:08d}"
            hour = random.randint(0, 23)
            data.append(make_row(contratto_cod, i, batch_date, hour, tipi_contratto, status_quote))
        else:
            # update existing contratto from batch1: pick random index
            idx = random.randint(0, existing_count - 1)
            contratto_cod = f"C{idx:08d}"
            # choose event_time maybe later in day to simulate update
            hour = random.randint(0, 23)
            data.append(make_row(contratto_cod, idx, batch_date, hour, tipi_contratto, status_quote))
            # pct_multi_event already implicitly possible because multiple selections may hit same contratto
    cols = [
        "contratto_cod", "codice_ordine_sap", "tipo_contratto", "codice_opec",
        "data_firma", "net_amount", "causale_annullamento", "data_annullamento",
        "codice_agente", "status_quote", "creazione_dta", "event_time"
    ]
    df = spark.createDataFrame([Row(**dict(zip(cols, r))) for r in data])
    df.repartition(partitions).write.mode("overwrite").option("header", True).csv(outpath)
    print(f"Written batch2 to {outpath} (rows={n_rows}, pct_new={pct_new})")

def parse_yyyymmdd(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y%m%d").date()

def main():
    parser = argparse.ArgumentParser(description="Generate header test datasets")
    parser.add_argument("--size", type=int, default=100000, help="rows per batch")
    parser.add_argument("--outdir", type=str, required=True, help="base output dir (will create header_YYYYMMDD.csv dirs)")
    parser.add_argument("--batch1_date", type=str, default="20230127", help="YYYYMMDD for batch1 filename")
    parser.add_argument("--batch2_date", type=str, default="20230228", help="YYYYMMDD for batch2 filename")
    parser.add_argument("--partitions", type=int, default=16, help="number of output partitions")
    parser.add_argument("--pct_new", type=float, default=50.0, help="percent new rows in batch2")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("GenerateHeaderDatasets").getOrCreate()

    tipi_contratto = ["365", "366", "400"]
    status_quote = ["Accepted", "Rejected", "Pending"]

    batch1_date = parse_yyyymmdd(args.batch1_date)
    batch2_date = parse_yyyymmdd(args.batch2_date)

    out1 = args.outdir.rstrip("/") + f"/header_{args.batch1_date}.csv"
    out2 = args.outdir.rstrip("/") + f"/header_{args.batch2_date}.csv"

    print("Generating batch1 ...")
    generate_batch1(spark, args.size, batch1_date, out1, args.partitions, args.seed, tipi_contratto, status_quote)

    print("Generating batch2 ...")
    generate_batch2(spark, args.size, batch2_date, out2, args.partitions, args.seed + 1, args.size, args.pct_new, 0.0, tipi_contratto, status_quote)

    print("Done.")

if __name__ == "__main__":
    main()
