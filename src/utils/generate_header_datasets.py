from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, rand
import random
import datetime
from pyspark.sql.functions import expr, concat, lpad, floor, rand, col, lit, to_timestamp, unix_timestamp
from pyspark.sql import DataFrame

"""
export SPARK_HOME=/opt/spark
$SPARK_HOME/bin/spark-submit \
  --master spark://10.0.1.7:7077 \
  --deploy-mode client \
  --conf "spark.sql.legacy.timeParserPolicy=CORRECTED" \
  --conf "spark.local.dir=/tmp/spark_local" \
  /data/delta-lake-pyspark-scd2/utils/generate_header_datasets.py \
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


def make_header_df_from_range(spark, n_rows: int, date_for_event: datetime.date,
                              partitions: int, seed: int,
                              tipi_contratto, status_quote) -> DataFrame:
    """
    Genera dataframe distribuito con n_rows righe usando spark.range.
    event_time in ISO +01:00 con secondi casuali.
    """
    # set seed for reproducibility of rand()
    df = spark.range(0, n_rows).repartition(partitions)

    # contratto_cod like C00000001 (8 digits)
    df = df.withColumn("contratto_cod", concat(lit("C"), lpad(col("id").cast("string"), 8, "0")))

    # ordine_sap: 3000000000 + id
    df = df.withColumn("codice_ordine_sap", (lit(3000000000) + col("id")).cast("string"))

    # tipo_contratto: pick by hashing id
    df = df.withColumn("tipo_contratto", expr(f"array('{','.join(tipi_contratto)}')[cast(id % {len(tipi_contratto)} as int)]"))

    # codice_opec
    df = df.withColumn("codice_opec", expr("concat('OPEC', lpad(cast(id % 1000 as string), 4, '0'))"))

    # data_firma and creazione_dta: use date_for_event minus random days
    base_date = date_for_event.strftime("%Y-%m-%d")
    # random day offsets
    df = df.withColumn("rand1", floor(rand(seed + 1) * 366).cast("int"))
    df = df.withColumn("rand2", floor(rand(seed + 2) * 31).cast("int"))
    df = df.withColumn("data_firma", expr(f"date_add('{base_date}', -rand1)"))
    df = df.withColumn("creazione_dta", expr("date_add(data_firma, -cast(rand2 as int))"))

    # net_amount
    df = df.withColumn("net_amount", (floor(rand(seed+3) * (50000-1000) * 100) / 100).cast("string"))

    df = df.withColumn("causale_annullamento", lit(""))
    df = df.withColumn("data_annullamento", lit(""))

    df = df.withColumn("codice_agente", expr("cast(10000 + cast(id % 500 as int) as string)"))

    # status_quote pick by modular index
    df = df.withColumn("status_quote", expr(f"array('{','.join(status_quote)}')[cast(id % {len(status_quote)} as int)]"))

    # event_time: choose hour/min/sec randomly, build ISO with +01:00
    df = df.withColumn("hour", floor(rand(seed+4) * 24).cast("int"))
    df = df.withColumn("minute", floor(rand(seed+5) * 60).cast("int"))
    df = df.withColumn("second", floor(rand(seed+6) * 60).cast("int"))

    df = df.withColumn("HH", lpad(col("hour").cast("string"), 2, "0"))
    df = df.withColumn("MM", lpad(col("minute").cast("string"), 2, "0"))
    df = df.withColumn("SS", lpad(col("second").cast("string"), 2, "0"))

    # Build ISO string like 2023-01-27T14:23:05.000+01:00
    df = df.withColumn("event_time",
        concat(lit(date_for_event.strftime("%Y-%m-%dT")), col("HH"), lit(":"), col("MM"), lit(":"), col("SS"), lit(".000+01:00"))
    )

    # select final columns
    out_cols = [
        "contratto_cod", "codice_ordine_sap", "tipo_contratto", "codice_opec",
        "data_firma", "net_amount", "causale_annullamento", "data_annullamento",
        "codice_agente", "status_quote", "creazione_dta", "event_time"
    ]
    df_out = df.select(*out_cols)
    return df_out

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

def generate_batch1_distributed(spark, n_rows: int, batch_date: datetime.date, outpath: str, partitions: int, seed: int,
                    tipi_contratto, status_quote):
    df = make_header_df_from_range(spark, n_rows, batch_date, partitions, seed, tipi_contratto, status_quote)
    # write: keep same pattern, use overwrite
    df.write.mode("overwrite").option("header", True).option("sep", "|").csv(outpath)
    print(f"Written batch1 to {outpath} (rows={n_rows})")


def generate_batch2_distributed(spark, n_rows: int, batch_date: datetime.date, outpath: str, partitions: int, seed: int,
                    existing_count: int, pct_new: float, pct_multi_event: float, tipi_contratto, status_quote):
    # produce a DataFrame that mixes new (N...) and existing (C...) rows.
    # we'll create two dfs: new_df and updates_df and union them
    pct_new_frac = float(pct_new) / 100.0
    new_count = int(round(n_rows * pct_new_frac))
    update_count = n_rows - new_count

    # new rows
    new_df = make_header_df_from_range(spark, new_count, batch_date, partitions, seed+10, tipi_contratto, status_quote) \
                .withColumn("contratto_cod", concat(lit("N"), lpad((col("contratto_cod").substr(2,8)).cast("string"), 8, "0")))

    # updates: pick random ids from 0..existing_count-1
    updates_df = spark.range(0, update_count).repartition(partitions)
    updates_df = updates_df.withColumn("idx", floor(rand(seed+20) * existing_count).cast("int"))
    updates_df = updates_df.withColumn("contratto_cod", concat(lit("C"), lpad(col("idx").cast("string"), 8, "0")))
    # reuse make_row logic by joining basic attributes from generated df of same size:
    helper = make_header_df_from_range(spark, update_count, batch_date, partitions, seed+21, tipi_contratto, status_quote) \
                .select("codice_ordine_sap", "tipo_contratto", "codice_opec", "data_firma",
                        "net_amount", "causale_annullamento", "data_annullamento",
                        "codice_agente", "status_quote", "creazione_dta", "event_time")
    updates_df = updates_df.drop("id")
    updates_df = updates_df.withColumn("rn", expr("monotonically_increasing_id()")) \
                           .drop("rn") \
                           .withColumn("tmp_idx", col("idx"))
    # attach helper columns by zipping via row_number (simple approach)
    helper = helper.withColumn("__rid", expr("row_number() over (order by rand())"))
    updates_df = updates_df.withColumn("__rid", expr("row_number() over (order by rand())"))
    joined = updates_df.join(helper, on="__rid", how="left").drop("__rid")
    # final select, keeping contratto_cod from updates and helper columns
    out_cols = [
        "contratto_cod", "codice_ordine_sap", "tipo_contratto", "codice_opec",
        "data_firma", "net_amount", "causale_annullamento", "data_annullamento",
        "codice_agente", "status_quote", "creazione_dta", "event_time"
    ]
    updates_final = joined.select(*out_cols)

    # union new + updates; ensure count==n_rows by possible adjustment
    df_out = new_df.unionByName(updates_final).limit(n_rows)
    df_out.write.mode("overwrite").option("header", True).option("sep", "|").csv(outpath)
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
    generate_batch1_distributed(spark, args.size, batch1_date, out1, args.partitions, args.seed, tipi_contratto, status_quote)

    print("Generating batch2 ...")
    generate_batch2_distributed(spark, args.size, batch2_date, out2, args.partitions, args.seed + 1, args.size, args.pct_new, 0.0, tipi_contratto, status_quote)

    print("Done.")

if __name__ == "__main__":
    main()
