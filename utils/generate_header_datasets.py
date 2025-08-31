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
  --conf "spark.sql.shuffle.partitions=112" \
  --total-executor-cores 40 \
  --executor-cores 4 \
  --executor-memory 12G \
  --driver-memory 4G \
  /data/delta-lake-pyspark-scd2/tools/generate_header_datasets.py \
  --size 1000000 \
  --out /data/crm_with_event_time/header/header_20230101_1M.csv \
  --partitions 112 \
  --pct_new 90 \
  --pct_multi_event 5 \
  --seed 42

  
#### SECOND BATCH:
$SPARK_HOME/bin/spark-submit \
  --master spark://10.0.1.7:7077 \
  --deploy-mode client \
  --conf "spark.sql.legacy.timeParserPolicy=CORRECTED" \
  --conf "spark.sql.shuffle.partitions=112" \
  --total-executor-cores 40 \
  --executor-cores 4 \
  --executor-memory 12G \
  --driver-memory 4G \
  /data/delta-lake-pyspark-scd2/tools/generate_header_datasets.py \
  --size 1000000 \
  --out /data/crm_with_event_time/header/header_20230102_1M_delta.csv \
  --partitions 112 \
  --pct_new 20 \
  --pct_update 80 \
  --pct_multi_event 5 \
  --seed 123


"""

spark = SparkSession.builder.appName("GenerateHeaderDatasets").getOrCreate()

def generate_header_dataset(n_rows: int, batch_id: int):
    """
    Crea un dataset 'header' fittizio.
    :param n_rows: numero di righe da generare
    :param batch_id: id batch (1 = prima versione, 2 = incrementale)
    """

    # valori di esempio
    tipi_contratto = ["365", "366", "400"]
    status_quote = ["Accepted", "Rejected", "Pending"]

    base_date = datetime.date(2022, 1, 1)

    data = []
    for i in range(n_rows):
        contratto_cod = f"C{i:08d}"
        ordine_sap = str(3000000000 + i)
        tipo_contratto = random.choice(tipi_contratto)
        codice_opec = f"OPEC{i%1000:04d}"
        data_firma = base_date + datetime.timedelta(days=random.randint(0, 365))
        net_amount = round(random.uniform(1000, 50000), 2)
        causale_annullamento = None
        data_annullamento = None
        codice_agente = str(10000 + (i % 500))
        status = random.choice(status_quote)
        creazione_dta = data_firma + datetime.timedelta(days=random.randint(0, 10))

        # event_time dipende dal batch
        if batch_id == 1:
            event_time = datetime.datetime(2023, 1, 1, random.randint(0, 23), 0, 0)
        else:
            # batch 2: metà nuovi contratti, metà update di contratti esistenti
            if i % 2 == 0:
                # nuovo contratto
                contratto_cod = f"N{i:08d}"
                event_time = datetime.datetime(2023, 2, 1, random.randint(0, 23), 0, 0)
            else:
                # update contratto già visto (SCD2 intraday)
                contratto_cod = f"C{i:08d}"
                event_time = datetime.datetime(2023, 1, 1, 23, 0, 0)

        row = (
            contratto_cod,
            ordine_sap,
            tipo_contratto,
            codice_opec,
            data_firma.strftime("%Y-%m-%d"),
            net_amount,
            causale_annullamento,
            data_annullamento,
            codice_agente,
            status,
            creazione_dta.strftime("%Y-%m-%d"),
            event_time.isoformat()
        )
        data.append(row)

    columns = [
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
        "event_time"
    ]

    return spark.createDataFrame(data, columns)

# === Parametri ===
N = 100000   # dimensione dataset (modificabile)
outdir = "/data/delta/header_input"

# Batch 1
df1 = generate_header_dataset(N, batch_id=1)
df1.write.mode("overwrite").option("header", True).csv(f"{outdir}/header_20230127.csv")

# Batch 2
df2 = generate_header_dataset(N, batch_id=2)
df2.write.mode("overwrite").option("header", True).csv(f"{outdir}/header_20230228.csv")

print(f"✅ Dataset batch1 e batch2 creati in {outdir}")
