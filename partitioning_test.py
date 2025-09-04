import argparse
import time
import os
from datetime import datetime
from pyspark.sql import SparkSession

def main(args):
    spark = SparkSession.builder.appName("PartitioningTest").getOrCreate()

    # timestamp per i file di output
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # output dir
    metrics_dir = "/data/delta/metrics/partitioning_metrics"
    os.makedirs(metrics_dir, exist_ok=True)
    metrics_file = os.path.join(metrics_dir, f"partitioning_{ts}.csv")

    # tabella partizionata
    part_path = args.part_path

    def run_query(table_path, tag):
        df = spark.read.format("delta").load(table_path)
        start = time.time()
        res = df.filter("valid_from_year = 2023 AND valid_from_month = 1 AND valid_from_day = 27 AND is_current = true").count()
        dur = time.time() - start
        return {"table": tag, "result": res, "duration_sec": dur}


    # run su entrambe le versioni
    record = run_query(part_path, "partitioned")
    # salva metriche in CSV
    safe_record = {k: str(v) if v is not None else "" for k, v in record.items()}
    df = spark.createDataFrame([safe_record])
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(metrics_file)
    print(f"Metrics saved to {metrics_file}")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part_path", required=True, help="Path Delta partizionata")
    args = parser.parse_args()
    main(args)
