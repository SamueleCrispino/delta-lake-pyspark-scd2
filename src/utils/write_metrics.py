import os
import glob
import shutil
import uuid
from pyspark.sql import SparkSession

def write_run_metrics_spark(spark, run_metrics: dict, base_dir="/data/delta/metrics"):
    import tempfile
    import glob, shutil, os, uuid

    batch_id = run_metrics.get("batch_id") or str(uuid.uuid4())
    tmp_dir = os.path.join(base_dir, f"tmp_{batch_id}_{uuid.uuid4().hex}")
    final_path = os.path.join(base_dir, f"{batch_id}.csv")

    os.makedirs(base_dir, exist_ok=True)

    df = spark.createDataFrame([run_metrics])
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(tmp_dir)

    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No part file written in {tmp_dir}")

    shutil.move(part_files[0], final_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"Metrics written at: {final_path}")
    return final_path
