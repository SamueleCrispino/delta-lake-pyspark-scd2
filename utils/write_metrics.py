import os
import glob
import shutil
import uuid
from pyspark.sql import SparkSession

def write_run_metrics_spark(spark: SparkSession, run_metrics: dict,
                            base_dir="/data/delta/metrics"):
    """
    Produce un singolo CSV per run:
      /data/delta/metrics/{batch_id}.csv
    Procedura:
      - scrive in una tmp dir con spark.coalesce(1)
      - individua il part-*.csv
      - sposta/rename atomico in destination
      - pulisce tmp dir
    """
    batch_id = run_metrics.get("batch_id") or str(uuid.uuid4())
    tmp_dir = os.path.join(base_dir, f"tmp_{batch_id}_{uuid.uuid4().hex}")
    final_path = os.path.join(base_dir, f"{batch_id}.csv")

    # crea DataFrame spark di una riga
    df = spark.createDataFrame([run_metrics])

    # scrive una singola part-file nella tmp_dir
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(tmp_dir)

    # trova il file part-*.csv nella tmp_dir
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        # cleanup se qualcosa è andato storto
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError("No part file found after write to tmp_dir")

    part_file = part_files[0]

    # sposta (rename) in modo atomico nella destinazione finale
    # os.replace è atomico se sorgente e destinazione sono sulla stessa mount
    os.replace(part_file, final_path)

    # pulizia tmp dir (rimuove file _SUCCESS e metadati)
    shutil.rmtree(tmp_dir, ignore_errors=True)

    return final_path
