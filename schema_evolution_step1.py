#!/usr/bin/env python3
# schema_evolution_step1.py
import argparse
import os
import time
import json
import glob
import shutil
import uuid
import subprocess
from datetime import datetime
from typing import Tuple

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from delta.tables import DeltaTable


def now_ts():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def dir_size_and_count(path: str) -> Tuple[int, int]:
    """Return (total_bytes, file_count) walking path. If path not exists return (0,0)."""
    total = 0
    count = 0
    if not os.path.exists(path):
        return 0, 0
    for root, dirs, files in os.walk(path):
        for f in files:
            try:
                fp = os.path.join(root, f)
                total += os.path.getsize(fp)
                count += 1
            except Exception:
                # ignore broken links, permission errors
                pass
    return total, count


def atomic_single_csv_write(spark, record: dict, out_dir: str, filename: str) -> str:
    """
    Write a single-line CSV atomically: write to tmp dir with coalesce(1) then move part file to destination filename.
    Returns final_path.
    """
    os.makedirs(out_dir, exist_ok=True)
    batch_id = record.get("run_id") or uuid.uuid4().hex
    tmp_dir = os.path.join(out_dir, f"tmp_{batch_id}_{uuid.uuid4().hex}")
    final_path = os.path.join(out_dir, filename)

    # create DF and write
    df = spark.createDataFrame([record])
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(tmp_dir)

    # find part file
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError("No part file produced when writing metrics CSV")

    part_file = part_files[0]
    # move atomically (same mount assumed)
    try:
        os.replace(part_file, final_path)
    except Exception as e:
        # fallback to copy+remove
        shutil.copy2(part_file, final_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return final_path


def query_spark_ui(ui_url: str, app_id: str) -> dict:
    """Try to query executors and jobs from Spark UI REST API, return dict with results or error info."""
    info = {}
    try:
        execs = requests.get(f"{ui_url}/api/v1/applications/{app_id}/executors", timeout=10)
        if execs.ok:
            info["executors"] = execs.json()
        jobs = requests.get(f"{ui_url}/api/v1/applications/{app_id}/jobs", timeout=10)
        if jobs.ok:
            info["jobs"] = jobs.json()
        stages = requests.get(f"{ui_url}/api/v1/applications/{app_id}/stages", timeout=10)
        if stages.ok:
            info["stages"] = stages.json()
    except Exception as e:
        info["error"] = str(e)
    return info


def main():
    parser = argparse.ArgumentParser(description="Schema evolution step 1 automation: mergeSchema append + metrics")
    parser.add_argument("--delta_path", required=True, help="Delta table path (e.g. /data/delta/landing/header)")
    parser.add_argument("--sample_csv", required=True, help="CSV to read and append as sample (will be augmented with new column)")
    parser.add_argument("--metrics_base", default="/data/delta/metrics/schema_evolution", help="where to place metrics csvs")
    parser.add_argument("--csv_sep", default="|", help="CSV separator for sample_csv (default '|')")
    parser.add_argument("--new_col", default="risk_score", help="name of the new nullable column to add")
    args = parser.parse_args()

    ts = now_ts()
    run_id = f"schema_step1_{ts}"
    os.makedirs(args.metrics_base, exist_ok=True)

    # build Spark (ensure Delta extensions if user didn't set via spark-submit)
    spark = SparkSession.builder \
        .appName("SchemaEvolution_Step1") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    app_id = spark.sparkContext.applicationId
    ui_url = getattr(spark.sparkContext, "uiWebUrl", None)

    result = {
        "run_id": run_id,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "delta_path": args.delta_path,
        "sample_csv": args.sample_csv,
        "new_column": args.new_col,
        "app_id": app_id,
        "spark_ui_url": ui_url
    }

    try:
        # read delta table current latest version
        dt = DeltaTable.forPath(spark, args.delta_path)
        hist = dt.history(1).collect()
        if hist:
            version_before = int(hist[0]["version"])
        else:
            # if history not available, try reading _delta_log
            version_before = 0
        result["version_before"] = version_before

        # measure write: read sample CSV, add column, append with mergeSchema
        sample_df = spark.read.option("header", True).option("sep", args.csv_sep).csv(args.sample_csv)
        sample_df = sample_df.withColumn(args.new_col, lit(None).cast(StringType()))

        t0 = time.time()
        # append with mergeSchema
        sample_df.write.format("delta").mode("append").option("mergeSchema", "true").save(args.delta_path)
        t1 = time.time()
        write_dur = t1 - t0
        result["write_duration_s"] = write_dur

        # get new version (should be version_before + 1)
        hist_after = dt.history(1).collect()
        if hist_after:
            # history returned entry for last commit (after append)
            version_after = int(hist_after[0]["version"])
        else:
            version_after = version_before + 1
        result["version_after"] = version_after

        # directory size / file count
        size_bytes, file_count = dir_size_and_count(args.delta_path)
        result["dir_size_bytes"] = int(size_bytes)
        result["dir_file_count"] = int(file_count)

        # read/measure time previous vs current (use time-travel)
        # previous:
        t0 = time.time()
        df_prev = spark.read.format("delta").option("versionAsOf", version_before).load(args.delta_path)
        prev_count = df_prev.count()
        t1 = time.time()
        result["read_prev_count"] = int(prev_count)
        result["read_prev_duration_s"] = t1 - t0

        # after:
        t0 = time.time()
        df_after = spark.read.format("delta").load(args.delta_path)
        after_count = df_after.count()
        t1 = time.time()
        result["read_after_count"] = int(after_count)
        result["read_after_duration_s"] = t1 - t0

        # quick schema prints in logs
        print("Schema before (version {}):".format(version_before))
        spark.read.format("delta").option("versionAsOf", version_before).load(args.delta_path).printSchema()
        print("Schema after:")
        spark.read.format("delta").load(args.delta_path).printSchema()

        # try to gather Spark UI data (executors, jobs, stages)
        if ui_url:
            try:
                spark_ui_info = query_spark_ui(ui_url, app_id)
                result["spark_ui_info"] = spark_ui_info
            except Exception as e:
                result["spark_ui_info_error"] = str(e)
        else:
            result["spark_ui_info"] = None

        # add some aggregate job metrics if available
        # e.g. count tasks from jobs list
        try:
            jobs = result.get("spark_ui_info", {}).get("jobs", [])
            total_tasks = sum([j.get("numTasks", 0) for j in jobs]) if jobs else None
            result["spark_total_tasks"] = total_tasks
        except Exception:
            result["spark_total_tasks"] = None

        # success flag
        result["error_flag"] = 0
        result["error_message"] = None

    except Exception as e:
        result["error_flag"] = 1
        result["error_message"] = str(e)
        print("ERROR during schema evolution step:", e)
    finally:
        # write metrics single CSV atomically
        out_filename = f"step_1_{ts}.csv"
        try:
            path_written = atomic_single_csv_write(spark, result, args.metrics_base, out_filename)
            print("Metrics written to:", path_written)
        except Exception as ew:
            print("Failed to write metrics CSV:", ew)
        # stop spark
        spark.stop()


if __name__ == "__main__":
    main()
