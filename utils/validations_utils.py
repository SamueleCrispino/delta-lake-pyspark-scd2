# utils/validations_utils.py

import re
from functools import reduce
from operator import or_

from pyspark.sql.functions import (
    col, to_date, to_timestamp, trim, when, input_file_name, lit, coalesce,
    row_number, sum as spark_sum
)
from pyspark.sql import Window


def validation(extracted_df, deduplication_keys, spark,
               date_regex, discarded_path, batch_id):
    """
    Validation pipeline (aggiornata):
      - mantiene la riga più recente all'interno di gruppi duplicati definiti da deduplication_keys
        (criterio di ordinamento: event_time_ts desc)
      - scarta righe se ANY key is NULL (NULL_KEY) oppure se event_date_parsed != batch_date (BATCH_DATE_MISMATCH)
      - marca e salva come scarto i record 'DUPLICATE_OLDER' (rappresentano occorrenze duplicate non più recenti)
      - salva scarti in discarded_path/discarded_<batch_date> (Delta) e metriche in discarded_path/metrics/<batch_date>/
      - restituisce kept_clean (righe pronte per trasformazioni successive)
    Assunzioni: extracted_df contiene la colonna input_file_name (es. .withColumn("input_file_name", input_file_name()))
                 il job processa un singolo file per run (se leggi più file, serve estendere la logica per file)
    """

    # sanity checks
    if not deduplication_keys or not isinstance(deduplication_keys, (list, tuple)):
        raise ValueError("deduplication_keys deve essere una lista di nomi di colonna")

    # ---------------------------------------------------
    # 1) parse/normalizzazione di timestamp/date utili
    #    event_date_parsed (DATE) e event_time_ts (TIMESTAMP)
    # ---------------------------------------------------
    df = extracted_df.withColumn(
        "event_time_ts",
        coalesce(
            to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("event_time"), "yyyy-MM-dd")
        )
    ).withColumn(
        "event_date_parsed",
        coalesce(
            to_date(col("event_time_ts")),
            to_date(col("event_time"), "yyyy-MM-dd"),
            to_date(col("event_time"))
        )
    )

    # ---------------------------------------------------
    # 2) estrai batch date dal nome file (assume single input file per run)
    # ---------------------------------------------------
    input_file_row = df.select("source_file").limit(1).collect()
    if not input_file_row:
        raise RuntimeError("Impossibile leggere input_file_name() dal DataFrame di input")
    input_file = input_file_row[0]["source_file"]
    m = re.search(date_regex, input_file)
    if not m:
        raise RuntimeError(f"Impossibile estrarre la data dal nome file: {input_file} con regex {date_regex}")
    batch_date_str = m.group(1)  # 'YYYYMMDD'
    batch_date_iso = f"{batch_date_str[0:4]}-{batch_date_str[4:6]}-{batch_date_str[6:8]}"
    batch_date_literal = to_date(lit(batch_date_iso), "yyyy-MM-dd")

    # ---------------------------------------------------
    # 3) costruisci condizioni: null su chiavi; mismatch tra event_date_parsed e batch_date
    # ---------------------------------------------------
    null_condition = reduce(or_, [col(k).isNull() for k in deduplication_keys])
    mismatch_condition = (col("event_date_parsed").isNull()) | (col("event_date_parsed") != batch_date_literal)

    # ---------------------------------------------------
    # 4) dedup: mantieni la riga più recente per ciascun gruppo deduplication_keys
    #    ordering: event_time_ts desc
    # ---------------------------------------------------
    w_dedup = Window.partitionBy(*deduplication_keys).orderBy(
        col("event_time_ts").desc_nulls_last()
    )
    df = df.withColumn("rn", row_number().over(w_dedup))

    # ---------------------------------------------------
    # 5) marca scarti e kept:
    #    - NULL_KEY: any key is null -> discard
    #    - BATCH_DATE_MISMATCH: event_date_parsed not equal batch_date -> discard
    #    - DUPLICATE_OLDER: rn > 1 -> older duplicate -> discard (keep rn==1)
    #    - kept: rn == 1 and not null and not mismatch
    # ---------------------------------------------------
    df = df.withColumn(
        "discard_reason",
        when(null_condition, lit("NULL_KEY"))
        .when(mismatch_condition, lit("BATCH_DATE_MISMATCH"))
        .when(col("rn") > 1, lit("DUPLICATE_OLDER"))
        .otherwise(lit(None))
    )

    kept = df.filter((col("rn") == 1) & (~null_condition) & (~mismatch_condition))
    discarded = df.filter(col("discard_reason").isNotNull())

    # ---------------------------------------------------
    # 6) salva scarti (Delta) - uso batch_date_str per path
    #    overwrite per batch (ogni batch scrive la propria cartella)
    # ---------------------------------------------------
    out_path = f"{discarded_path}/discarded_{batch_date_str}"
    discarded.write.mode("overwrite").format("delta").save(out_path)

    # ---------------------------------------------------
    # 7) metriche: calcola tutte le metriche in UNA sola aggregazione (evita molteplici count())
    # ---------------------------------------------------
    metrics_agg = df.select(
        when((col("rn") == 1) & (~null_condition) & (~mismatch_condition), 1).otherwise(0).alias("is_kept"),
        when(col("discard_reason").isNotNull(), 1).otherwise(0).alias("is_discarded"),
        when(col("discard_reason") == "DUPLICATE_OLDER", 1).otherwise(0).alias("is_dup_older"),
        when(col("discard_reason") == "NULL_KEY", 1).otherwise(0).alias("is_null_key"),
        when(col("discard_reason") == "BATCH_DATE_MISMATCH", 1).otherwise(0).alias("is_batch_mismatch")
    ).agg(
        spark_sum("is_kept").alias("kept"),
        spark_sum("is_discarded").alias("discarded"),
        spark_sum("is_dup_older").alias("duplicates_older"),
        spark_sum("is_null_key").alias("null_key"),
        spark_sum("is_batch_mismatch").alias("batch_date_mismatch")
    ).collect()[0].asDict()

    total = extracted_df.count()  # un unico count (opzionale: si può anche calcolarlo da somme)

    dq_metrics = {
        "batch_date": batch_date_str,
        "total": int(total),
        "kept": int(metrics_agg["kept"]),
        "discarded": int(metrics_agg["discarded"]),
        "duplicates_older": int(metrics_agg["duplicates_older"]),
        "null_key": int(metrics_agg["null_key"]),
        "batch_date_mismatch": int(metrics_agg["batch_date_mismatch"])
    }
    
    #metrics_values = [(total, metrics_agg["kept"], metrics_agg["discarded"],
    #                   metrics_agg["duplicates_older"], metrics_agg["null_key"], metrics_agg["batch_date_mismatch"])]
    #metrics_schema = ["total", "kept", "discarded", "duplicates_older", "null_key", "batch_date_mismatch"]
#
    #metrics_df = spark.createDataFrame(metrics_values, metrics_schema)
    #metrics_df.show(truncate=False)
    #
    #metrics_df.write.mode("append").json(f"metrics/{batch_id}/")

    # ---------------------------------------------------
    # 8) pulizia colonne interne e ritorno kept_clean
    # ---------------------------------------------------
    cols_to_drop = ["rn", "discard_reason", "event_date_parsed", "event_time_ts"]
    # mantieni input_file_name se ti serve in seguito; altrimenti puoi dropparla qui
    kept_clean = kept.drop(*[c for c in cols_to_drop if c in kept.columns])

    return kept_clean, dq_metrics
