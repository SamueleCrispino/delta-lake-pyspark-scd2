# test_scd2.py
import os
import shutil
import tempfile
import time
from pathlib import Path
import sys
# Aggiunge la directory genitore del progetto al Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# importa la funzione run_job_header dal tuo file
import src.header_etl as he

from src.utils.spark_utils import *


def run_all_tests():
    spark = get_spark()
    tmp_root = tempfile.mkdtemp(prefix="scd2_test_")
    try:
        crm_dir = os.path.join(tmp_root, "crm_with_event_time", "header")
        delta_path = os.path.join(tmp_root, "landing", "header")
        discarded_path = os.path.join(tmp_root, "discarded", "header")
        metrics_path = os.path.join(tmp_root, "metrics", "header")
        os.makedirs(crm_dir, exist_ok=True)

        # make he module use our temp paths (if your code references constants directly,
        # you can override them here by monkeypatching; otherwise pass paths to run_job_header)
        # we will call run_job_header(spark, read_path, write_path) directly.

        header = ["contratto_cod","codice_ordine_sap","tipo_contratto","codice_opec",
                  "data_firma","net_amount","causale_annullamento","data_annullamento",
                  "codice_agente","status_quote","creazione_dta","event_time"]

        ######################
        # TEST 1: initial load creates open version
        ######################
        f1 = os.path.join(crm_dir, "header_20230101.csv")
        rows1 = [
            ["C1","ORD1","365","P1","2022-01-01","100.00","","","AG1","Accepted","11/25/2022","2023-01-01 10:00:00"]
        ]
        write_csv(f1, header, rows1)

        he.run_job_header(spark, f1, delta_path, discarded_path, metrics_path)

        df = spark.read.format("delta").load(delta_path)
        assert df.filter(col("contratto_cod") == "C1").count() == 1, "TEST1: versione iniziale non creata"
        row = df.filter(col("contratto_cod")=="C1").collect()[0]
        # valid_from date should be 2023-01-01
        print("TEST1: row valid_from_ts:", row["valid_from_ts"], "valid_to_ts:", row["valid_to_ts"], "is_current:", row["is_current"])

        ######################
        # TEST 2: second run with changed status closes previous and inserts new open row
        ######################
        f2 = os.path.join(crm_dir, "header_20230102.csv")
        rows2 = [
            # same contratto, later event_time and different status_quote
            ["C1","ORD1","365","P1","2022-01-01","100.00","","","AG1","Rifiutata","11/25/2022","2023-01-02 12:00:00"]
        ]
        write_csv(f2, header, rows2)

        he.run_job_header(spark, f2, delta_path, discarded_path, metrics_path)

        df2 = spark.read.format("delta").load(delta_path).filter(col("contratto_cod") == "C1").orderBy(col("valid_from_ts").asc())
        rows = df2.collect()
        assert len(rows) >= 2, "TEST2: non sono presenti due version rows per contratto dopo update"
        # first row should have valid_to equal to the second valid_from (or at least closed)
        first = rows[0]
        second = rows[-1]
        print("TEST2: first.valid_from_ts:", first["valid_from_ts"], "first.valid_to_ts:", first["valid_to_ts"], "second.valid_from_ts:", second["valid_from_ts"], "second.valid_to_ts:", second["valid_to_ts"])
        assert not first["is_current"], "TEST2: prima riga dovrebbe essere chiusa (is_current False)"
        assert second["is_current"], "TEST2: seconda riga dovrebbe essere aperta (is_current True)"

        ######################
        # TEST 3: intrabatch multiple events -> multiple versions with contiguous intervals
        ######################
        f3 = os.path.join(crm_dir, "header_20230103.csv")
        rows3 = [
            ["C2","ORD2","365","P1","","200.00","","","AG2","Accepted","","2023-01-03 09:00:00"],
            ["C2","ORD2","365","P1","","200.00","","","AG2","Rifiutata","","2023-01-03 15:00:00"]
        ]
        write_csv(f3, header, rows3)

        he.run_job_header(spark, f3, delta_path, discarded_path, metrics_path)

        df_c2 = spark.read.format("delta").load(delta_path).filter(col("contratto_cod") == "C2").orderBy(col("valid_from_ts"))
        c2_rows = df_c2.collect()
        assert len(c2_rows) == 2, f"TEST3: expected 2 versions for C2, got {len(c2_rows)}"
        # first.valid_to should equal second.valid_from (timestamps as dates may differ depending on casting)
        print("TEST3 rows:")
        for r in c2_rows:
            print(r["valid_from_ts"], r["valid_to_ts"], r["is_current"])
        
        assert c2_rows[0]["valid_to_ts"] == c2_rows[1]["valid_from_ts"], "TEST3: intervalli non contigui"
        assert not c2_rows[0]["is_current"], "TEST3: prima riga dovrebbe essere chiusa (is_current False)"
        assert c2_rows[1]["is_current"], "TEST3: seconda riga dovrebbe essere aperta (is_current True)"

        ######################
        # TEST 4: deduplication keeps only latest duplicate (same contratto, event_time different)
        ######################
        f4 = os.path.join(crm_dir, "header_20230104.csv")

        # 2 records duplicated
        rows4 = [
            ["C3","ORD3","365","P1","","50.00","","","AG3","Accepted","","2023-01-04 08:00:00"],
            ["C3","ORD3","365","P1","","50.00","","","AG3","Accepted","","2023-01-04 08:00:00"],
            ["C3","ORD3","365","P1","","50.00","","","AG3","Accepted","","2023-01-04 08:00:00"],
            ["C3","ORD3","365","P1","","50.00","","","AG3","Accepted","","2023-01-04 08:00:00"],
            ["C3","ORD3","365","P1","","50.00","","","AG3","Signed","","2023-01-04 09:00:00"],
            ["C10","ORD3","365","P1","","50.00","","","AG3","Suspended","","2023-01-04 09:00:00"]  
        ]
        write_csv(f4, header, rows4)
        he.run_job_header(spark, f4, delta_path, discarded_path, metrics_path)
        df_c3 = spark.read.format("delta").load(delta_path).filter(col("contratto_cod") == "C3").orderBy(col("valid_from_ts").asc())
        # should keep only latest event (one open version)
        versions_c3 = df_c3.collect()
        assert len(versions_c3) == 2, f"TEST4: expected records dedup to 2, got {len(versions_c3)}"
        assert versions_c3[0]["status_quote"] == "Accepted", f"TEST4: expected first record status_quote Accepted, got {versions_c3[0]['status_quote']}"
        assert versions_c3[1]["status_quote"] == "Signed", f"TEST4: expected second record status_quote Signed, got {versions_c3[1]['status_quote']}"
        assert versions_c3[1]["is_current"], f"TEST4: expected second record is_current True, got {versions_c3[1]['is_current']}"
        assert not versions_c3[0]["is_current"], f"TEST4: expected first record is_current False, got {versions_c3[0]['is_current']}"

        df_c10 = spark.read.format("delta").load(delta_path).filter(col("contratto_cod") == "C10").orderBy(col("valid_from_ts").asc())
        versions_c10 = df_c10.collect()
        assert len(versions_c10) == 1, f"TEST4: expected records dedup to 1 for C10, got {len(versions_c10)}"
        assert versions_c10[0]["is_current"], f"TEST4: expected record is_current True, got {versions_c10[0]['is_current']}"
        
        ######################
        # TEST 5: idempotence - rerun same file should NOT increase inserted_count
        ######################
        f5 = os.path.join(crm_dir, "header_20230105.csv")
        rows5 = [
            ["C4","ORD4","365","P1","","75.00","","","AG4","Accepted","","2023-01-05 11:00:00"]
        ]
        write_csv(f5, header, rows5)
        he.run_job_header(spark, f5, delta_path, discarded_path, metrics_path)  # first run
        before = spark.read.format("delta").load(delta_path).filter(col("contratto_cod") == "C4")
        before_count = before.count()
        before.show(truncate=False)
        
        f6 = os.path.join(crm_dir, "header_20230105.csv")
        rows6 = [
            ["C4","ORD4","365","P1","","75.00","","","AG4","Accepted","","2023-01-05 11:00:00"],
            ["C4","ORD4","365","P1","","75.00","","","AG4","Signed","","2023-01-05 12:00:00"],
        ]
        write_csv(f6, header, rows6)
        
        
        he.run_job_header(spark, f6, delta_path, discarded_path, metrics_path)  # rerun same batch
        after = spark.read.format("delta").load(delta_path).filter(col("contratto_cod") == "C4").orderBy(col("valid_from_ts").asc())
        after_count = after.count()
        after.show(truncate=False)
        assert before_count == after_count - 1, f"TEST5: idempotence failed (before {before_count} != after {after_count} - 1)"
        assert after.collect()[-1]["status_quote"] == "Signed", f"TEST5: expected last record status_quote Signed, got {after.collect()[-1]['status_quote']}"
        print("TEST5 idempotence ok: count", before)

        print("\nALL TESTS PASSED ✅")

    finally:
        # cleanup
        try:
            spark.stop()
        except:
            pass
        # optionally remove tmp dir
        print("Cleaning tmp dir:", tmp_root)
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    run_all_tests()
