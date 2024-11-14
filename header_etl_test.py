import unittest
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, to_date, udf, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from header_etl import run_job_header
from delta.tables import DeltaTable



class TestDataContract(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestDataContract") \
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.1.0")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .getOrCreate()

        table_name = "header/"
        crm_path= "crm/"
        
        cls.crm_table_path = crm_path + table_name 
        cls.write_path = "landing_test/" + table_name

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_schema_header(self):

        data_contract_metadata = {
            "contratto_cod": {"data_type": "varchar", "length": 9, "key": True, "nullable": False},
            "codice_ordine_sap": {"data_type": "varchar", "length": 10, "key": False, "nullable": True},
            "tipo_contratto": {"data_type": "varchar", "length": 6, "key": False, "nullable": False},
            "codice_opec": {"data_type": "varchar", "length": 8, "key": False, "nullable": False},
            "data_firma": {"data_type": "date", "format": "dd/mm/yyyy", "key": False, "nullable": True},
            "net_amount": {"data_type": "decimal", "length": (18, 2), "key": False, "nullable": False},
            "causale_annullamento": {"data_type": "varchar", "length": 50, "key": False, "nullable": True},
            "data_annullamento": {"data_type": "date", "format": "dd/mm/yyyy", "key": False, "nullable": True},
            "codice_agente": {"data_type": "varchar", "length": 10, "key": False, "nullable": False},
            "status_quote": {"data_type": "varchar", "length": 50, "key": True, "nullable": False},
            "creazione_dta": {"data_type": "date", "format": "dd/mm/yyyy", "key": False, "nullable": False}
        }

        df = self.spark.read.option("header", "true").option("sep", "|")\
                            .csv(self.crm_table_path + "header_corrected_date.txt")


        for field, metadata in data_contract_metadata.items():
            if not metadata["nullable"]:
                self.assertEqual(df.filter(col(field).isNull()).count() > 0, 
                                False, 
                                f"Field '{field}' is null")

            if metadata["data_type"] == "varchar":

                max_length = int(metadata["length"])
                self.assertEqual(df.filter(length(col(field)) > max_length).count(), 
                                0, 
                                f"Field '{field}' has length greater than {max_length}")
            
            elif metadata["data_type"] == "date":
                check_df = df.withColumn("check_date_format", 
                                    when(col(field).isNotNull() & to_date(col(field), metadata['format']).isNull(),
                                     True).otherwise(False))

                self.assertEqual(check_df.filter(col("check_date_format")==True).count(), 
                                    0, 
                                    f"Field '{field}' hasn't {metadata['format']} format.")    
    
    def test_header_history(self):
        
        run_job_header(self.spark, self.crm_table_path + "header_20230121.txt", self.write_path)

        self.assertEqual(DeltaTable.isDeltaTable(self.spark, self.write_path), 
                        True, 
                        f"{self.write_path} is not a Delta Table")

        
        run_job_header(self.spark, self.crm_table_path + "header_20230125.txt", self.write_path)

        df = self.spark.read.format("delta").load(self.write_path)
        filtered = df.filter(col('contratto_cod')== 'Y02103210')

        self.assertEqual(filtered.count(), 
                        2, 
                        f"{self.write_path} Error in record history")

        self.assertEqual(filtered.filter(col('valid_to') == '9999-12-31').count(), 
                        1, 
                        f"{self.write_path} No uniqiue current record")

        self.assertEqual(filtered.filter(col('valid_to') == '2023-01-25').count(), 
                        1, 
                        f"{self.write_path} No unique old record")

        self.assertEqual(filtered.filter(col('valid_to') == '9999-12-31')\
                                    .filter(col('status_quote') == 'Other').count(), 
                        1, 
                        f"{self.write_path} Error in record history for field status quote")   

        
        run_job_header(self.spark, self.crm_table_path + "header_20230126.txt", self.write_path)

        df = self.spark.read.format("delta").load(self.write_path)
        filtered = df.filter(col('contratto_cod')== 'Y02103210')

        self.assertEqual(filtered.count(), 
                        3, 
                        f"{self.write_path} Error in record history")

        self.assertEqual(filtered.filter(col('valid_to') == '9999-12-31')\
                                    .filter(col('valid_from') == '2023-01-26').count(), 
                        1, 
                        f"{self.write_path} No uniqiue current record")

        self.assertEqual(filtered.filter(col('valid_to') == '2023-01-26').count(), 
                        1, 
                        f"{self.write_path} No unique old record")

        self.assertEqual(filtered.filter(col('valid_to') == '9999-12-31')\
                                    .filter(col('codice_ordine_sap') == '3014210222').count(), 
                        1, 
                        f"{self.write_path} Error in record history for field status quote")

        # Make this test idempotent
        os.system("rm -rf {}".format(self.write_path))


if __name__ == '__main__':
    unittest.main()
