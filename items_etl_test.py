import unittest
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, to_date, udf, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from items_etl import run_job_items
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

        table_name = "items/"
        crm_path= "crm/"
        
        cls.crm_table_path = crm_path + table_name 
        cls.write_path = "landing_test/" + table_name

        cls.crm_header_table_path = crm_path + "header/"
        cls.write_header_path = "landing_test/header/"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


    def test_schema_items(self):

        # Make this test idempotent
        os.system("rm -rf {}".format(self.write_path))

        data_items_metadata = {
            "contratto_cod": {"data_type": "varchar", "length": 9, "key": True, "nullable": False},
            "numero_annuncio": {"data_type": "varchar", "length": 2, "key": True, "nullable": False},
            "list_total": {"data_type": "decimal", "length": (18, 2), "key": False, "nullable": False},
            "contracted_price": {"data_type": "decimal", "length": (18, 2), "key": False, "nullable": True},
            "total_discount": {"data_type": "decimal", "length": (18, 2), "key": False, "nullable": True},
            "data_attivazione": {"data_type": "date", "format": "dd/mm/yyyy", "key": False, "nullable": True},
            "data_fine_prestazione": {"data_type": "date", "format": "dd/mm/yyyy", "key": False, "nullable": True},
            "product_code": {"data_type": "varchar", "length": 50, "key": False, "nullable": False},
            "quantity": {"data_type": "number", "length": None, "key": False, "nullable": False},
            "causale_annullamento": {"data_type": "varchar", "length": 50, "key": False, "nullable": True},
            "data_annullamento": {"data_type": "date", "format": "dd/mm/yyyy", "key": False, "nullable": True},
            "status_item": {"data_type": "varchar", "length": 1, "key": False, "nullable": False},
            "creazione_dta": {"data_type": "date", "format": "yyyyMMdd", "key": False, "nullable": False}
        }

        df = self.spark.read.option("header", "true").option("sep", "|")\
                            .csv(self.crm_table_path + 'items_corrected_date.txt')

        for field, metadata in data_items_metadata.items():
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
                                    when(col(field).isNotNull() & to_date(col(field),metadata['format']).isNull(),
                                    True).otherwise(False))

                self.assertEqual(check_df.filter(col("check_date_format")==True).count(), 
                                    0, 
                                    f"Field '{field}' hasn't {metadata['format']} format.")


    def test_items_history(self):

        # Make this test idempotent
        os.system("rm -rf {}".format(self.write_path))
        
        run_job_items(self.spark, self.crm_table_path + "items_20230123.txt", self.write_path)

        self.assertEqual(DeltaTable.isDeltaTable(self.spark, self.write_path), 
                        True, 
                        f"{self.write_path} is not a Delta Table")

        
        run_job_items(self.spark, self.crm_table_path + "items_20230125.txt", self.write_path)

        df = self.spark.read.format("delta").load(self.write_path)
        filtered = df.filter(col('contratto_cod')== 'Y06119362').filter(col('numero_annuncio')== '10')

        self.assertEqual(filtered.count(), 
                        2, 
                        f"{self.write_path} Error in record history")

        self.assertEqual(filtered.filter(col('valid_to') == '9999-12-31')\
                                    .filter(col('valid_from') == '2023-01-25').count(), 
                        1, 
                        f"{self.write_path} No uniqiue current record")

        self.assertEqual(filtered.filter(col('valid_to') == '2023-01-25').count(), 
                        1, 
                        f"{self.write_path} No unique old record")

        self.assertEqual(filtered.filter(col('valid_to') == '9999-12-31')\
                                    .filter(col('contracted_price') == 500.00).count(), 
                        1, 
                        f"{self.write_path} Error in record history for field status quote")   

        


    def test_items_queries(self):
        
        # Make this test idempotent
        os.system("rm -rf {}".format(self.write_path))
        os.system("rm -rf {}".format(self.write_header_path))

        run_job_items(self.spark, self.crm_table_path + "items_20230121.txt", self.write_path)

        run_job_items(self.spark, self.crm_table_path + "items_20230122.txt", self.write_path)

        delta_df = self.spark.read.format("delta").load(self.write_path)
        delta_df.createOrReplaceTempView("items")

        query = f"""
            SELECT *
            FROM items
            WHERE valid_from = date('2023-01-21') and valid_to = date('2023-01-22') 
        """

        self.spark.sql(query).show(truncate=False)

        run_job_items(self.spark, self.crm_table_path + "items_20230123.txt", self.write_path)

        run_job_items(self.spark, self.crm_table_path + "items_20230124.txt", self.write_path)


        delta_df = self.spark.read.format("delta").load(self.write_path)
        delta_df.createOrReplaceTempView("items")

        query = f"""
            SELECT count(*) as numero_variazioni
            FROM items
            WHERE 
            contratto_cod = 'Y06119362' AND numero_annuncio = 10
            AND valid_to <> date('9999-12-31') 
        """

        self.assertEqual(self.spark.sql(query).collect()[0][0], 
                        1,
                        f"unexpected number of variations")

        
        items_df = self.spark.read.format("delta").load(self.write_path)
        items_df.createOrReplaceTempView("items")

        run_job_header(self.spark, self.crm_header_table_path + "header_20230121.txt", self.write_header_path)
        run_job_header(self.spark, self.crm_header_table_path + "header_20230122.txt", self.write_header_path)
        run_job_header(self.spark, self.crm_header_table_path + "header_20230123.txt", self.write_header_path)
        run_job_header(self.spark, self.crm_header_table_path + "header_20230124.txt", self.write_header_path)

        header_df = self.spark.read.format("delta").load(self.write_header_path)
        header_df.createOrReplaceTempView("header")

        query = f"""
 
            SELECT h.*
                FROM header h
                LEFT JOIN items i
                    ON h.contratto_cod = i.contratto_cod 
                        AND h.valid_from_year = i.valid_from_year
                        AND h.valid_from_month = i.valid_from_month
                        AND h.valid_from_day = i.valid_from_day 
                WHERE  i.numero_annuncio is null 
        """

        self.spark.sql(query).show(truncate=False)

        query = f"""
            SELECT distinct 
            FROM items
            WHERE status_item = 'L' 
                AND h.valid_to = date('9999-12-31') 
				AND i.valid_to = date('9999-12-31')
        """

        self.spark.sql(query).show(truncate=False)
        


if __name__ == '__main__':

    unittest.main()
