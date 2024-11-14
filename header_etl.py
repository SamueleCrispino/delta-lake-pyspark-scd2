from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType

from delta.tables import *
from pyspark.sql import Window

##### CONSTANTS
max_date = "99991231"
# date_regex = r'(.+)_(\d{8})\.txt' TODO: make it dynamic
date_regex = r'header_(\d{8})\.txt'

deduplication_keys = ["contratto_cod"]
partition_columns = ["valid_from_year", "valid_from_month", "valid_from_day"]

table_name = "header/"

crm_path= "crm/"


crm_table_path = crm_path + table_name
delta_path = "landing/" + table_name


# HEADER SCHEMA
header_schema = StructType([
    StructField("contratto_cod", StringType(), nullable=False),
    StructField("codice_ordine_sap", StringType(), nullable=True),
    StructField("tipo_contratto", StringType(), nullable=False),
    StructField("codice_opec", StringType(), nullable=False),
    StructField("data_firma", DateType(), nullable=True),
    StructField("net_amount", DecimalType(18, 2), nullable=False),
    StructField("causale_annullamento", StringType(), nullable=True),
    StructField("data_annullamento", DateType(), nullable=True),
    StructField("codice_agente", StringType(), nullable=False),
    StructField("status_quote", StringType(), nullable=True),
    StructField("creazione_dta", DateType(), nullable=False)
])





def run_job_header(spark, read_path, write_path):

    # EXTRACT
    df_extracted = spark.read.option("header", "true").option("sep", "|")\
                    .schema(header_schema)\
                    .csv(read_path)\
                    .withColumn("valid_from", regexp_extract(input_file_name(), date_regex, 1))


    # DAILY BATCH DEDUPLICATION  # log number of records removed
    df_extracted = df_extracted.withColumn("flag", count("*")\
                                .over(Window.partitionBy(deduplication_keys)))\
                                .filter(col('flag')==1)


    # TRANSFORM
    df_transformed = df_extracted.withColumn("valid_from", to_date(col("valid_from"), "yyyyMMdd").cast(DateType()))\
                        .withColumn("valid_to", to_date(lit(max_date), "yyyyMMdd").cast(DateType()))\
                        .withColumn("valid_from_year", year("valid_from"))\
                        .withColumn("valid_from_month", month("valid_from"))\
                        .withColumn("valid_from_day", dayofmonth("valid_from"))\
                        .drop(col("flag"))

    print("SCHEMA:")
    df_transformed.printSchema()


    if not DeltaTable.isDeltaTable(spark, write_path):
        print("INIT DELTA TABLE")
        df_transformed.write.partitionBy(partition_columns).format("delta").save(write_path)

    else:
        print("MERGING DELTA TABLE")

        delta_table = DeltaTable.forPath(spark, write_path)

        # print("Schema delta_table:")
        # delta_table.printSchema()
        
        # Rows to INSERT new variations of existing headers
        newHeadersToInsert = df_transformed \
            .alias("updates") \
            .join(delta_table.toDF().alias("existing"), "contratto_cod") \
            .where("existing.valid_to = date('9999-12-31') AND (updates.status_quote <> existing.status_quote OR updates.codice_agente <> existing.codice_agente OR updates.codice_ordine_sap <> existing.codice_ordine_sap)")

        
        # Stage the update by unioning two sets of rows
        # 1. Rows that will be inserted in the whenNotMatched clause
        # 2. Rows that will either update the variations of existing header or insert the values of new headers
        stagedUpdates = (
            newHeadersToInsert
            .selectExpr("NULL as mergeKey", "updates.*")   # all updates (*)
            .union(df_transformed.selectExpr("contratto_cod as mergeKey", "*"))  # Rows for 2.
        )

        # merge condition won't be matched for (*)
        # Apply SCD Type 2 operation using merge
        delta_table.alias("existing").merge(
            stagedUpdates.alias("staged_updates"),
            "existing.contratto_cod = mergeKey")\
            .whenMatchedUpdate( 
                condition = "existing.valid_to = date('9999-12-31') AND (staged_updates.status_quote <> existing.status_quote OR staged_updates.codice_agente <> existing.codice_agente OR staged_updates.codice_ordine_sap <> existing.codice_ordine_sap)",
                set = {                                      
                    "valid_to": "staged_updates.valid_from"
                }
        ).whenNotMatchedInsert(
            values = {
                "contratto_cod": "staged_updates.contratto_cod",
                "codice_ordine_sap": "staged_updates.codice_ordine_sap",
                "tipo_contratto": "staged_updates.tipo_contratto",
                "codice_opec": "staged_updates.codice_opec",
                "data_firma": "staged_updates.data_firma",
                "net_amount": "staged_updates.net_amount",
                "causale_annullamento": "staged_updates.causale_annullamento",
                "data_annullamento": "staged_updates.data_annullamento",
                "codice_agente": "staged_updates.codice_agente",
                "status_quote": "staged_updates.status_quote",
                "creazione_dta": "staged_updates.creazione_dta",
                "valid_from": "staged_updates.valid_from",
                "valid_from_year": "staged_updates.valid_from_year",
                "valid_from_month": "staged_updates.valid_from_month",
                "valid_from_day": "staged_updates.valid_from_day",
                "valid_to": "staged_updates.valid_to"
            }
        ).execute()



if __name__ == '__main__':
    
    spark = SparkSession.builder \
        .appName("TestDataContract") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.1.0")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

    
    run_job_header(spark, crm_table_path, delta_path)