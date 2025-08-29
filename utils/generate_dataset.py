

# genera items sintetici (esempio)

def generate_data(spark, container, storage_account):
    n_contracts = 1_000_000  # scala per small/medium/large
    n_items_per_contract = 3

    contracts_df = spark.range(0, n_contracts).withColumnRenamed("id", "contratto_id")
    items_df = contracts_df.crossJoin(spark.range(0, n_items_per_contract).withColumnRenamed("id","numero_annuncio")) \
        .withColumn("contratto_cod", expr("concat('C', contratto_id)")) \
        .withColumn("numero_annuncio", expr("cast(numero_annuncio+1 as string)")) \
        .withColumn("contracted_price", expr("round(rand()*1000,2)")) \
        .withColumn("product_code", expr("concat('P', floor(rand()*1000))")) \
        .withColumn("status_item", lit("A")) \
        .select("contratto_cod","numero_annuncio","contracted_price","product_code","status_item")

    items_df.write.mode("overwrite").option("sep","|")\
        .csv(f"abfss://{container}@{storage_account}.dfs.core.windows.net/test/items_{n_contracts}.csv")
