from pyspark.sql.functions import regexp_extract, current_timestamp, lit, col, count

def ingest_bronze(
    spark,
    run_id,
    landing_path,
    bronze_path,
    schema_path,
    checkpoint_path,
    catalog_table,
):
    """
    Bronze ingestion function.
    - filename-based year/month partitioning
    - metadata columns: _ingest_ts, _source_file, _dataset
    - schema evolution enabled
    """

    # 1️ Read files with Autoloader and add metadata columns
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(landing_path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn ("run_id", lit(run_id))
        # 2️ Extract year/month from filename
        .withColumn("year", regexp_extract("_source_file", r"_(\d{4})-", 1).cast("int"))
        .withColumn("month", regexp_extract("_source_file", r"-(\d{2})", 1).cast("int"))
    )

    # 3️ Write to Bronze Delta with partitioning and schema evolution
    (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("year", "month")
        .trigger(availableNow=True)  # batch mode
        .start(bronze_path)
        .awaitTermination()
    )

    # 4️ Register Delta table in Unity Catalog
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {catalog_table}
            USING DELTA
            LOCATION '{bronze_path}'
        """
    )