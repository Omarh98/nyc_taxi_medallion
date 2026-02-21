from pyspark.sql.functions import regexp_extract, current_timestamp, lit, col, count

def ingest_bronze(spark, run_id, landing_path, bronze_path, schema_path, checkpoint_path, catalog_table):
    """
    Implements an Incremental Ingestion Engine using Databricks Autoloader (cloudFiles).
    
    This function serves as the entry point for the Medallion Architecture, providing 
    a highly scalable 'Bronze' layer that captures raw data with minimal transformation 
    while maintaining a robust audit trail.

    Key Engineering Patterns:
    - Autoloader (cloudFiles): Optimized incremental file discovery using file 
      notification and directory listing with persistent state management.
    - Schema Inference & Evolution: Decouples source data changes from pipeline 
      stability by managing schema drifts via 'schemaLocation'.
    - Metadata Injection: Enriches raw records with system-level lineage, including 
      the source filename, ingestion timestamp, and execution UUID (run_id).
    - Dynamic Partitioning: Leverages RegEx to extract 'year' and 'month' from 
      filenames to optimize downstream query performance and data pruning.
    - Idempotent Checkpointing: Ensures exactly-once processing semantics by 
      tracking file state in the 'checkpoint_path'.

    Args:
        spark (SparkSession): The active SparkSession.
        run_id (str): Unique identifier for the orchestration run.
        landing_path (str): Cloud storage path (S3/ADLS/GCS) where raw files arrive.
        bronze_path (str): Path where the raw Delta table is persisted.
        schema_path (str): Storage location for the Autoloader to track schema changes.
        checkpoint_path (str): Storage location for Spark Structured Streaming state.
        catalog_table (str): The Unity Catalog or Hive Metastore table name.

    Returns:
        None: Executes the stream to completion and registers the table in the catalog.
    """

    # 1️. Read files with Autoloader and add metadata columns
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(landing_path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn ("run_id", lit(run_id))
        # 2️. Extract year/month from filename
        .withColumn("year", regexp_extract("_source_file", r"_(\d{4})-", 1).cast("int"))
        .withColumn("month", regexp_extract("_source_file", r"-(\d{2})", 1).cast("int"))
    )

    # 3️. Write to Bronze Delta with partitioning and schema evolution
    (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("year", "month")
        .trigger(availableNow=True)  # batch mode
        .start(bronze_path)
        .awaitTermination()
    )

    # 4️. Register Delta table in Unity Catalog
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {catalog_table}
            USING DELTA
            LOCATION '{bronze_path}'
        """
    )