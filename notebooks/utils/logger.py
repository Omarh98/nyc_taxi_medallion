from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,DoubleType

def log_bronze_ingestion(spark, run_id, dataset_name, catalog_table, log_table, success=True, error_msg=""):
    """
    Persists operational metadata for the Bronze ingestion layer.
    
    This function captures high-level execution metrics, specifically focusing on 
    the volume of raw data discovered and successfully committed to the Bronze 
    Delta table for a specific execution run.

    Args:
        spark (SparkSession): The active SparkSession.
        run_id (str): Unique UUID for the current pipeline execution.
        dataset_name (str): Logical name of the source dataset (e.g., 'green_taxi').
        catalog_table (str): Target Bronze table name used to calculate record counts.
        log_table (str): The centralized Delta table where audit logs are stored.
        success (bool): Indicates if the ingestion completed without exceptions.
        error_msg (str): Detailed error description if success is False.
    """
    if success:
        #get row count for the specific run
        ingested_rows=spark.table(catalog_table).filter(f"run_id='{run_id}'").count()
        status_msg="Success"
    else:
        ingested_rows=0
        status_msg="Failure"

    log_schema = StructType([
    StructField("run_id", StringType(), nullable=False),
    StructField("dataset_name", StringType(), nullable=False),
    StructField("catalog_table", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("ingested_rows", IntegerType(), nullable=True),
    StructField("error_msg", StringType(), nullable=True)
])
    log_entry=spark.createDataFrame(
        [(run_id,dataset_name,catalog_table,status_msg,ingested_rows,error_msg)],
        schema=log_schema,
    ).withColumn("log_ts",current_timestamp())
    log_entry.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(log_table)    


def log_silver_ingestion(spark, run_id, dataset_name, catalog_table, log_table, start_ts, end_ts,
                         max_bronze_ts, bronze_count, silver_count, quarantine_count, 
                         duplicates_dropped, success=True, error_msg=""):
    """
    Records granular transformation metrics and performance data for the Silver layer.
    
    This function acts as the 'black box' recorder for the Silver engine. It tracks 
    data lineage (max_bronze_ts), processing efficiency (duration), and data 
    quality distribution (silver vs. quarantine vs. duplicates).

    Key Metrics Logged:
    - Data Balance: Tracks how many rows 'survived' the quality gates.
    - Watermark State: Records the 'max_bronze_ts' to ensure incremental continuity.
    - Performance: Calculates execution duration for bottleneck analysis.
    - Reliability: Captures stack traces in the 'error_msg' field for failed runs.

    Args:
        start_ts (datetime): Execution start time.
        end_ts (datetime): Execution end time.
        max_bronze_ts (timestamp): The latest event time processed in this batch.
        bronze_count (int): Total rows read from the source.
        silver_count (int): Rows successfully merged into Silver.
        quarantine_count (int): Rows diverted to the Quarantine table.
        duplicates_dropped (int): Rows removed during the deduplication phase.
    """
    if success:
        status_msg="Success"
    else:
        status_msg="Failure"
    duration = (end_ts - start_ts).total_seconds()
    log_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("catalog_table", StringType(), False),
        StructField("status", StringType(), False),
        StructField("start_ts", TimestampType(), False),
        StructField("end_ts", TimestampType(), False),
        StructField("duration", DoubleType(), True),
        StructField("max_bronze_ts", TimestampType(), True),
        StructField("bronze_count", IntegerType(), True),
        StructField("silver_count", IntegerType(), True),
        StructField("quarantine_count", IntegerType(), True),
        StructField("duplicates_dropped", IntegerType(), True),
        StructField("error_msg", StringType(), True)
    ])

    data=[(run_id, 
        dataset_name, 
        catalog_table, 
        status_msg, 
        start_ts, 
        end_ts, 
        duration,           
        max_bronze_ts,      
        bronze_count,       
        silver_count,       
        quarantine_count,   
        duplicates_dropped,
        error_msg
    )]
    log_entry=spark.createDataFrame( data, schema=log_schema)
    log_entry.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(log_table)
    
     

     


