from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,DoubleType

def log_bronze_ingestion(spark,run_id,dataset_name,catalog_table,log_table,success=True,error_msg=""):
    """
    Logging function: records ingested rows or failure for a run.
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


def log_silver_ingestion(spark,run_id,dataset_name,catalog_table,log_table,start_ts,end_ts,
                         max_bronze_ts,bronze_count,silver_count,quarantine_count,duplicates_dropped,success=True,error_msg=""):
    """
    Logging function: records ingested rows or failure for a run.
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
        duration,           # Matches Column 7
        max_bronze_ts,      # Matches Column 8
        bronze_count,       # Matches Column 9
        silver_count,       # Matches Column 10
        quarantine_count,   # Matches Column 11
        duplicates_dropped,
        error_msg
    )]
    log_entry=spark.createDataFrame( data, schema=log_schema)
    log_entry.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(log_table)
    
     

     


