from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,LongType

def log_bronze_ingestion(spark,run_id,dataset_name,catalog_table,log_table,success=True,error_msg=""):
    """
    Logging function: records ingested rows or failure for a run.
    """
    if success:
        #get row count for the specific run
        ingested_rows=spark.table(catalog_table).filter(f"run_id='{run_id}'").count().cast("long")
        status_msg="Success"
    else:
        ingested_rows=0
        status_msg="Failure"

    log_schema = StructType([
    StructField("run_id", StringType(), nullable=False),
    StructField("dataset_name", StringType(), nullable=False),
    StructField("catalog_table", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("ingested_rows", LongType(), nullable=True),
    StructField("error_msg", StringType(), nullable=True)
])
    log_entry=spark.createDataFrame(
        [(run_id,dataset_name,catalog_table,status_msg,ingested_rows,error_msg)],
        schema=log_schema,
    ).withColumn("log_ts",current_timestamp())
    log_entry.write.format("delta").mode("append").saveAsTable(log_table)    

     

     


