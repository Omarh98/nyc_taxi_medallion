from delta.tables import DeltaTable
import pyspark.sql.functions as F
import datetime

def run_silver_pipeline(spark, config, run_id, log_fn):
    """
    Modular ETL Engine for high-volume ingestion from Bronze to Silver/Quarantine layers.
    
    This function implements a robust, incremental processing pattern designed for 
    production-grade data lakes. It orchestrates the end-to-end lifecycle of a 
    Silver-tier dataset, including state management, data contract enforcement, 
    and multi-target atomic writes.

    Key Engineering Patterns:
    - Watermarking: Tracks 'max_bronze_ts' via Delta logs to enable incremental processing.
    - Functional Injection: Supports dynamic derived transformations passed via config.
    - Quality Gates: Implements a 'Quarantine' pattern where rows failing validation 
      are diverted to a separate table for inspection without breaking the pipeline.
    - Deterministic Hashing: Generates 'trip_id' via SHA-256 for idempotent deduplication.
    - Fault Tolerance: Features a 'Delta Restore' rollback mechanism that reverts the 
      target tables to their previous valid state in the event of a runtime exception.

    Args:
        spark (SparkSession): The active SparkSession.
        config (dict): Centralized configuration dictionary containing:
            - dataset_name (str): Logical identifier for the pipeline.
            - bronze_table/silver_table/quarantine_table (str): Target table paths.
            - rename_mapping/type_mapping (dict): Schema enforcement contracts.
            - validation_rules (list): Column expressions for data quality filtering.
            - hash_columns (list): Columns used to generate the unique row fingerprint.
        run_id (str/UUID): Unique execution ID for audit trailing.
        log_fn (callable): External logging utility to persist operational metrics.

    Returns:
        None: Updates the Silver and Quarantine tables and persists metadata to audit logs.
    """
    
    # --- 1. SETUP & UNPACKING ---
    start_ts = datetime.datetime.now()
    dataset_name = config["dataset_name"]
    log_table = config.get("log_table", "nyc_taxi.logs.silver_ingestion_logs")
    is_init = config.get("init_run", False)
    
    # Initialize metrics
    metrics = {
        "bronze": 0, "silver": 0, "quarantine": 0, 
        "duplicates": 0, "silver_survivors": 0, "quarantine_survivors": 0
    }
    success = silver_load_success = quarantine_load_success = False
    error_msg = ""
    max_bronze_ts = None

    try:
        # --- 2. WATERMARKING (Handling Cold Start) ---
        print(f"[{dataset_name}] Initializing metadata...")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} USING DELTA")
        
        last_success_ts = None
        if not is_init:
            log_history = spark.read.table(log_table) \
                .filter(f"status = 'Success' AND dataset_name = '{dataset_name}'")
            
            # Check if there is any history for this dataset
            if log_history.limit(1).count() > 0:
                last_success_ts = log_history.agg({"max_bronze_ts": "max"}).collect()[0][0]

        # --- 3. INGESTION ---
        df = spark.read.table(config["bronze_table"])
        
        # Apply Watermark if not in Init mode
        if last_success_ts and not is_init:
            lookback = config.get("lookback_minutes", 5)
            watermark = last_success_ts - datetime.timedelta(minutes=lookback)
            print(f"[{dataset_name}] Incremental load from: {watermark}")
            # Filtering on system column discovered in Bronze
            df = df.filter(F.col("_ingest_ts") > F.lit(watermark))
        else:
            print(f"[{dataset_name}] Full/Initial load triggered.")

        metrics["bronze"] = df.count()
        if metrics["bronze"] == 0:
            print(f"[{dataset_name}] No new data to process.")
            max_bronze_ts = last_success_ts
            success = True
            error_msg = "No new data to process."
            return

        # --- 4. TRANSFORMATIONS ---
        # A. Rename Columns
        df = df.withColumnsRenamed(config["rename_mapping"])

        # B. Derived Columns (Function Injection from Notebook)
        if "derived_transformations" in config:
            for transform_func in config["derived_transformations"]:
                print(f"[{dataset_name}] Applying derivation: {transform_func.__name__}")
                df = transform_func(df)
        
        # C. Cast Types (Dynamic based on Dictionary)
        # We only cast if the column exists to avoid "column not found" errors
        df = df.select([
            F.col(c).cast(config["type_mapping"][c]) if c in config["type_mapping"] else F.col(c) 
            for c in df.columns
        ])

        # D. Technical Metadata & Deduplication
        df = (df.withColumn("trip_id", F.sha2(F.concat_ws("||", *config["hash_columns"]), 256))
                .withColumn("ingest_ts", F.current_timestamp())
                .withColumn("run_id", F.lit(run_id))
                .dropDuplicates(["trip_id"]))

        # --- 5. VALIDATION (Quarantine Logic) ---
        df = df.withColumn("quarantine_reasons", F.array())
        for rule in config["validation_rules"]:
            df = df.withColumn("quarantine_reasons", 
                F.when(rule.isNotNull(), F.array_union("quarantine_reasons", F.array(rule)))
                .otherwise(F.col("quarantine_reasons")))

        # --- 6. ATOMIC MERGES ---
        silver_df = df.filter(F.size("quarantine_reasons") == 0).drop("quarantine_reasons")
        quarantine_df = df.filter(F.size("quarantine_reasons") > 0)

        targets = [
            (config["silver_table"], silver_df, "silver"),
            (config["quarantine_table"], quarantine_df, "quarantine")
        ]

        # Record max timestamp for the log
        max_bronze_ts = df.select(F.max("ingest_ts")).collect()[0][0]

        for path, target_df, label in targets:
            print(f"[{dataset_name}] Merging to {label} table...")
            dt = DeltaTable.forName(spark, path)
            
            dt.alias("t").merge(
                target_df.alias("s"), "t.trip_id = s.trip_id"
            ).whenNotMatchedInsertAll().execute()
            
            if label == "silver": silver_load_success = True
            else: quarantine_load_success = True
            
            # Extract Metrics from Delta History
            op_metrics = dt.history(1).select("operationMetrics").collect()[0][0]
            metrics[label] = int(op_metrics.get("numTargetRowsInserted", 0))
            metrics[f"{label}_survivors"] = int(op_metrics.get("numSourceRows", 0))

        # --- 7. FINAL METRICS ---
        metrics["duplicates"] = metrics["bronze"] - (metrics["silver_survivors"] + metrics["quarantine_survivors"])
        success = True
        print(f"[{dataset_name}] Processed: {metrics['bronze']} | Silver: {metrics['silver']} | Dupes: {metrics['duplicates']}")

    except Exception as e:
        error_msg = str(e)
        print(f"[{dataset_name}] JOB FAILED: {error_msg}")
        
        # --- ROBUST ROLLBACK ---
        for table, flag in [(config["silver_table"], silver_load_success), 
                            (config["quarantine_table"], quarantine_load_success)]:
            if flag:
                print(f"Checking rollback capability for {table}...")
                # Fetch history to see if we have a version to go back to
                history = spark.sql(f"DESCRIBE HISTORY {table}").limit(2).collect()
                
                if len(history) > 1:
                    # history[0] is current (failed/incomplete), history[1] is the safe state
                    prev_version = history[1][0] 
                    print(f"Restoring {table} to version {prev_version}...")
                    spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {prev_version}")
                else:
                    # If only 1 version exists, the table was likely created in this run.
                    # Restoring isn't possible, we may need to truncate or let the next merge handle it.
                    print(f"No previous version for {table}. Manual cleanup may be required.")

    finally:
        # capturing end timestamp for the job
        end_ts = datetime.datetime.now()
        
        # AUDIT LOGGING
        log_fn(
            spark=spark, run_id=run_id, dataset_name=dataset_name, 
            start_ts=start_ts, end_ts=end_ts, max_bronze_ts=max_bronze_ts,
            success=success, bronze_count=metrics["bronze"], 
            silver_count=metrics["silver"], quarantine_count=metrics["quarantine"], 
            duplicates_dropped=metrics["duplicates"], error_msg=error_msg, 
            log_table=log_table, catalog_table=config["silver_table"]
        )


def apply_scd_type_1(source_df, target_table, business_key, rename_dict, cast_dict):
    """
    Performs a Slowly Changing Dimension (SCD) Type 1 Merge on a Delta table.
    
    This function synchronizes the target table with the source data by updating 
    existing records and inserting new ones. It does NOT maintain history; 
    existing values in the target table are overwritten if they differ from the source.

    Args:
        source_df (DataFrame): The raw/Bronze Spark DataFrame containing new data.
        target_table (str): The name of the Silver table to update (e.g., 'silver.zones').
        business_key (str): The unique identifier (after renaming) used for matching.
        rename_dict (dict): A mapping of {"Old_Name": "new_name"} for column alignment.
        cast_dict (dict): A mapping of {"new_name": "data_type"} to ensure type safety.

    Returns:
        None: Executes the merge directly on the Delta Lake table.
    """
    
    # 1. Transform: Rename and cast/select based on the new names
    clean_df = source_df.withColumnsRenamed(rename_dict).select([
        F.col(c).cast(cast_dict[c]) for c in rename_dict.values()
    ])

    # 2. Initialization: Create the table if it doesn't exist
    if not spark.catalog.tableExists(target_table):
        clean_df.write.format("delta").saveAsTable(target_table)
        return

    # 3. Atomic Merge: Overwrite matching records and insert new ones
    target_delta = DeltaTable.forName(spark, target_table)
    
    (target_delta.alias("target")
     .merge(
         clean_df.alias("source"), 
         f"target.{business_key} = source.{business_key}"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    

def apply_scd_type_2(source_df, target_table, business_key, rename_dict, cast_dict, attr_columns):
    """
    Performs a Slowly Changing Dimension (SCD) Type 2 Merge on a Delta table.
    
    This function tracks historical changes for specific attributes by 'expiring' 
    old records and inserting new 'current' versions. It handles renaming, 
    casting, and versioning logic in a single atomic transaction.

    Args:
        source_df (DataFrame): The raw/Bronze Spark DataFrame containing new data.
        target_table (str): The name of the Silver table to update (e.g., 'silver.licenses').
        business_key (str): The unique identifier (after renaming) used for matching.
        rename_dict (dict): A mapping of {"Old_Name": "new_name"} for column alignment.
        cast_dict (dict): A mapping of {"new_name": "data_type"} to ensure type safety.
        attr_columns (list): Columns where a value change triggers a new historical record.

    Returns:
        None: Executes the merge directly on the Delta Lake table.
    """
    
    # 1. Transform: Clean and align with the Silver "Contract"
    clean_df = source_df.withColumnsRenamed(rename_dict).select([
        F.col(c).cast(cast_dict[c]) for c in rename_dict.values()
    ])

    # 2. Initialization: Create the table if it doesn't exist
    if not spark.catalog.tableExists(target_table):
        (clean_df
         .withColumn("start_at", F.current_timestamp())
         .withColumn("end_at", F.lit(None).cast("timestamp"))
         .withColumn("is_current", F.lit(True))
         .write.format("delta").saveAsTable(target_table))
        return

    target_delta = DeltaTable.forName(spark, target_table)
    
    # 3. Change Detection: Check if any tracked attributes have changed
    change_cond = " OR ".join([f"target.{c} <> source.{c}" for c in attr_columns])

    # Find records that exist and have changed values
    staged_updates = clean_df.join(
        target_delta.toDF().filter("is_current = true"),
        business_key
    ).where(change_cond).select(clean_df["*"], F.lit(True).alias("needs_update"))

    # Combine original data with the "update" markers
    upsert_df = clean_df.withColumn("needs_update", F.lit(False)).unionByName(staged_updates)

    # 4. Atomic Merge: Expire old versions and insert new ones
    (target_delta.alias("target")
     .merge(
         upsert_df.alias("source"), 
         f"target.{business_key} = source.{business_key} AND target.is_current = true"
     )
     .whenMatchedUpdate(
         condition = "source.needs_update = true",
         set = {"end_at": F.current_timestamp(), "is_current": "false"}
     )
     .whenNotMatchedInsert(values = {
         **{c: f"source.{c}" for c in rename_dict.values()},
         "start_at": F.current_timestamp(),
         "end_at": "null",
         "is_current": "true"
     })
     .execute())