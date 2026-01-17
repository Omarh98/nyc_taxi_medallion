from delta.tables import DeltaTable
import pyspark.sql.functions as F
import datetime

def run_silver_pipeline(spark, config, run_id, log_fn):
    """
    Modular Engine to process any dataset into Silver/Quarantine layers.
    
    :param spark: SparkSession
    :param config: Dictionary containing table paths, mappings, rules, and init_run flag
    :param run_id: Unique UUID for the run
    :param log_fn: The logging function imported from your utils
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
        # cleanup: technically not needed on Serverless but good practice
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