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
        if not success:
            raise Exception(error_msg)

def apply_scd(spark, staged_df, target_table, business_key, comparison_cols, scd_type):
    """
    Applies SCD Type 1 or Type 2 changes to a Delta Table.
    
    Args:
        spark: SparkSession
        source_df: DataFrame containing the source data (with bronze_id)
        target_table: String name of the target Delta table
        business_key: String name of the unique identifier column
        comparison_cols: List of strings for columns to detect changes on
        scd_type: Integer (1 or 2)
        silver_run_id: String/Int identifier for the current Silver execution
    
    Returns:
        dict: {'rows_inserted': int, 'rows_updated': int, 'total_rows_affected': int}
    """

    # --- 1. PREPARE SOURCE DATA ---
    # We add the Silver-specific metadata here so it's ready for the merge/insert
    # Note: bronze_id is assumed to be in source_df already

    
    # Identify all data columns (excluding the keys and SCD2 tracking cols if they exist in source)
    # We use the source columns as the "Master List" of what to insert/update
    # We filter out columns that shouldn't be in the update set (like the business key itself)
    source_cols = staged_df.columns
    update_columns = [c for c in source_cols if c != business_key]
    
    # --- 2. INITIAL LOAD HANDLER ---
    if not spark.catalog.tableExists(target_table):
        print(f"Target {target_table} does not exist. Performing initial load...")
        initial_df = staged_df
        
        if scd_type == 2:
            initial_df = initial_df.withColumn("start_at", F.current_timestamp()) \
                                   .withColumn("end_at", F.lit(None).cast("timestamp")) \
                                   .withColumn("is_current", F.lit(True))
        
        initial_df.write.format("delta").saveAsTable(target_table)
        
        # Get metrics from the commit we just made
        dt = DeltaTable.forName(spark, target_table)
        metrics = dt.history(1).collect()[0]["operationMetrics"]
        rows_inserted = int(metrics.get("numOutputRows", initial_df.count()))
        
        return {
            "rows_inserted": rows_inserted, 
            "rows_updated": 0, 
            "total_rows_affected": rows_inserted
        }

    target_delta = DeltaTable.forName(spark, target_table)

    # --- 3. DEFINE MATCH LOGIC ---
    # Null-safe comparison (<=>) prevents NULLs from breaking the logic
    change_condition = " OR ".join([f"target.{c} <=> source.{c} = False" for c in comparison_cols])
    
    # --- 4. EXECUTE MERGE ---
    if scd_type == 1:
        # SCD Type 1: Overwrite on change, Ignore on no change
        (target_delta.alias("target")
            .merge(
                staged_df.alias("source"), 
                f"target.{business_key} = source.{business_key}"
            )
            .whenMatchedUpdate(
                condition = change_condition,
                set = {c: f"source.{c}" for c in update_columns} 
                # This includes bronze_id, run_id, ingest_ts because they are in update_columns
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    elif scd_type == 2:
        # SCD Type 2: History Tracking
        # Step A: Identify rows that need to be expired (Matched + Changed)
        # We perform a join to find records where the key matches but data is different
        rows_to_update = staged_df.alias("source").join(
            target_delta.toDF().alias("target"),
            (F.col(f"source.{business_key}") == F.col(f"target.{business_key}")) &
            (F.col("target.is_current") == True)
        ).filter(F.expr(change_condition)).select(
            "source.*", 
            F.lit(True).alias("needs_update")
        )

        # Step B: Combine "New Rows" (from source) with "Rows to Update" (marked)
        # We explicitly set needs_update=False for the standard source rows
        merge_df = staged_df.withColumn("needs_update", F.lit(False)).unionByName(rows_to_update)

        (target_delta.alias("target")
            .merge(
                merge_df.alias("source"), 
                f"target.{business_key} = source.{business_key} AND target.is_current = true"
            )
            # 1. Update existing current row -> Expire it
            .whenMatchedUpdate(
                condition = "source.needs_update = true",
                set = {
                    "is_current": "false",
                    "end_at": F.current_timestamp()
                    # We DO NOT update bronze_id, run_id, or ingest_ts here.
                    # They retain their original values from when the record was created.
                }
            )
            # 2. Insert new row (either brand new OR the new version of an updated row)
            .whenNotMatchedInsert(
                values = {
                    **{c: f"source.{c}" for c in source_cols},
                    "is_current": "true",
                    "start_at": F.current_timestamp(),
                    "end_at": "null"
                }
            )
            .execute()
        )

    # --- 5. EXTRACT METRICS ---
    history = target_delta.history(1).collect()[0]
    metrics = history["operationMetrics"]
    
    rows_inserted = int(metrics.get("numTargetRowsInserted", 0))
    rows_updated = int(metrics.get("numTargetRowsUpdated", 0))
    
    return {
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "total_rows_affected": rows_inserted + rows_updated
    }


def run_dimension_pipeline(spark, config, run_id, log_fn):
    """
    Orchestrator for Dimension (SCD) Tables.
    
    This function manages the lifecycle of loading a dimension table:
    1. Ingests from Bronze.
    2. Normalizes schema (Rename & Cast).
    3. Executes the SCD Logic (via apply_scd).
    4. Logs the results (Success/Failure) using the unified logger.
    
    Args:
        spark: SparkSession
        config (dict): Configuration containing:
            - dataset_name, log_table
            - bronze_table, silver_table
            - business_key
            - rename_mapping, type_mapping
            - scd_type
            - comparison_columns (optional for SCD1, required for SCD2)
        run_id: Unique identifier for this execution.
        log_fn: The 'log_silver_ingestion' function.
    """
    
    # --- 1. SETUP ---
    start_ts = datetime.datetime.now()
    dataset_name = config["dataset_name"]
    log_table = config.get("log_table", "default_logs")
    
    # Initialize Defaults
    bronze_count = 0
    metrics = {"rows_inserted": 0, "rows_updated": 0, "total_rows_affected": 0}
    success = False
    error_msg = ""
    max_bronze_ts = None
    
    try:
        print(f"[{dataset_name}] Starting Dimension Pipeline...")
        
        # --- 2. INGESTION ---
        bronze_df = spark.read.table(config["bronze_table"])
        bronze_count = bronze_df.count()

        # Capture lineage watermark (if available) for the log
        # We look for standard timestamp columns usually found in Bronze
        max_bronze_ts = bronze_df.agg(F.max('_ingest_ts')).collect()[0][0]
                

        # --- 3. TRANSFORMATION (Rename & Cast) ---
        
        # A. Handle Lineage Column
        # We preemptively rename 'run_id' to 'bronze_id' if it exists to avoid collisions
        # with the new Silver 'run_id' we will generate later.
            
        # B. Apply Configured Renames
        renamed_df = bronze_df.withColumnsRenamed(config["rename_mapping"])
        
        # C. Apply Strict Schema (Casting)
        # We iterate through 'type_mapping' to select only the allowed columns.
        # This acts as both a Caster and a Column Filter.
        select_exprs = []
        for col_name, data_type in config["type_mapping"].items():
            select_exprs.append(F.col(col_name).cast(data_type))
            
        # Note: If 'bronze_id' is needed in Silver, ensure it is in your type_mapping config!
        cast_df = renamed_df.select(select_exprs)

        staged_df = cast_df.withColumn("run_id", F.lit(run_id)) \
                         .withColumn("ingest_ts", F.current_timestamp())
        # --- 4. EXECUTION (Apply SCD) ---
        
        # Determine Comparison Columns
        # If not provided in config for SCD1, we compare ALL columns (except the PK)
        comparison_cols = config.get("comparison_columns")
        if not comparison_cols:
            comparison_cols = [c for c in config["type_mapping"].keys() if c != config["business_key"]]

        metrics = apply_scd(
            spark=spark,
            staged_df=staged_df,
            target_table=config["silver_table"],
            business_key=config["business_key"],
            comparison_cols=comparison_cols,
            scd_type=config.get("scd_type", 1),
        )
        
        success = True
        print(f"[{dataset_name}] Success. Total Rows Affected: {metrics['total_rows_affected']}")

    except Exception as e:
        error_msg = str(e)
        print(f"[{dataset_name}] FAILED: {error_msg}")
        # We do not raise here immediately, so that the finally block runs and logs the error.
        # However, in a real orchestrator (like Airflow), you might want to `raise e` AFTER logging.

    finally:
        # --- 5. LOGGING ---
        end_ts = datetime.datetime.now()
        
        log_fn(
            spark=spark,
            run_id=run_id,
            dataset_name=dataset_name,
            catalog_table=config["silver_table"],
            log_table=log_table,
            start_ts=start_ts,
            end_ts=end_ts,
            max_bronze_ts=max_bronze_ts,
            bronze_count=bronze_count,
            silver_count=metrics["total_rows_affected"],
            quarantine_count=0,      # N/A for dimensions in this pattern
            duplicates_dropped=0,    # N/A for dimensions in this pattern
            success=success,
            error_msg=error_msg
        )
        
        # Re-raise exception if failed, so the job actually fails in Databricks/Spark UI
        if not success:
            raise Exception(error_msg)