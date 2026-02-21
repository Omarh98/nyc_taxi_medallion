# NYC Taxi Medallion Architecture Lakehouse
<br>

## 1. Project Overview
- **Goal:** Build a metadata-driven data processing framework on the medallion architecture
- **Datasets:** Yellow Taxi, Green Taxi, FHV Trips, HVFHV Trips (Parquet format), NYC Taxi Zones (CSV), Licences
- **Tech Stack:** Databricks,Spark, Delta Lake, S3 (external location), Unity Catalog, AWS Glue / Athena
- **Pipeline Type:** Batch processing / ETL
<br><br>
---

## 2. Architecture
- **Medallion Layers:**
  - **Bronze:** Raw ingestion of source files,Autoloader,Schema evolution handling
  - **Silver:** Cleaning, deduplication, enrichment, Data validation and isolation framework
  - **Gold:** Aggregations, ready-to-query tables for analysis
- **Catalog / Schema Structure:**
  - Catalog: `nyc_taxi_catalog`
  - Schemas: `bronze`, `silver`, `gold`,`quarantine`
- **External Storage:** S3 bucket `s3://nyc-tlc-data-398563707364/` mapped via Unity Catalog external location
- **Diagram:** `diagrams/medallion_pipeline.png`
<br>
<br>
---

## 3. Folder Structure
nyc_taxi_medallion/<br>
├─ notebooks/<br>
├─ notes/<br>
├─ diagrams/<br>
├─ queries/<br>
├─ configs/
<br>
<br>
---

## 4. Medallion Layer Details

###🥉 Bronze Layer: Raw Data Ingestion
  **Objective:** To create an immutable, append-only landing zone that mirrors the source system with high fidelity  while injecting technical metadata for lineage.

  **Key Engineering Patterns:**
 - **Incremental Discovery (Autoloader):** Uses Databricks cloudFiles to detect new files in S3/ADLS without expensive directory listing. It maintains state via checkpointLocation, ensuring that if the job fails, it resumes exactly where it left off.

- **Schema Evolution:** Configured with `cloudFiles.schemaLocation` and `mergeSchema = True`. This allows the pipeline to adapt to upstream changes (like a new column in the Yellow Taxi parquet files) without manual intervention.

- **Technical Lineage Injection:**  Every record is enriched with:

  - `_ingest_ts:` The precise moment of ingestion.

  - `_source_file:` The full path to the raw file, enabling "Root Cause Analysis" for data quality issues.

  - `run_id:` A UUID linking the record to a specific orchestration instance in the logging table.

 - **Dynamic Pruning:** High-volume taxi data is partitioned by year and month extracted via RegEx from the filenames to optimize downstream predicate pushdown.

###🥈 Silver Layer: The Conformance & Validation Engine
  **Objective:** Transform untrusted raw data into Conformed assets. This is the Workhorse of the framework, where schema contracts and quality rules are enforced through a modular ETL Engine.

1. **Incremental State Management**

    To ensure scalability, the engine calculates a High-Water Mark. It identifies the `max(ingest_ts)` currently in the Silver table and uses this as a predicate when reading from Bronze. This ensures we only process delta data, avoiding redundant compute.

2. **The Data Contract (Conformance)**

    The engine applies a strict schema contract defined in the configuration for metadata-driven transformations

    - **Renaming:** Normalizes inconsistent naming conventions (e.g., `Vendor_ID` vs `vendor_id`).

    - **Type Casting:** Enforces strict typing (e.g., ensuring `passenger_count` is an integer and fare is a decimal).

    - **Functional Injection:** The engine supports custom transformation functions `(transform_func)` passed via config, allowing for complex derived columns (like trip duration or borough enrichment) to be injected without modifying the core engine code.

3. **The Quality Gate (Quarantine)**

    Rather than failing the job on a data anomaly, the engine uses a non-blocking split:

     - **Logic:** A boolean `is_valid` flag is calculated based on a list of expectations (e.g., total_amount >= 0).

    - **Distribution:** Valid records are routed to the Silver table. Invalid records are diverted to a quarantine table with a column documenting the specific rule violation. This ensures the production environment remains clean while giving engineers a "Dead Letter Queue" to debug.

4. **Idempotency via Deterministic Hashing**

    To handle duplicate raw files, the engine generates a `trip_id` using a cryptographic hash (SHA-256) of the composite business key. This allows the use of Delta MERGE (Upsert) logic: if a record with the same ID arrives again, it is updated rather than duplicated.

5. **State Handling (SCD Type 1 & 2)**

    For lookup and master data, the engine supports:

    - **SCD Type 1:** Overwriting values to reflect the current state (e.g., Vendor Name changes).

    - **SCD Type 2:** Tracking historical changes using `is_current`, `start_at`, and `end_at` flags. This preserves the history of a record, which is vital for accurate historical reporting.

6. **Atomic Resiliency (Rollback)**

    In the event of a transformation failure, the engine utilizes the Delta History API. It identifies the table version prior to the start of the transaction and executes a RESTORE TABLE command. This ensures the Silver layer never exists in a partially updated or corrupt state.

###🥇 Gold Layer: The Semantic Serving Layer
  **Objective:** Provide highly optimized, business-ready views. The Gold layer is designed for "Read-Heavy" performance and semantic clarity.

**Technical Implementation:**

- **Schema Unification:** Merges disparate Silver Fact tables (Yellow, Green, FHV, HVFHV) into a single "Master Fact" table. It uses unionByName to handle missing or extra columns across different taxi providers.

- **Denormalization (Late Binding):** To make the data "Self-Service Ready," the engine joins the Silver Facts with Silver Dimensions. It uses Broadcast Joins to append human-readable strings (e.g., "Manhattan", "JFK Airport") to the Fact records.

- **Aggregation Grain:** Shifts the grain from "Trip-Level" to "Daily-Level." This pre-calculation reduces the row count by orders of magnitude, allowing BI tools to aggregate metrics (Total Revenue, Avg Distance) in milliseconds.

- **Optimization:** Tables are optimized via Liquid Clustering or Z-Ordering on time-series and location columns to ensure high-performance filtering for end-users.s
<br><br>
---

##4. The Metadata Framework: The Data Contract Engine

 The platform’s intelligence is centered around a Metadata-Driven Processing Engine. Rather than maintaining unique, hard-coded ETL scripts for every taxi dataset, the Spark scripts function as generic executors that interpret a Data Contract. This centralized configuration defines the unique business logic, schema requirements, and quality rules for each dataset, allowing the same codebase to process high-volume trip telemetry (Yellow Taxi) and reference data (Taxi Zones) with identical reliability.

**A. Dynamic Schema Enforcement**

 The engine uses a mapping contract to bridge the gap between inconsistent Bronze sources and standardized Silver tables.

 - **Renaming Logic:** The engine iterates through a rename_mapping dictionary to normalize inconsistent source column names, such as standardizing `tpep_pickup_datetime` to `pickup_datetime`.

- **Strict Type Casting**: A `type_mapping` defines the required Spark data types for every field. The engine dynamically applies `cast()` operations to every column during the selection process to ensure the Silver layer adheres to a rigid schema contract.

- **Column Filtering:** By iterating through the `type_mapping`, the engine selects only the documented columns, effectively acting as a schema enforcer that drops any unexpected or undocumented fields from the Bronze layer.

**B. Functional Injection**

For transformations that require more than simple renames, the framework utilizes Functional Injection.

- **Custom Derivations:** Specific business logic, such as calculating `trip_duration` or `driver_pay`, is defined as standalone Python functions.

- **Runtime Application:** These functions are passed into the engine via the `derived_transformations` config key. The engine loops through these functions and applies them to the DataFrame before the final write, allowing for dataset-specific logic within a generic pipeline.

**C. Validation & Quality Rules**

Data quality rules are stored as a list of boolean Spark expressions within the contract.

- **Declarative Quality:** Rules such as `passenger_count.between(1,6)` or `trip_distance >= 0` are defined directly in the configuration.

- **Rule Compilation:** The engine compiles these rules into an array of violation reasons. This metadata determines if a record is promoted to the Silver tier or isolated in the Quarantine layer.

**D. Logic Control & State Management**

The contract also dictates how the engine manages state and table updates:


- **Execution Mode**: Specifies the write strategy, such as standard Append/Merge for facts or SCD Type 1/2 for dimensions.

- **Watermarking:** Defines `lookback_minutes` to control how the incremental loader fetches data from the Bronze layer relative to the last successful run.
<br><br>
---

##5. Unified Observability & Audit Logging

  The platform features a custom-built logging framework designed to provide total transparency into pipeline execution. Rather than relying on ephemeral stdout logs, the `logger.py` module converts every orchestration run into structured, queryable data.

**A. Centralized Audit Telemetry**

  Every run—whether at the Bronze ingestion layer or the Silver conformance layer—is captured in the monitoring logs tables. This provides a single source of truth for:

- **Pipeline Health:** Tracks Success vs Failure status with the capture of full `error_msg` strings for rapid debugging.

- **Performance Benchmarking:** Records `start_ts`, `end_ts`, and calculates duration (in seconds) to identify latency spikes or resource bottlenecks.

- **Data Lineage:** Links every execution to a unique `run_id` (UUID), allowing for absolute auditability of data as it flows through the Medallion layers.

**B. High-Water Mark & State Management**
A critical function of the logging framework is managing the Incremental Watermark.

- State Capture: The `log_silver_ingestion` function persists the `max_bronze_ts`—the most recent timestamp successfully processed from the source table.

- **Efficient Resumption:** During subsequent runs, the `run_silver_pipeline` engine queries this log to identify the starting point for the next incremental batch, ensuring no data is skipped or double-processed.

**C. Data Volume & Quality Monitoring**

The framework monitors data movement and quality in real-time by capturing granular row counts:

- **Bronze Ingestion Counts:** Tracks the total volume of raw data discovered by the Autoloader.
- **Silver Conformance Counts:** Tracks the number of records successfully written to the target Delta table after deduplication and cleaning.
- **Quarantine Metrics:** Specifically captures the `quarantine_count` to monitor the volume of dirty data arriving from upstream providers.
- **Deduplication Efficiency:** Records `duplicates_dropped` by comparing row counts before and after the deterministic hashing and deduplication phase.

**D. Operational Resiliency**

The logging logic is wrapped in try-finally blocks within the processing engines. This ensures that even if a Spark job crashes, the failure is recorded, the timestamps are captured, and the error_msg is persisted, allowing for automated alerting and remediation.
<br><br>
---
##6. Data Validation & Quality Framework (The Quarantine Pattern)

The platform implements a Non-Blocking Quality Gate. Unlike traditional pipelines that fail entirely when a data error is encountered, this engine uses a Quarantine Pattern to isolate "dirty" data while allowing valid records to proceed to production.

**A. Declarative Expectation Suite**

Data quality rules are not hard-coded into the Spark logic. Instead, they are defined as a list of Boolean Spark Expressions within the dataset's configuration.

- **Schema Validation:** Checks for nulls in critical business keys (e.g., `vendor_id.isNull()`).
- **Domain Logic:** Enforces realistic boundaries for taxi operations (e.g., `passenger_count.between(1, 6)` or `trip_distance >= 0`).
- **Financial Integrity:** Validates that monetary values are within expected ranges (e.g., `airport_fee.isin(0, 1.75)`).

**B. The Evaluation Engine**

During the Silver transformation, the engine dynamically evaluates the expectation suite against every row:

- **Rule Compilation:** The engine loops through the validation_rules in the contract and creates an array of strings representing any failed conditions.
- **The quarantine_reasons Column:** A derived column is injected into the DataFrame. If a row fails a rule, the specific rule name (e.g., `invalid_trip_distance`) is appended to this array.

- **Logical Split:**
  - **Valid Stream:** Records where the quarantine_reasons array is empty are promoted to the Silver table.
  - **Quarantined Stream:** Records with one or more reasons are diverted to the Quarantine table.

**C. Atomic Multi-Target Writes**

To ensure data consistency, the engine manages writes to both the Silver and Quarantine tables within the same execution run.

- **Silver Write:** Uses a Delta MERGE (Upsert) based on the deterministic `trip_id` to prevent duplicates.

- **Quarantine Write:** Uses an Append mode to act as a permanent Dead Letter Queue for the engineering team to audit.

**D. Operational Benefits**
- **Zero Pipeline Downtime:** Production BI dashboards are updated daily with "mostly-good" data, rather than being empty due to a single malformed row.

- **Root Cause Analysis:** The `quarantine_reasons` column provides immediate insight into why data was rejected, allowing for faster communication with upstream data providers (e.g., the TLC).

- **Observability:** The `logger.py` module explicitly captures the quarantine_count for every run, enabling automated alerts if the "dirty data" percentage exceeds a specific threshold (e.g., >5%).
<br><br>
---
## 7. Queries
- Creation scripts for the bronze, silver and gold tables
- Queries to explore Gold tables
- Stored in `queries/` folder
- Example: Average trip amount per month by taxi type

---

## 7. Challenges & Lessons Learned
- CE cluster limitations (RAM, cores, storage)
- IAM / external location setup
- Data quirks (missing values, inconsistent formats)
- Schema evolution handling

---

## 8. Future Improvements
- Streaming pipeline support
- Integration with Snowflake / Redshift for CDW
- Automated notifications on validation failures
- More extensive metrics / dashboards
