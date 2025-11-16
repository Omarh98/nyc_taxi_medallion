# NYC Taxi Trip Medallion Pipeline

## 1. Project Overview
- **Goal:** Build a Bronze → Silver → Gold Delta Lake pipeline for NYC taxi trip datasets.
- **Datasets:** Yellow Taxi, Green Taxi, FHV Trips, HVFHV Trips (Parquet format)
- **Tech Stack:** Databricks CE, Delta Lake, S3 (external location), Unity Catalog, Spark, AWS Glue / Athena
- **Pipeline Type:** Batch processing / ETL

---

## 2. Architecture
- **Medallion Layers:**
  - **Bronze:** Raw ingestion of parquet files
  - **Silver:** Cleaning, deduplication, enrichment, data quality checks
  - **Gold:** Aggregations, ready-to-query tables for analysis
- **Catalog / Schema Structure:**
  - Catalog: `nyc_taxi_catalog`
  - Schemas: `bronze`, `silver`, `gold`
- **External Storage:** S3 bucket `s3://nyc-tlc-data-398563707364/` mapped via Unity Catalog external location
- **Diagram:** `diagrams/medallion_pipeline.png`

---

## 3. Folder Structure
nyc_taxi_medallion/
├─ notebooks/
├─ logs/
├─ diagrams/
├─ queries/
├─ configs/
└─ data/


---

## 4. Medallion Layer Details

### Bronze Layer
- **Purpose:** Store raw parquet files in Delta format
- **Process:**
  - Load from S3 landing folder
  - Minimal transformations (parse dates, standardize columns)
- **Notes / Logs:** Track row counts, file ingestion, schema issues

### Silver Layer
- **Purpose:** Cleaned, enriched, and validated data
- **Process:**
  - Remove duplicates, handle nulls
  - Apply enrichment (calculated columns, joins if any)
  - Expectations / quality checks
- **Notes / Logs:** Track rows filtered, failed validations

### Gold Layer
- **Purpose:** Aggregated, analytics-ready data
- **Process:**
  - Aggregate metrics per month / year / taxi type
  - Prepare tables for queries or dashboards
- **Notes / Logs:** Track output row counts and aggregation metrics

---

## 5. Pipeline Logging
- **Logging Table:** `nyc_taxi_catalog.monitoring.pipeline_log`
- **Fields:** layer, table_name, run_timestamp, input_rows, output_rows, nulls_count, duplicates_count, validation_failures, notes
- **Purpose:** Track pipeline runs, data quality, and schema changes

---

## 6. Sample Queries
- Queries to explore Gold tables
- Stored in `sample_queries/` folder
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
