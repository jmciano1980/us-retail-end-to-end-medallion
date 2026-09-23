# M4 - Databricks Bronze/Silver/Gold

## Current Order
```
04_databricks/
├── 01_postgres_to_parquet/      # 01_export_postgres_to_parquet.py - Export Postgres -> Parquet (53 files / 1.93GB)
├── 02_data_export/              # 02_upload_parquet_to_databricks_volume.py - Upload to Volume /Volumes/workspace/retail/raw_postgres_export/
├── 03_bronze_ingestion/
│   ├── 03_bronze_ingest_from_parquet.py  # Legacy - local Spark version
│   ├── 04_bronze_ddl.sql        # MANUAL - Once - Creates schema, volume, bronze_file_log + 5 bronze tables (NEW)
│   ├── 05_bronze_loading_process.py  # JOB - Incremental loader with file tracking (NEW)
│   ├── 06_bronze_control_queries.sql  # Verification queries (NEW)
│   └── notebook_bronze_ingest_databricks.py
└── README.md
```

Order: 01 -> 02 -> 03. 02_data_export is BEFORE 03_bronze_ingestion.

## M4.1 - Export (01)
`python 04_databricks/01_postgres_to_parquet/01_export_postgres_to_parquet.py --batch-size 5000000`

## M4.1b - Upload (02)
`databricks auth login --host https://dbc-a6423fd6-0d52.cloud.databricks.com`
`python 04_databricks/02_data_export/02_upload_parquet_to_databricks_volume.py`

## M4.2 - Bronze (03)

### Step 1 - DDL Manual Once
In Databricks, create notebook from `04_bronze_ddl.sql` and run once.
Creates:
- workspace.retail schema
- raw_postgres_export volume
- bronze_file_log control table
- bronze_stores, bronze_products, bronze_customers, bronze_invoices, bronze_invoice_items with metadata _source_file_name, _ingest_timestamp, _update_timestamp, _bronze_load_id

Type: .sql for DDL is recommended - pure SQL, idempotent with IF NOT EXISTS. .py alternative provided for Python notebook users.

### Step 2 - Loading Process via Jobs
`05_bronze_loading_process.py` is a Databricks Job notebook:
- Lists files in volume
- Checks bronze_file_log for already LOADED files -> skip
- Loads new files as-is (keeps nulls, no transforms, allows duplicates)
- Adds metadata columns
- Logs success/failure to control table

Set as Databricks Job with parameters:
volume_path=/Volumes/workspace/retail/raw_postgres_export/
job_run_id={{job.run_id}}

### Step 3 - Control Queries
Run `06_bronze_control_queries.sql` to verify counts, metadata, duplicates, incremental behavior.

## Validation Checklist
- [ ] 53 parquet files in volume
- [ ] 5 bronze tables created with metadata columns
- [ ] bronze_file_log shows LOADED files
- [ ] Re-running loader skips already loaded files
- [ ] Control queries show expected counts
