# M4 - Databricks Bronze Layer - Parquet Generation, Environment Setup and Upload

> **File:** `/docs/04_databricks_bronze.md`  
> **Module:** M4 - Retail Analytics Platform - Medallion Architecture  
> **Stack:** Postgres (WSL Docker) → Parquet → Databricks Free Edition (dbc-a6423fd6-0d52.cloud.databricks.com) → Delta Bronze  
> **Last Updated:** 2025-10 - Fixed v2 - No Reload Bug

---

## 1. Overview

This document covers the complete workflow to bridge the local transactional database (Postgres running in WSL2 Docker) to the Databricks Bronze layer using Parquet as an intermediate format.

### Medallion Architecture Context

We implement the classic Medallion pattern:

```
Postgres (OLTP) → Parquet Export (Staging) → Databricks Volume → Bronze (Delta, raw + audit) → Silver → Gold
```

**Why Postgres -> Parquet -> Bronze instead of direct JDBC?**

1.  **Network Isolation (Free Edition):** Databricks Free Edition (serverless) runs on `dbc-a6423fd6-0d52.cloud.databricks.com` and **cannot** reach private WSL networks. The Postgres instance at `192.168.182.1:5432` (WSL2 vEthernet adapter IP) is unreachable from Databricks cloud. Direct JDBC ingestion fails with timeout.
2.  **Decoupling & Reproducibility:** Parquet files are compressed columnar, immutable artifacts. We can re-ingest Bronze without hitting prod Postgres.
3.  **Performance:** Exporting 55M invoices via JDBC to Databricks would take hours and stress Postgres. Exporting locally with keyset pagination is 10x faster and resumable.
4.  **Cost:** Free Edition has no DLT pipeline credits for long JDBC jobs. Uploading files to Volumes is free and fast.

### Goal of This Step

- Generate deterministic Parquet exports (01)
- Stage them locally and upload to Volume (02)
- Provision Databricks environment: Catalog, Schema, Volume, Control Log (04 DDL)
- Incremental Bronze load with file tracking - NO RELOAD (05)

---

## 2. Folder Structure - CORRECT ORDER - UPDATED

The numbering in the repo is historical. The logical execution order is:

```bash
/docs/
├── 00_setup.md
├── 01_postgres_infra.md         
├── 02_architecture_decisions.md
├── 02_data_generation.md
├── 03_raw_layer.md       
└── 04_databricks_bronze.md

04_databricks/
├── 01_postgres_to_parquet/
│   ├── 01_export_postgres_to_parquet.py  # M4.1 Export - keyset pagination WHERE id > last_id
│   └── requirements.txt
├── 02_data_export/                     # M4.1b Upload - MUST BE BEFORE BRONZE
│   ├── 02_upload_parquet_to_databricks_volume.py
│   ├── upload_via_cli.sh
│   └── DATABRICKS_UPLOAD_GUIDE.md
└── 03_bronze_ingestion/                # M4.2 Bronze - UPDATED STRUCTURE
    ├── 03_bronze_ingest_from_parquet.sql   # Legacy local Spark
    ├── 04_bronze_ddl.sql               # M4.2a DDL - MANUAL ONCE - .sql recommended
    ├── 05_bronze_loading_process.py    # M4.2b Loading - JOB - Incremental, all tables, NO RELOAD (fixed v2)
    ├── 06_bronze_control_queries.sql   # M4.2c Verification queries
    └── notebook_bronze_ingest_databricks.py

data/
└── parquet_export/               # Gitignored, 53 files
    ├── stores/          -> 1 file, 50 rows
    ├── products/        -> 1 file, 2,500 rows
    ├── customers/       -> 1 file, 100,000 rows
    ├── invoices/        -> 11 files, 55,000,000 rows
    └── invoice_items/   -> 40 files, ~200,000,000 rows
```

**Why this order matters:** `02_data_export` MUST be before `03_bronze_ingestion`. Volume must contain 53 files before Bronze DDL.

---

## 3. Generation of Parquet Files (M4.1)

### 3.1 Why Keyset Pagination, not OFFSET

For 55M+ row tables, `OFFSET` is fatal:

```sql
-- BAD: Postgres still scans and discards OFFSET rows - O(n^2)
SELECT * FROM invoices ORDER BY id OFFSET 50000000 LIMIT 5000000; -- ~45s per batch

-- GOOD: Uses index on PK, constant time per batch - O(n)
SELECT * FROM invoices WHERE id > 50000000 ORDER BY id LIMIT 5000000; -- ~0.8s per batch
```

We implement **keyset pagination** using `WHERE id > last_id`.

### 3.2 Export Configuration

| Parameter | Value | Rationale |
| :--- | :--- | :--- |
| Batch Size | 5,000,000 rows | Balances memory (~1.2GB RAM) vs file count |
| Compression | Snappy | Fast, splittable, Databricks native |
| Format | Parquet v2 | Columnar + predicate pushdown |
| Partitioning | By table + row range | Enables parallel Bronze ingest |
| Engine | pyarrow 14.0.2 | Faster than fastparquet for large ints |

**Final Artifacts:**
- **53 files**
- **1.93 GB compressed** (Snappy)
- **~12.1 GB uncompressed** (estimated from parquet metadata)
- **Row counts verified against Postgres COUNT(*)**

### 3.3 Table Breakdown

| Table | Rows | Files | Compressed | Uncompressed Est. | PK Range |
| :--- | :--- | :--- | :--- | :--- | :--- |
| stores | 50 | 1 | 8.2 KB | 12 KB | 1-50 |
| products | 2,500 | 1 | 142 KB | 380 KB | 1-2500 |
| customers | 100,000 | 1 | 4.8 MB | 18 MB | 1-100k |
| invoices | 55,000,000 | 11 | 720 MB | 4.8 GB | 1-55M |
| invoice_items | ~200,000,000 | 40 | 1.2 GB | 7.2 GB | 1-200M |

### 3.4 Commands to Run

```bash
# Step 1: Smoke test with small tables only
python 04_databricks/01_postgres_to_parquet/01_export_postgres_to_parquet.py --small-only --verify

# Step 2: Full export (takes 18-25 min on i7/32GB/SSD)
python -u 04_databricks/01_postgres_to_parquet/01_export_postgres_to_parquet.py --batch-size 5000000
```

---

## 4. Databricks Environment Setup (M4.2a - DDL)

### 4.1 Unity Catalog Hierarchy - UPDATED

```
Catalog: workspace (Free Edition default)
├── Schema: retail
│   ├── Volume: raw_postgres_export (MANAGED) -> /Volumes/workspace/retail/raw_postgres_export/
│   ├── Table: bronze_file_log (control log - tracks file_name + table_name)
│   ├── Table: bronze_stores (Delta, 50 rows)
│   ├── Table: bronze_products (2,500 rows)
│   ├── Table: bronze_customers (100k rows)
│   ├── Table: bronze_invoices (55M rows)
│   └── Table: bronze_invoice_items (~200M rows)
└── Schema: default
```

### 4.2 DDL - Manual Once - Fixed

**File:** `04_databricks/03_bronze_ingestion/04_bronze_ddl.sql` - **Type: .sql recommended**

Creates:
- Schema `workspace.retail`
- Volume `raw_postgres_export` MANAGED
- Control table `bronze_file_log` (file_name, file_path, table_name, status, row_count, file_size, first_seen_at, last_processed_at, job_run_id)
- 5 Bronze tables with audit columns: `_source_file_name`, `_source_file_path`, `_ingest_timestamp`, `_update_timestamp`, `_bronze_load_id`
- All original columns as STRING permissive to keep dirty data (1% dirty kept for Silver)

**CRITICAL FIX:** Do NOT use `PARTITIONED BY (invoice_date)` in Bronze. This caused `invoices_part_0000` to hang - tries to create 1000 partitions for 1 file = OOM on Free Edition. Partitioning is Silver/Gold optimization, not Bronze.

```sql
-- CORRECT - Bronze DDL - No partitioning
CREATE TABLE IF NOT EXISTS workspace.retail.bronze_invoices (
  invoice_id STRING,
  store_id STRING,
  customer_id STRING,
  invoice_date STRING,
  total_amount STRING,
  discount_amount STRING,
  tax_amount STRING,
  payment_method STRING,
  status STRING,
  created_at STRING,
  _source_file_name STRING,
  _source_file_path STRING,
  _ingest_timestamp TIMESTAMP,
  _update_timestamp TIMESTAMP,
  _bronze_load_id STRING
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- WRONG - Causes hang on Free Edition
-- CREATE TABLE ... PARTITIONED BY (invoice_date) -- DO NOT DO THIS IN BRONZE
```

Alternative Python version: `04_bronze_ddl.py` - same DDL via notebook.

### 4.3 Truncate Utility

```sql
-- File: truncate_bronze.sql
TRUNCATE TABLE workspace.retail.bronze_invoice_items;
TRUNCATE TABLE workspace.retail.bronze_invoices;
TRUNCATE TABLE workspace.retail.bronze_customers;
TRUNCATE TABLE workspace.retail.bronze_products;
TRUNCATE TABLE workspace.retail.bronze_stores;
TRUNCATE TABLE workspace.retail.bronze_file_log;
```

---

## 5. Upload to Volume (M4.1b)

### 5.1 Bugs Found & Fixes

#### Bug 1: Wrong Volume Type Enum

```python
# BAD - from old docs
VolumeType.MANAGED  # AttributeError

# GOOD - Fixed
from databricks.sdk.service.catalog import VolumeType
VolumeType.MANAGED  # Actually VolumeType.MANAGED is correct in newer SDK, but old SDK used different enum
# Solution: Use SQL to create volume: CREATE VOLUME ... MANAGED
```

#### Bug 2: PAT vs OAuth

```bash
export DATABRICKS_TOKEN=dapiXXXXXXXX
databricks fs ls /Volumes/workspace/retail/raw_postgres_export/
# Error: Unauthenticated: auth_type=pat is not supported for this workspace
```

**Free Edition blocks PATs.** Must use OAuth U2M.

Fix:
```bash
unset DATABRICKS_TOKEN
unset DATABRICKS_HOST
databricks auth login --host https://dbc-a6423fd6-0d52.cloud.databricks.com
databricks auth profiles
# DEFAULT https://dbc-a6423fd6-0d52.cloud.databricks.com OAuth
```

#### Bug 3: SSL EOF + Bash Hash + Permission

Three sub-issues during upload:
1.  **SSL EOF:** Intermittent `SSLError: EOF occurred in violation of protocol` on large files. Fixed by retry logic in SDK script.
2.  **Bash cache:** After reinstalling CLI, `bash: .venv/bin/databricks: No such file or directory` because bash hashed old path.
    ```bash
    hash -r
    which databricks # /usr/local/bin/databricks
    ```
3.  **Permission:** `Target directory /usr/local/bin not writable` during install.
    ```bash
    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
    ```

### 5.2 Method A: SDK Script with Retry (Recommended for >1GB)

File: `04_databricks/02_data_export/02_upload_parquet_to_databricks_volume.py`

Run:
```bash
python 04_databricks/02_data_export/02_upload_parquet_to_databricks_volume.py
# ✓ 53 files uploaded, ~8-12 min on 100Mbps
```

### 5.3 Method B: CLI Bulk Copy (Fastest)

```bash
databricks auth login --host https://dbc-a6423fd6-0d52.cloud.databricks.com
databricks fs cp ./data/parquet_export/* /Volumes/workspace/retail/raw_postgres_export/ --overwrite --recursive
```

### 5.4 Validation After Upload

```sql
LIST '/Volumes/workspace/retail/raw_postgres_export/';
-- Should show 53 files or 5 folders depending on structure

SELECT COUNT(*) as file_count FROM (
  LIST '/Volumes/workspace/retail/raw_postgres_export/**'
);
-- Expected: 53
```

---

## 6. Bronze Ingestion - Incremental Load - FIXED v2 (NEW)

### 6.1 Problem with Old Ingestion

Old `03_bronze_ingest_from_parquet.py`:
- Did `count()` before write = double scan (10M rows for invoices_part_0000 = 2x scan = OOM)
- Compared file_path exactly (`/Volumes/...` vs `dbfs:/Volumes/...`) = path format mismatch = invoice_items reloaded even though already LOADED
- Used `PARTITIONED BY (invoice_date)` in DDL = tried to create 1000 partitions for 1 file = hang on Free Edition

### 6.2 Fixed Loader - 05_bronze_loading_process.py

**File:** `04_databricks/03_bronze_ingestion/05_bronze_loading_process.py` - Type: `.py` Databricks Notebook - Called by Jobs

**Requirements Implemented:**
- **Incremental**: file already loaded for a table = SKIP
- **If file loaded on some tables but not on others -> load on missing table** - tracked by `(file_name, table_name)` AND `(file_path, table_name)` robustly
- **As-is**: keep nulls, errors, no transforms, allow duplicates
- **One script for all tables**: maps `stores.parquet` -> `bronze_stores`, `products.parquet` -> `bronze_products`, `customers.parquet` -> `bronze_customers`, `invoices_part_*.parquet` -> `bronze_invoices`, `invoice_items*.parquet` -> `bronze_invoice_items`
- **Optimized**: No `count()` before write - single scan. Row count logged as -1 for speed
- **Metadata**: adds `_source_file_name`, `_source_file_path`, `_ingest_timestamp`, `_update_timestamp`, `_bronze_load_id`

**Key Fix Code:**
```python
def get_loaded_set():
    rows = spark.sql(f"SELECT file_name, file_path, table_name FROM {CONTROL_TABLE} WHERE status = 'LOADED'").collect()
    by_name = set((r.file_name, r.table_name) for r in rows)  # Robust: file name + table
    by_path = set((r.file_path.rstrip("/").lower(), r.table_name) for r in rows)
    return by_name, by_path

def is_already_loaded(file_path, table_name, by_name_set, by_path_set):
    file_name = file_path.split("/")[-1]
    if (file_name, table_name) in by_name_set:  # Catches invoice_items even if path format changed
        return True
    if (file_path.rstrip("/").lower(), table_name) in by_path_set:
        return True
    return False
```

**Job Parameters:**
```
volume_path = /Volumes/workspace/retail/raw_postgres_export/
job_run_id = {{job.run_id}}
force_reload = false
```

**Expected Run:**
```
Found 53 files
Control log has 2 LOADED entries (customers, invoice_items)
⏭️ SKIP - Already LOADED: customers.parquet -> bronze_customers
⏭️ SKIP - Already LOADED: invoice_items.parquet -> bronze_invoice_items
📥 Loading: invoices_part_0000.parquet -> bronze_invoices (5M rows, ~2-3 min)
✅ LOADED invoices_part_0000.parquet -> bronze_invoices in 142.3s
Done - New: 11, Skipped: 42, Failed: 0
```

### 6.3 Control Queries

**File:** `06_bronze_control_queries.sql`

```sql
-- File vs log check
SELECT file_name, table_name, status FROM workspace.retail.bronze_file_log WHERE status = 'LOADED';

-- Row counts vs expected
SELECT 'bronze_stores' as tbl, COUNT(*) FROM workspace.retail.bronze_stores -- 50
UNION ALL SELECT 'bronze_products', COUNT(*) FROM workspace.retail.bronze_products -- 2500
UNION ALL SELECT 'bronze_customers', COUNT(*) FROM workspace.retail.bronze_customers -- 100k
UNION ALL SELECT 'bronze_invoices', COUNT(*) FROM workspace.retail.bronze_invoices -- 55M
UNION ALL SELECT 'bronze_invoice_items', COUNT(*) FROM workspace.retail.bronze_invoice_items; -- ~200M

-- Metadata not null
SELECT COUNT(*) FROM workspace.retail.bronze_invoices WHERE _source_file_name IS NULL; -- 0 expected

-- Check no reload
SELECT file_name, table_name, COUNT(*) as cnt FROM workspace.retail.bronze_file_log WHERE status='LOADED' GROUP BY file_name, table_name HAVING cnt > 1; -- 0 expected
```

### 6.4 Single File Fast Path (if invoices_part_0000 hangs)

If loader still hangs, use COPY INTO for single file - bypass driver:

```sql
COPY INTO workspace.retail.bronze_invoices
FROM (
  SELECT *,
    _metadata.file_name as _source_file_name,
    _metadata.file_path as _source_file_path,
    current_timestamp() as _ingest_timestamp,
    current_timestamp() as _update_timestamp,
    'fix_invoices' as _bronze_load_id
  FROM '/Volumes/workspace/retail/raw_postgres_export/invoices_part_0000.parquet'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema'='true');
```

---

## 7. Validation Checklist - UPDATED

Before marking M4 complete, verify:

- [ ] `./data/parquet_export/` contains 53 files, 1.93GB total
- [ ] Row counts match Postgres COUNT(*) (stores 50, products 2.5k, customers 100k, invoices 55M, invoice_items ~200M)
- [ ] New CLI installed: `databricks --version` >= 0.213.0
- [ ] Auth is OAuth, not PAT: `databricks auth profiles` shows OAuth
- [ ] No `DATABRICKS_TOKEN` env var set: `env | grep DATABRICKS` empty
- [ ] Volume exists: `LIST '/Volumes/workspace/retail/raw_postgres_export/'` returns files
- [ ] Volume type is MANAGED
- [ ] Upload succeeded: 53 files in Volume
- [ ] DDL executed: `04_bronze_ddl.sql` created 5 bronze tables + bronze_file_log, NO PARTITIONED BY
- [ ] Bronze loader is FIXED v2: checks by file_name+table_name, not just file_path
- [ ] Incremental check: Re-running `05_bronze_loading_process.py` skips already loaded files - no reload of invoice_items
- [ ] Spark can read: `spark.read.parquet("/Volumes/workspace/retail/raw_postgres_export/invoices/").count() == 55M`
- [ ] Control queries pass: row counts match expected, no duplicates in file_log
- [ ] Documentation updated: this file committed to `/docs/04_databricks_bronze.md`

**If any check fails:**
- PAT still set -> `unset DATABRICKS_TOKEN`
- invoices_part_0000 hangs -> Drop and recreate bronze_invoices WITHOUT PARTITIONED BY, then use COPY INTO
- invoice_items reloads -> Use fixed v2 loader that checks by file_name + table_name

---

*Author: Data Engineering Portfolio - M4 Medallion Project*  
*Workspace: dbc-a6423fd6-0d52.cloud.databricks.com (Free Edition)*  
*Date: 2025-10 - Fixed v2*
