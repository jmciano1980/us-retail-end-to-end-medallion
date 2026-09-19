# M4 - Databricks Bronze Layer - Parquet Generation, Environment Setup and Upload

> **File:** `/docs/04_databricks_bronze.md`  
> **Module:** M4 - Retail Analytics Platform - Medallion Architecture  
> **Stack:** Postgres (WSL Docker) → Parquet → Databricks Free Edition (dbc-a6423fd6-0d52.cloud.databricks.com) → Delta Bronze

---

## 1. Overview

This document covers the complete workflow to bridge the local transactional database (Postgres running in WSL2 Docker) to the Databricks Bronze layer using Parquet as an intermediate format.

### Medallion Architecture Context

We implement the classic Medallion pattern:

```
Postgres (OLTP) → Parquet Export (Staging) → Databricks Volume → Bronze (Delta, raw + audit) → Silver → Gold
```

**Why Postgres -> Parquet -> Bronze instead of direct JDBC?**

1.  **Network Isolation (Free Edition):** Databricks Free Edition (serverless) runs on ``dbc-a6423fd6-0d52.cloud.databricks.com`` and **cannot** reach private WSL networks. The Postgres instance at `192.168.182.1:5432` (WSL2 vEthernet adapter IP) is unreachable from Databricks cloud. Direct JDBC ingestion fails with timeout.
2.  **Decoupling & Reproducibility:** Parquet files are compressed columnar, immutable artifacts. We can re-ingest Bronze without hitting prod Postgres.
3.  **Performance:** Exporting 55M invoices via JDBC to Databricks would take hours and stress Postgres. Exporting locally with keyset pagination is 10x faster and resumable.
4.  **Cost:** Free Edition has no DLT pipeline credits for long JDBC jobs. Uploading files to Volumes is free and fast.

### Goal of This Step

- Generate deterministic Parquet exports (01)
- Stage them locally (03)
- Provision Databricks environment: Catalog, Schema, Volume (02 setup)
- Upload to `/Volumes/workspace/retail/raw_postgres_export/` reliably

---

## 2. Folder Structure - Critical Order


The numbering in the repo is historical. The logical execution order is:

```bash
/docs/
├── 01_postgres_to_parquet.md         # Design of export script
├── 02_data_export.md                 # M4.1 - Actual export run (53 files, 1.93GB)
├── 03_bronze_ingestion.md            # Depends on 02 output existing
└── 04_databricks_bronze.md           # THIS FILE - Setup + Upload (depends on 03)

src/
├── 01_postgres_to_parquet/
│   └── 01_export_postgres_to_parquet.py          # Keyset pagination, Snappy compression
├── 02_data_export/
│   └── 02_upload_parquet_to_databricks_volume.py
├── 03_bronze_ingestion/
│   ├── 03_bronze_ingest_from_parquet.py            # VolumeType.MANAGED fix
│   ├── notebook_bronze_ingest_databricks.py
└── data/
    └── parquet_export/               # Gitignored, 53 files
        ├── stores/          -> 1 file, 50 rows
        ├── products/        -> 1 file, 2,500 rows
        ├── customers/       -> 1 file, 100,000 rows
        ├── invoices/        -> 11 files, 55,000,000 rows
        └── invoice_items/   -> 40 files, ~200,000,000 rows
```

**Why this order matters:** `03_bronze_ingestion` validates existence of `./data/parquet_export` and lists Unity Catalog volume. 

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
python src/01_postgres_to_parquet/01_export_postgres_to_parquet.py --small-only --verify

# Expected: 3 files, <5MB, exit code 0

# Step 2: Full export (takes 18-25 min on i7/32GB/SSD)
python src/01_postgres_to_parquet/01_export_postgres_to_parquet.py --full --batch-size 5000000

# Step 3: Verification
du -sh ./data/parquet_export/*
# stores      8.0K
# products    144K
# customers   4.8M
# invoices    721M (11 files)
# invoice_items 1.2G (40 files)

ls -1 ./data/parquet_export/**/*.parquet | wc -l
# 53

# Postgres checkpoint - row counts must match
psql -h 192.168.182.1 -U retail_user -d retail_db -c "
SELECT 'stores' as tbl, COUNT(*) FROM stores
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'customers', COUNT(*) FROM customers
UNION ALL SELECT 'invoices', COUNT(*) FROM invoices
UNION ALL SELECT 'invoice_items', COUNT(*) FROM invoice_items;
"
```

### 3.5 Python Export Snippet (Key Logic)

```python
def export_table_keyset(conn, table: str, batch_size: int = 5_000_000):
    last_id = 0
    file_idx = 0
    while True:
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE id > %(last_id)s ORDER BY id LIMIT %(limit)s",
            conn, params={"last_id": last_id, "limit": batch_size}
        )
        if df.empty:
            break
        last_id = int(df["id"].max())
        pq.write_table(
            pa.Table.from_pandas(df),
            f"./data/parquet_export/{table}/{table}_{file_idx:03d}.parquet",
            compression="snappy"
        )
        file_idx += 1
```

---

## 4. Databricks Environment Setup and Structure

### 4.1 Why Free Edition Needs New CLI

Host: `https://dbc-a6423fd6-0d52.cloud.databricks.com`

**Old CLI (pip install databricks-cli 0.17.x):**
- Uses PAT-based auth only
- No `auth` command: `databricks auth login` -> `Error: No such command 'auth'`
- `fs cp` uses legacy `dbfs:/` API, not Unity Catalog Volumes

**New CLI (v0.213.0+):**
- Go-based, installed via curl script
- Supports OAuth U2M (user-to-machine) browser flow
- Supports Volumes: `databricks fs cp` -> maps to `/Volumes/...`
- Auth profiles stored in `~/.databrickscfg` + `~/.config/databricks/auth.json`

```bash
# Install new CLI (DO NOT use pip)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

databricks --version
# Databricks CLI v0.218.0
```

### 4.2 Unity Catalog Structure

```
Unity Catalog Hierarchy:
├── Catalog: workspace (default catalog for Free Edition)
│   ├── Schema: retail
│   │   ├── Volume: raw_postgres_export (MANAGED) -> /Volumes/workspace/retail/raw_postgres_export/
│   │   ├── Table: bronze_stores (Delta)
│   │   ├── Table: bronze_products
│   │   ├── Table: bronze_customers
│   │   ├── Table: bronze_invoices
│   │   └── Table: bronze_invoice_items
│   └── Schema: default
```

### 4.3 Volume Creation - Critical Fix

**Bug:** SDK `<1.20` expects enum, not string.

```python
# ❌ WRONG - Fails with ValidationError: 'MANAGED' is not a valid VolumeType
from databricks.sdk.service.catalog import VolumeType
w.volumes.create(catalog_name="workspace", schema_name="retail",
                 name="raw_postgres_export", volume_type="MANAGED")

# ✅ CORRECT - Use enum
from databricks.sdk.service.catalog import VolumeType
w.volumes.create(
    catalog_name="workspace",
    schema_name="retail",
    name="raw_postgres_export",
    volume_type=VolumeType.MANAGED,
    comment="Raw Postgres Parquet export - 53 files, 1.93GB"
)
```

**SQL equivalent (works in Databricks SQL editor):**

```sql
CREATE VOLUME IF NOT EXISTS workspace.retail.raw_postgres_export
COMMENT 'Raw Postgres export staging';
```

**Volume Path (absolute):**
```
/Volumes/workspace/retail/raw_postgres_export/
```

List to confirm:

```sql
LIST '/Volumes/workspace/retail/raw_postgres_export/';
```

---

## 5. Uploading Parquet Files into Databricks Container

### 5.1 The 3 Bugs Encountered (and Fixes)

#### Bug 1: Old CLI - `databricks auth` missing

```bash
databricks auth login --host https://dbc-a6423fd6-0d52.cloud.databricks.com
# Error: No such command 'auth'

# Root cause: pip databricks-cli 0.17.x is deprecated
pip uninstall databricks-cli -y
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
```

#### Bug 2: PAT Authentication Blocked on Free Edition

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
# Opens browser -> Login -> OAuth code -> Stores in ~/.databrickscfg

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

File: `src/03_bronze_ingestion/03_bronze_ingest_from_parquet.py`

```python
from databricks.sdk import WorkspaceClient
import os, time
from pathlib import Path

w = WorkspaceClient(host="https://dbc-a6423fd6-0d52.cloud.databricks.com", auth_type="oauth")

SRC = Path("./data/parquet_export")
VOLUME = "/Volumes/workspace/retail/raw_postgres_export"

def upload_with_retry(local_path: Path, remote_path: str, max_retries=3):
    for attempt in range(max_retries):
        try:
            with open(local_path, "rb") as f:
                w.files.upload(remote_path, f, overwrite=True)
            print(f"✓ {local_path.name} -> {remote_path}")
            return True
        except Exception as e:
            print(f"Retry {attempt+1}/{max_retries} for {local_path.name}: {e}")
            time.sleep(2 ** attempt)
    return False

for parquet_file in SRC.rglob("*.parquet"):
    relative = parquet_file.relative_to(SRC)
    remote = f"{VOLUME}/{relative}"
    upload_with_retry(parquet_file, remote)
```

Run:

```bash
python src/03_bronze_ingestion/03_bronze_ingest_from_parquet.py
# ✓ 53 files uploaded
# Time: ~8-12 min on 100Mbps uplink
```

### 5.3 Method B: CLI Bulk Copy (Fastest)

```bash
# Ensure OAuth login done
databricks auth login --host https://dbc-a6423fd6-0d52.cloud.databricks.com

# Bulk recursive copy
databricks fs cp ./data/parquet_export/* /Volumes/workspace/retail/raw_postgres_export/ --overwrite --recursive

# With verbose
databricks fs cp ./data/parquet_export/ /Volumes/workspace/retail/raw_postgres_export/ --recursive --overwrite -v
```

**Upload Time Estimates:**

| Connection | Size | Time | Method |
| :--- | :--- | :--- | :--- |
| 50 Mbps | 1.93 GB | 12-15 min | CLI |
| 100 Mbps | 1.93 GB | 7-10 min | CLI |
| 200 Mbps | 1.93 GB | 4-6 min | CLI |
| Any | 1.93 GB | 8-12 min | SDK + retry (slower but resilient) |

### 5.4 Validation After Upload

**1. LIST volume (SQL):**

```sql
LIST '/Volumes/workspace/retail/raw_postgres_export/';
LIST '/Volumes/workspace/retail/raw_postgres_export/invoices/';
-- Should show 11 files

SELECT COUNT(*) as file_count FROM (
  LIST '/Volumes/workspace/retail/raw_postgres_export/**'
);
-- Expected: 53
```

**2. Read test with Spark:**

```python
df = spark.read.format("parquet").load("/Volumes/workspace/retail/raw_postgres_export/invoices/")
print(f"Rows: {df.count():,}")  # 55,000,000
df.printSchema()

# All tables
for tbl in ["stores", "products", "customers", "invoices", "invoice_items"]:
    cnt = spark.read.parquet(f"/Volumes/workspace/retail/raw_postgres_export/{tbl}/").count()
    print(f"{tbl}: {cnt:,}")
```

**3. Python SDK validation:**

```python
files = list(w.files.list_directory_contents("/Volumes/workspace/retail/raw_postgres_export/"))
assert len(files) == 5 # 5 folders
```

---

## 6. Next Steps - Bronze Ingestion

Once Parquet files are in the Volume:

1.  **Create Bronze Delta tables** with audit columns:
    ```sql
    CREATE TABLE IF NOT EXISTS workspace.retail.bronze_invoices
    USING DELTA
    AS SELECT *, current_timestamp() as _ingest_timestamp, input_file_name() as _source_file
       FROM parquet.`/Volumes/workspace/retail/raw_postgres_export/invoices/`;
    ```

2.  **Auto Loader alternative** (for incremental):
    ```python
    (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .load("/Volumes/workspace/retail/raw_postgres_export/invoices/")
      .writeStream.format("delta")
      .option("checkpointLocation", "/Volumes/workspace/retail/_checkpoints/bronze_invoices/")
      .start("workspace.retail.bronze_invoices")
    )
    ```

3.  **Data quality checks:** Row counts match Postgres, no null PKs, file mod time < 24h.

See `02_bronze_ingestion.md` for full Bronze DDL.

---

## 7. Validation Checklist

Before marking M4 complete, verify:

- [ ] `./data/parquet_export/` contains 53 files, 1.93GB total
- [ ] Row counts match Postgres COUNT(*) (stores 50, products 2.5k, customers 100k, invoices 55M, invoice_items ~200M)
- [ ] New CLI installed: `databricks --version` >= 0.213.0
- [ ] Auth is OAuth, not PAT: `databricks auth profiles` shows OAuth
- [ ] No `DATABRICKS_TOKEN` env var set: `env | grep DATABRICKS` empty
- [ ] Volume exists: `LIST '/Volumes/workspace/retail/raw_postgres_export/'` returns 5 folders
- [ ] Volume type is MANAGED (enum fix applied)
- [ ] Upload method succeeded: 53 files in Volume (LIST recursive count)
- [ ] Spark can read: `spark.read.parquet("/Volumes/workspace/retail/raw_postgres_export/invoices/").count() == 55M`
- [ ] No `bash: .venv/bin/databricks No such file` error (hash -r applied if needed)
- [ ] Documentation updated: this file committed to `/docs/04_databricks_bronze.md`

**If any check fails:** Re-run relevant section. Most common is PAT still set - `unset DATABRICKS_TOKEN`.

---

*Author: Data Engineering Portfolio - M4 Medallion Project*  
*Workspace: dbc-a6423fd6-0d52.cloud.databricks.com (Free Edition)*  
*Date: 2025*
