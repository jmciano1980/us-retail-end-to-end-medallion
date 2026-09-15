# M4 - Databricks Bronze/Silver/Gold

## Milestone Goal
Build Medallion Architecture starting from Postgres OLTP (`retail_raw.*`) to Databricks Delta Lake.

Your repo spec:
- **Raw**: Postgres tables = system of record (M3 DONE)
- **Bronze**: Exact copy from Postgres via JDBC/Parquet, no transforms, incremental watermark on `invoice_date`
- **Silver**: Deduplication, null handling, type casting, SCD2 for products
- **Gold**: Star schema - fact_sales, dim_store, dim_product, dim_customer, agg_sales_by_region, agg_product_performance

### Why Postgres -> Parquet -> Bronze?

Databricks Free Edition cannot reach your WSL Docker Postgres at `192.168.182.1` / `localhost:5432`. 
Enterprise pattern for on-prem -> lakehouse:

```
[WSL2 Docker Postgres: retail_raw.*] --COPY--> [Parquet: data/parquet_export/*.parquet] --upload--> [Databricks Volume / Local Delta: bronze.*]
```

This is still 100% from Postgres, not from generator CSVs.

## Folder Structure

```
04_databricks/
├── README.md
├── 01_postgres_to_parquet/
│   ├── export_postgres_to_parquet.py  # M4.1 - FULL optimized, keyset pagination
│   └── requirements.txt
└── 02_bronze_ingestion/
    └── 01_bronze_ingest_from_parquet.py  # M4.2 - reads Parquet -> Delta Bronze
```

## M4.1 - Postgres to Parquet Export

### Volume (Full Mode)

| Table | Rows | Parquet | Parts |
|-------|------|---------|-------|
| stores | 50 | ~10 KB | 1 |
| products | 2,500 | ~500 KB | 1 |
| customers | 100,000 | ~15 MB | 1 |
| invoices | 55,000,000 | ~2.5 GB | 11 x 5M |
| invoice_items | ~200,000,000 | ~9 GB | 40 x 5M |
| **Total** | **~255M** | **~12 GB** | **53 files** |

Need 20GB free in WSL.

### How to Run (WSL2)

```bash
cd ~/projects/us-retail-end-to-end-medallion

# Create venv in WSL (not in VMware VM)
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r 04_databricks/01_postgres_to_parquet/requirements.txt

# Verify Postgres (M3 checkpoint)
docker exec -it retail_postgres psql -U retail_admin -d retail_db -c "
SELECT 'stores' AS t, COUNT(*) FROM retail_raw.stores
UNION ALL SELECT 'products', COUNT(*) FROM retail_raw.products
UNION ALL SELECT 'customers', COUNT(*) FROM retail_raw.customers
UNION ALL SELECT 'invoices', COUNT(*) FROM retail_raw.invoices
UNION ALL SELECT 'invoice_items', COUNT(*) FROM retail_raw.invoice_items;
"

# Export - test small tables first (10 sec)
mkdir -p data/parquet_export
python 04_databricks/01_postgres_to_parquet/export_postgres_to_parquet.py --small-only

# Export big tables - FULL takes 60-90 min
python -u 04_databricks/01_postgres_to_parquet/export_postgres_to_parquet.py --batch-size 5000000

# Verify
du -sh data/parquet_export/
ls -lh data/parquet_export/
```

Key optimization: Uses keyset pagination `WHERE invoice_id > last_id` instead of slow `OFFSET` - critical for 55M rows.

## M4.2 - Bronze Ingestion

After Parquet export, you have two paths (same code):

**Path A - Local Spark (no time limits, recommended for FULL):**

```bash
python -m pip install pyspark==3.5.1 delta-spark==3.2.0
python 04_databricks/02_bronze_ingestion/01_bronze_ingest_from_parquet.py --local
```

Creates `~/lakehouse/bronze/` Delta tables.

**Path B - Databricks Free Edition (for portfolio screenshots):**

1. Upload Parquet to DBFS: `databricks fs cp -r data/parquet_export/ dbfs:/FileStore/retail/raw_postgres_export/`
2. Create notebook in workspace, paste `01_bronze_ingest_from_parquet.py`, run with `RAW_PATH = "dbfs:/FileStore/retail/raw_postgres_export"`

Creates `bronze.stores, bronze.products, bronze.customers, bronze.invoices, bronze.invoice_items` as Delta tables - all columns STRING permissive to keep 1% dirty rows for Silver cleaning, plus `_ingest_timestamp`, `_source`.

### Gitignore

```gitignore
data/parquet_export/
*.parquet
lakehouse/
```

### Next Steps

- M4.3 Silver: Deduplication, null handling, type casting, SCD2 for products
- M4.4 Gold: Star schema for Power BI

### Validation Checklist

- [ ] Postgres counts: 50 / 2500 / 100k / 55M / ~200M
- [ ] `data/parquet_export/` has 53 parquet files ~12 GB
- [ ] `stores.parquet` readable with `pd.read_parquet()`
- [ ] Bronze Delta tables created with `_ingest_timestamp`