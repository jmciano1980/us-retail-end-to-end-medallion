"""
M4.2 - 01_bronze_ingest_from_parquet.py
Reads Parquet files exported from Postgres and creates Bronze Delta tables

Location: 04_databricks/02_bronze_ingestion/01_bronze_ingest_from_parquet.py

Works in two modes:
1. --local: Local Spark + Delta OSS (no Databricks account, no time limits) -> ~/lakehouse/bronze/
2. Databricks: Paste into Databricks notebook -> /Volumes/main/retail/ or dbfs:/FileStore/retail/

Bronze spec from your README:
- Exact copy from Postgres, no transforms
- All columns kept as STRING permissive (to preserve 1% dirty rows for Silver cleaning)
- Add metadata: _ingest_timestamp, _source, _batch_id
- Incremental watermark on invoice_date (for future loads)

Tables:
- bronze.stores (50)
- bronze.products (2500)
- bronze.customers (100k)
- bronze.invoices (55M)
- bronze.invoice_items (~200M)
"""

import argparse
from pathlib import Path

# Args to support both local and Databricks
parser = argparse.ArgumentParser()
parser.add_argument("--local", action="store_true", help="Run with local Spark Delta OSS")
parser.add_argument("--raw-path", type=str, default=None, help="Override raw parquet path")
args, _ = parser.parse_known_args()

IS_LOCAL = args.local

if IS_LOCAL:
    # --- LOCAL SPARK MODE ---
    print("Running in LOCAL mode - Spark + Delta OSS")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    RAW_PATH = args.raw_path or str(Path(__file__).resolve().parents[2] / "data" / "parquet_export")
    BRONZE_BASE = str(Path.home() / "lakehouse" / "bronze")
    Path(BRONZE_BASE).mkdir(parents=True, exist_ok=True)

    builder = SparkSession.builder \
        .appName("RetailBronzeLocal") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", BRONZE_BASE)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def ingest_table(table_name):
        print(f"\n--- Ingesting {table_name} from {RAW_PATH} ---")
        # For big tables, wildcard: invoices_part_*.parquet
        df = spark.read.format("parquet").load(f"{RAW_PATH}/{table_name}*.parquet")
        
        # Bronze: keep raw + metadata, no transforms
        df_bronze = df \
            .withColumn("_ingest_timestamp", F.current_timestamp()) \
            .withColumn("_source", F.lit(f"postgres.retail_raw.{table_name}")) \
            .withColumn("_batch_id", F.lit("2026-01-14_initial_load_m4"))

        target_path = f"{BRONZE_BASE}/{table_name}"
        df_bronze.write.format("delta").mode("overwrite").option("mergeSchema","true").save(target_path)
        
        cnt = spark.read.format("delta").load(target_path).count()
        print(f" -> {target_path}: {cnt:,} rows")
        return cnt

else:
    # --- DATABRICKS MODE ---
    # This block runs as Databricks notebook
    print("Running in DATABRICKS mode")
    from pyspark.sql import functions as F

    # Change this to your Volume / DBFS path
    RAW_PATH = args.raw_path or "/Volumes/main/retail/raw_postgres_export"
    # For Free Edition: "dbfs:/FileStore/retail/raw_postgres_export"
    
    CATALOG = "main"
    BRONZE_SCHEMA = "bronze"

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
    except Exception as e:
        print(f"Note: {e} - may be Free Edition without Unity Catalog, using hive_metastore")
        CATALOG = "hive_metastore"
        BRONZE_SCHEMA = "bronze"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_SCHEMA}")

    def ingest_table(table_name):
        print(f"\n--- Ingesting {table_name} ---")
        df = spark.read.format("parquet").load(f"{RAW_PATH}/{table_name}*.parquet")
        
        df_bronze = df \
            .withColumn("_ingest_timestamp", F.current_timestamp()) \
            .withColumn("_source", F.lit(f"postgres.retail_raw.{table_name}")) \
            .withColumn("_batch_id", F.lit("2026-01-14_initial_load_m4"))

        full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}" if CATALOG != "hive_metastore" else f"{BRONZE_SCHEMA}.{table_name}"
        df_bronze.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(full_table)
        
        cnt = spark.table(full_table).count()
        print(f" -> {full_table}: {cnt:,} rows")
        return cnt


# Common execution for both modes
if __name__ == "__main__" or not IS_LOCAL:
    # In Databricks, __name__ != "__main__" when run as notebook, so we run anyway
    TABLES = ["stores", "products", "customers", "invoices", "invoice_items"]
    
    results = {}
    for tbl in TABLES:
        try:
            results[tbl] = ingest_table(tbl)
        except Exception as e:
            print(f"FAILED {tbl}: {e}")
            import traceback
            traceback.print_exc()

    print("\n=== BRONZE VERIFY ===")
    for tbl, cnt in results.items():
        print(f"  {tbl:15s}: {cnt:,} rows")

    print("\nNext: M4.3 Silver cleaning (deduplication, null handling, type casting)")
