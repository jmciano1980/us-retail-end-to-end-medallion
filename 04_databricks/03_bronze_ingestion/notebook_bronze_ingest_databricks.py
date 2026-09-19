# Databricks notebook source
# MAGIC %md
# MAGIC ### M4.2 Bronze Ingestion - Databricks Free Edition (DBFS disabled)
# MAGIC Reads Parquet from Volume /Volumes/workspace/retail/raw_postgres_export
# MAGIC Writes to workspace.bronze.* as Delta
# MAGIC Works with DBFS_DISABLED workspaces (new Free Edition)

# COMMAND ----------
# Check catalogs - should contain 'workspace'
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------
# Setup - idempotent
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.retail")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.retail.raw_postgres_export")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.quarantine")

RAW_PATH = "/Volumes/workspace/retail/raw_postgres_export"
print(f"RAW_PATH = {RAW_PATH}")
display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------
from pyspark.sql import functions as F

def ingest_table(table_name: str):
    print(f"\n--- Ingesting {table_name} from {RAW_PATH}/{table_name}*.parquet ---")
    df = spark.read.format("parquet").option("mergeSchema","true").load(f"{RAW_PATH}/{table_name}*.parquet")
    df_bronze = (
        df.withColumn("_ingest_timestamp", F.current_timestamp())
         .withColumn("_source", F.lit(f"postgres.retail_raw.{table_name}"))
         .withColumn("_batch_id", F.lit("2026-01-14_initial_load_m4"))
    )
    target_table = f"workspace.bronze.{table_name}"
    (
        df_bronze.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .option("mergeSchema","true")
       .saveAsTable(target_table)
    )
    cnt = spark.table(target_table).count()
    print(f" -> {target_table}: {cnt:,} rows")
    return cnt

# COMMAND ----------
TABLES = ["stores","products","customers","invoices","invoice_items"]
results = {}
for tbl in TABLES:
    results[tbl] = ingest_table(tbl)

print("\n=== BRONZE VERIFY ===")
for tbl, cnt in results.items():
    print(f" {tbl:15s}: {cnt:,} rows")

# Expected: stores 50, products 2500, customers 100000, invoices 54890070, invoice_items 193028767