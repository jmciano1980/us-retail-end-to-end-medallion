# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Bronze Loading Process - FIXED v2 - NO RELOAD
# MAGIC **Bug fix:** Previous version compared file_path exactly, but dbutils.fs.ls returns different format than stored path -> invoice_items reloaded.
# MAGIC **Fix:** Check by file_name + table_name (robust) AND file_path + table_name.

# COMMAND ----------
dbutils.widgets.text("volume_path", "/Volumes/workspace/retail/raw_postgres_export/", "Volume Path")
dbutils.widgets.text("job_run_id", "manual", "Job Run ID")
dbutils.widgets.text("force_reload", "false", "Force Reload true/false")

volume_path = dbutils.widgets.get("volume_path").rstrip("/") + "/"
job_run_id = dbutils.widgets.get("job_run_id")
force_reload = dbutils.widgets.get("force_reload").lower() == "true"

print(f"Volume: {volume_path}, Force: {force_reload}")

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit
import time

CATALOG = "workspace"
SCHEMA = "retail"
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.bronze_file_log"

def ensure_control_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
          file_name STRING,
          file_path STRING,
          table_name STRING,
          status STRING,
          row_count BIGINT,
          file_size_bytes BIGINT,
          first_seen_at TIMESTAMP,
          last_processed_at TIMESTAMP,
          load_duration_seconds DOUBLE,
          job_run_id STRING,
          error_message STRING
        ) USING DELTA
    """)

def get_volume_files(path):
    files = dbutils.fs.ls(path)
    parquet_files = [f.path for f in files if f.path.endswith(".parquet")]
    def sort_key(p):
        pl = p.lower()
        if "store" in pl: return (0, p)
        if "product" in pl: return (1, p)
        if "customer" in pl: return (2, p)
        if "invoice_item" in pl: return (3, p)
        if "invoice" in pl: return (4, p)
        return (5, p)
    return sorted(parquet_files, key=sort_key)

def map_file_to_table(file_path):
    name = file_path.lower()
    if "invoice_item" in name or "invoice_items" in name:
        return "bronze_invoice_items"
    elif "invoice" in name:
        return "bronze_invoices"
    elif "customer" in name:
        return "bronze_customers"
    elif "product" in name:
        return "bronze_products"
    elif "store" in name:
        return "bronze_stores"
    else:
        return None

def get_loaded_set():
    """
    Returns TWO sets for robust check:
    - loaded by (file_name, table_name)
    - loaded by (file_path, table_name) normalized
    """
    try:
        rows = spark.sql(f"SELECT file_name, file_path, table_name FROM {CONTROL_TABLE} WHERE status = 'LOADED'").collect()
        by_name = set((r.file_name, r.table_name) for r in rows)
        # Normalize path: strip trailing slash, lower
        by_path = set((r.file_path.rstrip("/").lower(), r.table_name) for r in rows)
        print(f"Control log has {len(rows)} LOADED entries")
        for r in rows[:20]:
            print(f"  LOGGED: {r.file_name} -> {r.table_name} | path={r.file_path}")
        return by_name, by_path
    except Exception as e:
        print(f"Control table read failed or empty: {e}")
        return set(), set()

def is_already_loaded(file_path, table_name, by_name_set, by_path_set):
    if force_reload:
        return False
    file_name = file_path.split("/")[-1]
    # Check by file_name + table (most robust)
    if (file_name, table_name) in by_name_set:
        return True
    # Check by normalized path + table
    if (file_path.rstrip("/").lower(), table_name) in by_path_set:
        return True
    return False

# COMMAND ----------
def load_file(file_path, table_name, run_id):
    start = time.time()
    file_name = file_path.split("/")[-1]
    target = f"{CATALOG}.{SCHEMA}.{table_name}"
    print(f"\n📥 Loading: {file_name} -> {table_name}")
    try:
        try:
            fsize = dbutils.fs.ls(file_path)[0].size
        except:
            try:
                parent = "/".join(file_path.rstrip("/").split("/")[:-1]) + "/"
                fsize = [f.size for f in dbutils.fs.ls(parent) if f.path == file_path][0]
            except:
                fsize = 0

        df = spark.read.parquet(file_path)
        df_meta = df.withColumn("_source_file_name", lit(file_name)) \
                    .withColumn("_source_file_path", lit(file_path)) \
                    .withColumn("_ingest_timestamp", current_timestamp()) \
                    .withColumn("_update_timestamp", current_timestamp()) \
                    .withColumn("_bronze_load_id", lit(run_id))
        
        df_meta.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
        dur = time.time() - start
        
        spark.sql(f"""
            INSERT INTO {CONTROL_TABLE}
            VALUES ('{file_name}', '{file_path}', '{table_name}', 'LOADED', -1, {fsize}, current_timestamp(), current_timestamp(), {dur}, '{run_id}', NULL)
        """)
        print(f"✅ LOADED {file_name} -> {table_name} in {dur:.1f}s")
        return True
    except Exception as e:
        dur = time.time() - start
        err = str(e).replace("'", "''")[:2000]
        print(f"❌ FAILED {file_name}: {e}")
        try:
            spark.sql(f"""
                INSERT INTO {CONTROL_TABLE}
                VALUES ('{file_name}', '{file_path}', '{table_name}', 'FAILED', 0, 0, current_timestamp(), current_timestamp(), {dur}, '{run_id}', '{err}')
            """)
        except:
            pass
        return False

# COMMAND ----------
ensure_control_table()
all_files = get_volume_files(volume_path)
print(f"\nFound {len(all_files)} files in volume:")
for f in all_files:
    print(f"  {f} -> {map_file_to_table(f)}")

by_name, by_path = get_loaded_set()

total_new = 0
total_skip = 0
total_fail = 0

for fp in all_files:
    tbl = map_file_to_table(fp)
    if not tbl:
        print(f"⚠️ Skip unknown: {fp}")
        continue
    
    if is_already_loaded(fp, tbl, by_name, by_path):
        print(f"⏭️ SKIP - Already LOADED: {fp.split('/')[-1]} -> {tbl}")
        total_skip += 1
        continue
    
    ok = load_file(fp, tbl, job_run_id)
    if ok:
        total_new += 1
        # Update sets immediately so same file not reloaded again in this run
        by_name.add((fp.split("/")[-1], tbl))
        by_path.add((fp.rstrip("/").lower(), tbl))
    else:
        total_fail += 1

print(f"\n{'='*60}")
print(f"Done - New: {total_new}, Skipped: {total_skip}, Failed: {total_fail}")
print(f"{'='*60}")

for tbl in ["bronze_stores","bronze_products","bronze_customers","bronze_invoices","bronze_invoice_items"]:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) as c FROM {CATALOG}.{SCHEMA}.{tbl}").collect()[0].c
        print(f"{tbl}: {cnt} rows")
    except:
        print(f"{tbl}: not exists")

spark.sql(f"SELECT file_name, table_name, status, last_processed_at FROM {CONTROL_TABLE} ORDER BY last_processed_at DESC LIMIT 30").display()
