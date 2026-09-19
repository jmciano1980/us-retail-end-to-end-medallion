"""
M4.1 - export_postgres_to_parquet.py - FULL MODE OPTIMIZED
Exports 5 tables FROM Postgres (retail_raw.*) to Parquet for Bronze ingestion

Location: 04_databricks/01_postgres_to_parquet/export_postgres_to_parquet.py

Why Parquet?
- Columnar + Snappy compressed, 70% smaller than CSV
- Databricks native with predicate pushdown
- Handles 200M rows without OOM

Full mode estimation:
- stores 50 rows ~10 KB
- products 2500 rows ~500 KB
- customers 100k rows ~15 MB
- invoices 55M rows ~2.5 GB (11 parts of 5M)
- invoice_items ~200M rows ~9 GB (40 parts of 5M)
Total: ~12 GB, 53 files

Run in WSL2:
  python 04_databricks/01_postgres_to_parquet/export_postgres_to_parquet.py --batch-size 5000000

Time: stores/products/customers 10 sec, invoices 15-20 min, invoice_items 45-60 min
"""

import os
import sys
import time
import argparse
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Load env from project root and 01_infra
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
load_dotenv(ROOT / "01_infra" / ".env")
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "retail_db"),
    "user": os.getenv("POSTGRES_USER", "retail_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail_admin123"),
}

SCHEMA = "retail_raw"
EXPORT_DIR = ROOT / "data" / "parquet_export"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

def get_conn():
    print(f"Connecting to Postgres {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} as {DB_CONFIG['user']}")
    print(f"Export dir: {EXPORT_DIR.resolve()}")
    return psycopg2.connect(**DB_CONFIG)

def get_count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
        return cur.fetchone()[0]

def export_small_table(conn, table):
    import pandas as pd
    print(f"\n--- Exporting {SCHEMA}.{table} (small) ---")
    total = get_count(conn, table)
    print(f"  Rows in Postgres: {total:,}")
    
    df = pd.read_sql(f"SELECT * FROM {SCHEMA}.{table} ORDER BY 1", conn)
    
    parquet_path = EXPORT_DIR / f"{table}.parquet"
    df.to_parquet(parquet_path, index=False, compression="snappy")
    
    size_mb = parquet_path.stat().st_size / 1024 / 1024
    print(f"  -> {parquet_path.name} | {len(df):,} rows | {size_mb:.2f} MB | {len(df.columns)} cols")
    return len(df)

def export_big_table(conn, table, batch_size, order_col="invoice_id"):
    """
    Keyset pagination for 55M+ rows - much faster than LIMIT/OFFSET
    """
    import pandas as pd
    print(f"\n--- Exporting {SCHEMA}.{table} (BIG - {batch_size:,} rows/batch) ---")
    total = get_count(conn, table)
    print(f"  Rows in Postgres: {total:,}")
    print(f"  Estimated parts: {total // batch_size + 1}")

    t0 = time.time()
    last_id = 0
    part_num = 0
    exported = 0
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX({order_col}::bigint) FROM {SCHEMA}.{table}")
        max_id = cur.fetchone()[0] or total
        print(f"  Max {order_col}: {max_id:,}")

    while True:
        if table == "invoice_items":
            query = f"""
                SELECT * FROM {SCHEMA}.{table}
                WHERE {order_col}::bigint > {last_id}
                ORDER BY {order_col}::bigint, line_item::bigint
                LIMIT {batch_size}
            """
        else:
            query = f"""
                SELECT * FROM {SCHEMA}.{table}
                WHERE {order_col}::bigint > {last_id}
                ORDER BY {order_col}::bigint
                LIMIT {batch_size}
            """
        
        df = pd.read_sql(query, conn)
        if df.empty:
            break
        
        parquet_path = EXPORT_DIR / f"{table}_part_{part_num:04d}.parquet"
        df.to_parquet(parquet_path, index=False, compression="snappy")
        
        exported += len(df)
        last_id = int(df[order_col].astype(int).max())
        
        elapsed = time.time() - t0
        rate = exported / elapsed if elapsed > 0 else 0
        size_mb = parquet_path.stat().st_size / 1024 / 1024
        
        print(f"  Part {part_num:04d}: {len(df):,} rows (total {exported:,}/{total:,} {exported/total*100:.1f}%) -> {parquet_path.name} {size_mb:.1f} MB | {rate:.0f} rows/sec | last_id={last_id:,}")
        
        part_num += 1
        
        if exported >= total:
            break
    
    total_time = time.time() - t0
    print(f"  DONE {table}: {exported:,} rows in {part_num} parts, {total_time/60:.1f} min")
    return exported

def verify_export():
    print("\n=== VERIFY PARQUET EXPORT ===")
    total_size = 0
    files = sorted(EXPORT_DIR.glob("*.parquet"))
    for f in files:
        size_mb = f.stat().st_size / 1024 / 1024
        total_size += size_mb
        print(f"  {f.name:45s} {size_mb:8.2f} MB")
    print(f"  TOTAL: {total_size/1024:.2f} GB in {len(files)} files")
    print(f"  Location: {EXPORT_DIR.resolve()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="M4.1 Export Postgres retail_raw.* to Parquet")
    parser.add_argument("--batch-size", type=int, default=5000000, help="Rows per parquet part for big tables (default 5M)")
    parser.add_argument("--small-only", action="store_true", help="Export only small tables")
    parser.add_argument("--big-only", action="store_true", help="Export only big tables")
    args = parser.parse_args()

    print(f"ROOT: {ROOT}")
    print(f"Batch size: {args.batch_size:,}")

    conn = get_conn()
    try:
        summary = {}
        
        if not args.big_only:
            summary["stores"] = export_small_table(conn, "stores")
            summary["products"] = export_small_table(conn, "products")
            summary["customers"] = export_small_table(conn, "customers")
        
        if not args.small_only:
            summary["invoices"] = export_big_table(conn, "invoices", args.batch_size, "invoice_id")
            summary["invoice_items"] = export_big_table(conn, "invoice_items", args.batch_size, "invoice_id")
        
        print("\n=== FINAL SUMMARY ===")
        for k,v in summary.items():
            print(f"  {k:15s}: {v:,} rows")
        
        verify_export()
        
    finally:
        conn.close()
    
    print("\nNext: M4.2 Bronze ingestion")
    print("  Local: python 04_databricks/02_bronze_ingestion/01_bronze_ingest_from_parquet.py --local")
    print("  Databricks: upload to dbfs:/FileStore/retail/raw_postgres_export/")
