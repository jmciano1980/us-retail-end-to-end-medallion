"""
load_to_postgres.py - FINAL CORRECT VERSION
4 files -> 5 tables (Bronze raw layer, all TEXT permissive to allow 1% dirty rows)

Inputs (from data/raw or data/samples):
  stores.csv       -> retail_raw.stores
  products.csv     -> retail_raw.products
  customers.csv    -> retail_raw.customers
  retail_raw.csv   -> retail_raw.invoices + retail_raw.invoice_items (SPLIT)

Structures (REAL, as generated):
  stores: store_id, store_name, city, state, zip_code, region, manager_name, opened_date, square_footage
  products: product_id, sku, product_name, category, subcategory, brand, unit_price, cost, is_active
  customers: customer_id, first_name, last_name, email, phone, street_address, city, state, zip_code, loyalty_tier, join_date
  retail_raw.csv: invoice_id, store_id, customer_id, invoice_date, payment_method, line_item, product_id, quantity, unit_price, discount
    -> invoices: invoice_id, store_id, customer_id, invoice_date, payment_method (DISTINCT)
    -> invoice_items: invoice_id, line_item, product_id, quantity, unit_price, discount

Run:
  python 03_raw_layer/load_to_postgres.py --truncate
  python 03_raw_layer/load_to_postgres.py --sample --truncate
"""
import os, sys, time
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()
load_dotenv("01_infra/.env")
load_dotenv("03_raw_layer/.env")
load_dotenv(".env")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "192.168.182.1"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "retail_db"),
    "user": os.getenv("POSTGRES_USER", "retail_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail_admin123"),
}

SCHEMA = "retail_raw"

# FINAL DDL - 5 TABLES WITH REAL STRUCTURES, ALL TEXT FOR BRONZE
DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

DROP TABLE IF EXISTS {SCHEMA}.invoice_items CASCADE;
DROP TABLE IF EXISTS {SCHEMA}.invoices CASCADE;
DROP TABLE IF EXISTS {SCHEMA}.stores CASCADE;
DROP TABLE IF EXISTS {SCHEMA}.products CASCADE;
DROP TABLE IF EXISTS {SCHEMA}.customers CASCADE;
DROP TABLE IF EXISTS {SCHEMA}.retail_raw CASCADE;
DROP TABLE IF EXISTS {SCHEMA}.retail_raw_staging CASCADE;

-- 1. stores.csv
CREATE TABLE {SCHEMA}.stores (
    store_id TEXT,
    store_name TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    region TEXT,
    manager_name TEXT,
    opened_date TEXT,
    square_footage TEXT
);

-- 2. products.csv
CREATE TABLE {SCHEMA}.products (
    product_id TEXT,
    sku TEXT,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    brand TEXT,
    unit_price TEXT,
    cost TEXT,
    is_active TEXT
);

-- 3. customers.csv - WITH FIRST_NAME, LAST_NAME, ADDRESS AS YOU DEMANDED
CREATE TABLE {SCHEMA}.customers (
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    loyalty_tier TEXT,
    join_date TEXT
);

-- 4. invoices (header from retail_raw.csv)
CREATE TABLE {SCHEMA}.invoices (
    invoice_id TEXT,
    store_id TEXT,
    customer_id TEXT,
    invoice_date TEXT,
    payment_method TEXT
);

-- 5. invoice_items (lines from retail_raw.csv)
CREATE TABLE {SCHEMA}.invoice_items (
    invoice_id TEXT,
    line_item TEXT,
    product_id TEXT,
    quantity TEXT,
    unit_price TEXT,
    discount TEXT
);

-- Staging for the denormalized file
CREATE TABLE {SCHEMA}.retail_raw_staging (
    invoice_id TEXT,
    store_id TEXT,
    customer_id TEXT,
    invoice_date TEXT,
    payment_method TEXT,
    line_item TEXT,
    product_id TEXT,
    quantity TEXT,
    unit_price TEXT,
    discount TEXT
);
"""

def get_conn():
    print(f"Connecting {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} as {DB_CONFIG['user']}")
    return psycopg2.connect(**DB_CONFIG)

def copy_csv_with_cols(conn, table, csv_path):
    if not csv_path.exists():
        print(f"  SKIP {table}: {csv_path} not found")
        return 0
    size_mb = csv_path.stat().st_size / 1024 / 1024
    print(f"\nCOPY {SCHEMA}.{table} <- {csv_path.name} ({size_mb:.1f} MB)")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        header = f.readline().strip()
    cols = [c.strip() for c in header.split(',')]
    cols_sql = ', '.join([f'"{c}"' for c in cols])

    t0 = time.time()
    with conn.cursor() as cur:
        with open(csv_path, 'r', encoding='utf-8') as f:
            cur.copy_expert(f"COPY {SCHEMA}.{table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
    conn.commit()
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
        cnt = cur.fetchone()[0]
    print(f"  -> {cnt} rows in {time.time()-t0:.1f}s")
    return cnt

if __name__ == "__main__":
    is_sample = "--sample" in sys.argv
    do_truncate = "--truncate" in sys.argv

    base = Path("data/samples") if is_sample else Path("data/raw")
    print(f"Loading from: {base.resolve()} - Mode: {'SAMPLE' if is_sample else 'FULL'}")

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
        print("DDL done: 5 tables created with REAL structures (stores, products, customers, invoices, invoice_items) + staging")

        if do_truncate:
            print("Truncating all 5 tables...")
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE {SCHEMA}.stores, {SCHEMA}.products, {SCHEMA}.customers, {SCHEMA}.invoices, {SCHEMA}.invoice_items, {SCHEMA}.retail_raw_staging CASCADE;")
            conn.commit()

        # 1,2,3 - direct loads
        copy_csv_with_cols(conn, "stores", base / "stores.csv")
        copy_csv_with_cols(conn, "products", base / "products.csv")
        copy_csv_with_cols(conn, "customers", base / "customers.csv")

        # 4 - retail_raw.csv -> staging -> split into invoices + invoice_items
        raw_file = base / "retail_raw.csv"
        if not raw_file.exists():
            raw_file = base / "invoices_sample.csv"
            print(f"Using sample file: {raw_file}")

        print(f"\nLoading denormalized {raw_file.name} into staging...")
        copy_csv_with_cols(conn, "retail_raw_staging", raw_file)

        print("\nSplitting staging -> invoices (DISTINCT header) + invoice_items (lines)...")
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {SCHEMA}.invoices (invoice_id, store_id, customer_id, invoice_date, payment_method)
                SELECT DISTINCT invoice_id, store_id, customer_id, invoice_date, payment_method
                FROM {SCHEMA}.retail_raw_staging;
            """)
            inv_cnt = cur.rowcount
            cur.execute(f"""
                INSERT INTO {SCHEMA}.invoice_items (invoice_id, line_item, product_id, quantity, unit_price, discount)
                SELECT invoice_id, line_item, product_id, quantity, unit_price, discount
                FROM {SCHEMA}.retail_raw_staging;
            """)
            items_cnt = cur.rowcount
        conn.commit()
        print(f"  -> invoices: {inv_cnt} rows")
        print(f"  -> invoice_items: {items_cnt} rows")

        # Clean staging
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {SCHEMA}.retail_raw_staging;")
        conn.commit()

        # VERIFY
        print("\n=== FINAL VERIFY - 5 TABLES ===")
        with conn.cursor() as cur:
            for tbl in ["stores","products","customers","invoices","invoice_items"]:
                cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{tbl}")
                cnt = cur.fetchone()[0]
                print(f"  {tbl}: {cnt}")

    finally:
        conn.close()
