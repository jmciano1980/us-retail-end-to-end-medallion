import os, re, csv
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extras

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "retail_db"),
    "user": os.getenv("POSTGRES_USER", "retail_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "Retail123!"),
}
CSV_DIR = Path(os.getenv("CSV_INPUT_DIR", "data/raw"))

# 4 files -> 5 tables. retail_raw.csv feeds 2 tables.
FILE_MAP = {
    "stores.csv": "retail_raw.stores",
    "products.csv": "retail_raw.products",
    "customers.csv": "retail_raw.customers",
}

RETAIL_RAW_CSV = CSV_DIR / "retail_raw.csv"
INVOICES_TABLE = "retail_raw.invoices"
ITEMS_TABLE = "retail_raw.invoice_items"

def normalize(s):
    return re.sub(r'[^0-9a-z]+','_', str(s).strip().lower()).strip('_')

def get_table_cols(cur, full_table):
    schema, table = full_table.split('.',1)
    cur.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position
    """,(schema, table))
    rows = cur.fetchall()
    if not rows:
        raise Exception(f"Table {full_table} not found in WSL")
    return rows

def clean(v, pg_type):
    if v is None: return None
    s=str(v).strip()
    if s=='' or s.lower()=='null': return None
    if pg_type in ('integer','bigint','smallint'):
        try: return int(s)
        except:
            m=re.search(r'-?\d+', s)
            return int(m.group()) if m else None
    if pg_type in ('numeric','double precision','real','decimal'):
        try: return float(re.sub(r'[^0-9\.\-]','',s))
        except: return None
    return s

def load_simple_table(cur, csv_path, full_table):
    print(f"\n=== {csv_path.name} -> {full_table} ===")
    table_cols = get_table_cols(cur, full_table)
    print(f"WSL cols: {[c[0] for c in table_cols]}")

    with open(csv_path,'r',encoding='utf-8-sig',newline='') as f:
        reader = csv.DictReader(f)
        csv_headers = reader.fieldnames
        print(f"CSV cols: {csv_headers}")
        csv_norm = {normalize(h): h for h in csv_headers}

        insert_cols=[]
        for t_col, t_type in table_cols:
            if t_col == 'raw_id' and normalize(t_col) not in csv_norm:
                continue
            if t_col == 'ingestion_ts' and normalize(t_col) not in csv_norm:
                continue
            src = csv_norm.get(normalize(t_col))
            if not src:
                for cnorm, corig in csv_norm.items():
                    if normalize(t_col) in cnorm or cnorm in normalize(t_col):
                        src=corig; break
            if src:
                insert_cols.append((t_col, src, t_type))

        print(f"Mapping: {[(c[0]+'<-'+c[1]) for c in insert_cols]}")
        cur.execute(f"TRUNCATE TABLE {full_table} CASCADE;")
        rows=[tuple(clean(r.get(src), ttype) for _,src,ttype in insert_cols) for r in reader]
        if not rows: return
        cols_sql=", ".join([f'"{c}"' for c,_,_ in insert_cols])
        ph=", ".join(["%s"]*len(insert_cols))
        extras.execute_batch(cur, f'INSERT INTO {full_table} ({cols_sql}) VALUES ({ph})', rows, page_size=1000)
        print(f"Loaded {len(rows)} rows")

def load_invoices_and_items(cur):
    print(f"\n=== {RETAIL_RAW_CSV.name} -> {INVOICES_TABLE} + {ITEMS_TABLE} ===")
    if not RETAIL_RAW_CSV.exists():
        print("retail_raw.csv not found"); return

    # read once
    with open(RETAIL_RAW_CSV,'r',encoding='utf-8-sig',newline='') as f:
        reader = list(csv.DictReader(f))
    csv_headers = reader[0].keys() if reader else []
    csv_norm = {normalize(h): h for h in csv_headers}
    print(f"retail_raw.csv cols: {list(csv_headers)}")

    # --- INVOICES: distinct invoice_id ---
    inv_cols = get_table_cols(cur, INVOICES_TABLE)
    print(f"Invoices WSL cols: {[c[0] for c in inv_cols]}")
    inv_insert=[]
    for t_col, t_type in inv_cols:
        if t_col in ('raw_id','ingestion_ts') and normalize(t_col) not in csv_norm:
            continue
        src = csv_norm.get(normalize(t_col))
        if not src:
            for cnorm, corig in csv_norm.items():
                if normalize(t_col) in cnorm or cnorm in normalize(t_col):
                    src=corig; break
        if src:
            inv_insert.append((t_col, src, t_type))

    cur.execute(f"TRUNCATE TABLE {ITEMS_TABLE} CASCADE;")
    cur.execute(f"TRUNCATE TABLE {INVOICES_TABLE} CASCADE;")

    seen=set()
    inv_rows=[]
    for r in reader:
        inv_id = r.get(csv_norm.get('invoice_id') or 'invoice_id')
        if inv_id in seen: continue
        seen.add(inv_id)
        inv_rows.append(tuple(clean(r.get(src), ttype) for _,src,ttype in inv_insert))

    cols_sql=", ".join([f'"{c}"' for c,_,_ in inv_insert])
    ph=", ".join(["%s"]*len(inv_insert))
    extras.execute_batch(cur, f'INSERT INTO {INVOICES_TABLE} ({cols_sql}) VALUES ({ph})', inv_rows, page_size=1000)
    print(f"Loaded {len(inv_rows)} invoices")

    # --- ITEMS ---
    item_cols = get_table_cols(cur, ITEMS_TABLE)
    print(f"Items WSL cols: {[c[0] for c in item_cols]}")
    item_insert=[]
    for t_col, t_type in item_cols:
        if t_col in ('raw_id','ingestion_ts') and normalize(t_col) not in csv_norm:
            continue
        src = csv_norm.get(normalize(t_col))
        if not src:
            for cnorm, corig in csv_norm.items():
                if normalize(t_col) in cnorm or cnorm in normalize(t_col):
                    src=corig; break
        if src:
            item_insert.append((t_col, src, t_type))

    item_rows=[tuple(clean(r.get(src), ttype) for _,src,ttype in item_insert) for r in reader]
    cols_sql=", ".join([f'"{c}"' for c,_,_ in item_insert])
    ph=", ".join(["%s"]*len(item_insert))
    extras.execute_batch(cur, f'INSERT INTO {ITEMS_TABLE} ({cols_sql}) VALUES ({ph})', item_rows, page_size=1000)
    print(f"Loaded {len(item_rows)} invoice_items")

def main():
    print(f"CSV_DIR: {CSV_DIR.resolve()}")
    conn=psycopg2.connect(**DB_CONFIG)
    try:
        cur=conn.cursor()
        # 1. simple dims
        for csv_name, table in FILE_MAP.items():
            p=CSV_DIR/csv_name
            if p.exists():
                load_simple_table(cur, p, table)
            else:
                print(f"Missing {p}")
        # 2. fact split
        load_invoices_and_items(cur)
        conn.commit()
        print("\nCOMMIT OK")
    except Exception as e:
        conn.rollback()
        print(f"ROLLBACK {e}")
        raise
    finally:
        conn.close()

if __name__=="__main__":
    main()