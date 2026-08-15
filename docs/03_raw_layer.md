# M3 - Load to Postgres (Raw Layer)

**Status:** Done  
**Environment:** DEV VM (Windows) -> Host (Windows) -> WSL2 Ubuntu (Docker Postgres)

## 1. Objective

Load CSVs generated in M2 into Postgres `retail_raw` schema without creating new tables. Raw layer must be **permissive**: allow NULLs, bad types, and keep ingestion metadata.

We have 4 files but 5 existing tables in WSL. `retail_raw.csv` is denormalized and feeds two tables.

## 2. Infrastructure

Postgres runs in Docker inside WSL2. DEV VM cannot reach WSL2 directly.

```
[DEV VM 192.168.182.x]
      |
      | 192.168.182.1:5432 (HOST_IP from WSL /etc/resolv.conf)
      v
[HOST Windows 10/11]
  0.0.0.0:5432 portproxy -> 172.18.54.20:5432 (WSL2 IP, changes on reboot)
      |
      v
[WSL2 Ubuntu - docker container retail_db:5432]
```

### Host setup (Admin PowerShell) - must re-run after WSL reboot

```powershell
$wslIp = (wsl hostname -I).Split()[0]
Write-Host "WSL IP: $wslIp"
netsh interface portproxy delete v4tov4 listenaddress=0.0.0.0 listenport=5432
netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=5432 connectaddress=$wslIp connectport=5432
netsh advfirewall firewall add rule name="Postgres 5432" dir=in action=allow protocol=TCP localport=5432
netsh interface portproxy show v4tov4
```

### WSL2 verification

```bash
docker ps | grep postgres
# CONTAINER ID ... 0.0.0.0:5432->5432/tcp retail_postgres

psql -U retail_admin -d retail_db -h localhost -c "\dt retail_raw.*"
# retail_raw.stores
# retail_raw.products
# retail_raw.customers
# retail_raw.invoices
# retail_raw.invoice_items

psql -U retail_admin -d retail_db -h localhost -c "
SELECT 'ALTER TABLE '||table_schema||'.'||table_name||' ALTER COLUMN '||column_name||' DROP NOT NULL;'
FROM information_schema.columns
WHERE table_schema='retail_raw' AND is_nullable='NO' AND column_name NOT IN ('raw_id')
" -t -A | psql -U retail_admin -d retail_db -h localhost
```

### DEV VM .env

```env
POSTGRES_HOST=192.168.182.1
POSTGRES_PORT=5432
POSTGRES_DB=retail_db
POSTGRES_USER=retail_admin
POSTGRES_PASSWORD=Retail123!
CSV_INPUT_DIR=data/raw
```

## 3. Source Files (data/raw)

| File | Size | Header |
|---|---|---|
| `stores.csv` | 2.5 KB | `store_id,store_name,city,state,region` |
| `products.csv` | 135 KB | product dimension |
| `customers.csv` | 6.5 MB | customer dimension, ~100k rows |
| `retail_raw.csv` | 164 MB | `raw_id,invoice_id,line_item,store_id,store_name,region,product_id,product_name,category,subcategory,customer_id,customer_email,quantity,unit_price,discount,line_total,invoice_date,payment_method` |
| `invoices_sample.csv` | 346 KB | sample only, not used in final load |

## 4. Destination Tables (5 existing tables in WSL)

All tables have `raw_id BIGSERIAL PRIMARY KEY` and `ingestion_ts TIMESTAMPTZ DEFAULT NOW()`. All other columns are **NULLABLE** in raw layer.

```sql
-- retail_raw.stores
raw_id, store_id, store_name, region, state, city, zip_code, manager_name, opened_date, square_footage, ingestion_ts

-- retail_raw.products
raw_id, product_id, product_name, category, subcategory, unit_price, ... ingestion_ts

-- retail_raw.customers
raw_id, customer_id, customer_email, ... ingestion_ts

-- retail_raw.invoices (header)
raw_id, invoice_id, store_id, customer_id, invoice_date, payment_method, ingestion_ts

-- retail_raw.invoice_items (line items)
raw_id, invoice_id, line_item, store_id, product_id, quantity, unit_price, discount, line_total, ingestion_ts
```

## 5. Mapping: 4 Files -> 5 Tables

```
stores.csv      -> retail_raw.stores
products.csv    -> retail_raw.products
customers.csv   -> retail_raw.customers
retail_raw.csv  -distinct invoice_id-> retail_raw.invoices
retail_raw.csv  -all rows-----------> retail_raw.invoice_items
```

No new tables are created during load. `retail_raw.csv` is split in memory.

## 6. Loader: 01_infra/load_to_postgres.py

### Design Principles (M3 fix)

1. **No assumption CSV name == table column name**
   - Reads real columns from `information_schema.columns` at runtime
   - Normalizes names: `normalize(col) = lower + [^a-z0-9] -> _`
   - Matches `Store Name` -> `store_name`, `StoreID` -> `store_id`
   - Fuzzy fallback: `store` contains `store_name`

2. **No raw_id violation**
   ```python
   if t_col == 'raw_id' and normalize(t_col) not in csv_norm: continue
   if t_col == 'ingestion_ts' and normalize(t_col) not in csv_norm: continue
   ```
   If CSV provides `raw_id` (retail_raw.csv does), insert it. Otherwise let DB auto-generate.

3. **Type cleaning**
   - Integer column gets `"Store 1 - North Judithbury"` -> regex `\d+` -> `1`
   - Numeric: strip currency symbols, parse float
   - Empty string -> NULL

4. **Idempotent**
   - `TRUNCATE ... CASCADE` before insert
   - Uses `psycopg2.extras.execute_batch` page_size=1000

### Full Script

See `01_infra/load_to_postgres.py` v5 (final). Key functions:
- `get_table_cols(cur, full_table)` - introspection
- `load_simple_table()` - dims
- `load_invoices_and_items()` - splits retail_raw.csv into distinct invoices + all items

## 7. How to Run

DEV VM PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python 01_infra\load_to_postgres.py
```

Expected:

```
=== stores.csv -> retail_raw.stores ===
WSL cols: [raw_id, store_id, store_name, region, state, city, ...]
CSV cols: [store_id, store_name, city, state, region]
Mapping: [store_id<-store_id, store_name<-store_name, ...]
Loaded 50 rows

=== retail_raw.csv -> retail_raw.invoices + retail_raw.invoice_items ===
Loaded 348319 invoices
Loaded 1000000 invoice_items

COMMIT OK
```

## 8. Verification

```sql
-- WSL2 or DEV VM psql
SELECT 'stores' as table_name, count(*) FROM retail_raw.stores
UNION ALL SELECT 'products', count(*) FROM retail_raw.products
UNION ALL SELECT 'customers', count(*) FROM retail_raw.customers
UNION ALL SELECT 'invoices', count(*) FROM retail_raw.invoices
UNION ALL SELECT 'invoice_items', count(*) FROM retail_raw.invoice_items;

-- sample
SELECT * FROM retail_raw.invoice_items LIMIT 5;
SELECT * FROM retail_raw.invoices LIMIT 5;
```

DEV VM Python quick check:

```powershell
python -c "
import psycopg2, os; from dotenv import load_dotenv; load_dotenv();
conn=psycopg2.connect(host=os.getenv('POSTGRES_HOST'),port=os.getenv('POSTGRES_PORT'),dbname=os.getenv('POSTGRES_DB'),user=os.getenv('POSTGRES_USER'),password=os.getenv('POSTGRES_PASSWORD'));
cur=conn.cursor();
[print(t, cur.execute(f'SELECT COUNT(*) FROM {t}') or cur.fetchone()) for t in ['retail_raw.stores','retail_raw.products','retail_raw.customers','retail_raw.invoices','retail_raw.invoice_items']];
"
```

## 9. Troubleshooting Log

| Error | Root Cause | Fix Applied |
|---|---|---|
| `InvalidTextRepresentation: invalid input syntax for type integer: "Store 1 - North Judithbury"` | Used `COPY table FROM STDIN` without column list, CSV order != table order | Map by normalized name, not position |
| `NotNullViolation: null value in column "raw_id"` | Inserted NULL into BIGSERIAL PK | Skip `raw_id`/`ingestion_ts` if CSV doesn't have them |
| `sales.csv not found` / 0 rows for fact | Generator creates `retail_raw.csv`, not `sales.csv` | Map `retail_raw.csv` -> `invoices` + `invoice_items`, distinct + full |
| WSL IP changes after reboot | `172.18.54.20` is ephemeral | Re-run `netsh interface portproxy` script on Host |

## 10. Next Steps (M4 Bronze)

- Clean `retail_raw` -> `retail_bronze` with proper types: `invoice_date::DATE`, `unit_price::NUMERIC`, etc.
- Deduplicate, handle SCD for stores/products
- Add data quality checks: `quantity >0`, `line_total = quantity*unit_price*(1-discount)`

---
Generated for M3 Load to Postgres Raw - 2026-08-14
