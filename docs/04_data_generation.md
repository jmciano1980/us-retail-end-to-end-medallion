# M2 - Data Generation

## Objective
Generate dirty US retail data simulating a real source system.

## Tool
VMWare Win11 Dev VM, Python 3.11, Faker en_US, seed 42

## Datasets Generated
- `data/raw/retail_raw.csv` - 1,000,000 rows denormalized fact (main source for Postgres)
- `data/raw/stores.csv` - 50 rows
- `data/raw/products.csv` - 2500 rows
- `data/raw/customers.csv` - 100,000 rows
- `data/raw/invoices_sample.csv` - 10k sample headers

## Schema retail_raw.csv
raw_id PK, invoice_id, line_item, store_id, store_name, region,
product_id, product_name, category, subcategory,
customer_id, customer_email,
quantity, unit_price, discount, line_total,
invoice_date, payment_method

## Error Injection 1-2%
- 0.4% null customer_id -> customer_id = ''
- 0.3% invalid store_id -> store_id = 99999
- 0.3% negative quantity -> quantity = -qty
- 0.3% invalid product_id -> product_id = 9999999
- 0.3% zero price -> unit_price = 0
- 0.4% future date -> invoice_date = 2026-06-15

Total dirty ~2% to be quarantined in Silver layer.

## Validation

## How to run
```powershell
.\.venv\Scripts\Activate.ps1
python 02_data_gen/generate_retail_data.py

