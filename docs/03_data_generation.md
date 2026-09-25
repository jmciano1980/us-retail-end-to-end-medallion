# 02 - Data Generation

**Script:** 02_data_gen/generate_retail_data.py
**Output:** data/raw/ (FULL gitignored) stores.csv, products.csv, customers.csv, retail_raw.csv
**Sample:** data/samples/ (for GitHub) stores.csv, products.csv, customers.csv, invoices_sample.csv

retail_raw schema: invoice_id,store_id,customer_id,invoice_date,payment_method,line_item,product_id,quantity,unit_price,discount

Scale: 50 stores, 2500 products, 100k customers, 55M invoices, ~200M rows
Years: 2023 30%, 2024 33%, 2025 37%
Errors 1%: null_date, null_payment, no_customer, no_items, bad_product

Run TEST:
python 02_data_gen/generate_retail_data.py 1000000

FULL:
python 02_data_gen/generate_retail_data.py 55000000

Git: data/raw/*.csv ignored, only data/samples/ committed
