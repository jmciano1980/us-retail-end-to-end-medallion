# 03 - Medallion Flow & Raw Layer

Related to 03_raw_layer/ and data/raw/retail_raw.csv

Flow: data/raw/retail_raw.csv -> staging COPY -> split SQL -> retail_raw.invoices + invoice_items -> bronze -> silver -> gold

Load sample: python 01_infra/load_to_postgres.py --sample --truncate
Load full: python 01_infra/load_to_postgres.py --truncate
