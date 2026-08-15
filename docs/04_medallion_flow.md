# Medallion Architecture for US Retail

Raw (Postgres in WSL2 Docker): OLTP normalized tables. Source of truth.

Bronze (Delta on Databricks): Incremental load, no transforms. Adds _ingested_at, _source. Partitioned by ingestion date. Retains bad data.

Silver (Delta): Cleaning layer
- Deduplicate invoice_items by invoice_item_id keeping latest
- Handle nulls: customer_id null = Guest, product_id null -> quarantine table
- Fix invalid quantity: abs() + flag, or quarantine if <=0
- Standardize states, zips, emails
- SCD Type 2 for products price changes

Gold (Delta / Star Schema):
- dim_store (SCD2), dim_product, dim_customer, dim_date
- fact_sales (one row per invoice_item, cleaned)
- agg_sales_by_region (for Regional Director dashboard)
- agg_product_performance (for Product Manager dashboard)
- agg_customer_loyalty

Serving: Databricks SQL Warehouse -> Power BI Direct Query on Gold tables.
