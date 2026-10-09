-- M8 Gold base model DDL. Filename retained exactly as requested.
-- All objects created here are GOLD objects under workspace.gold.
CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_date (
 date_key INT, full_date DATE, day INT, day_of_week INT, day_name STRING,
 week_of_year INT, month INT, month_name STRING, quarter INT, year INT,
 year_month STRING, year_quarter STRING, is_weekend BOOLEAN,
 is_month_start BOOLEAN, is_month_end BOOLEAN,
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_geography (
 geography_key BIGINT, region STRING, state STRING, city STRING, zip_code STRING,
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_customer (
 customer_key BIGINT, customer_id STRING, first_name STRING, last_name STRING,
 email STRING, phone STRING, street_address STRING, loyalty_tier STRING,
 join_date DATE, geography_key BIGINT, effective_from TIMESTAMP, effective_to TIMESTAMP,
 is_current BOOLEAN, attribute_hash STRING, _insert_datetime_utc TIMESTAMP,
 _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_store (
 store_key BIGINT, store_id STRING, store_name STRING, manager_name STRING,
 opened_date DATE, square_footage BIGINT, geography_key BIGINT,
 effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
 attribute_hash STRING, _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_category (
 category_key BIGINT, category_name STRING,
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_subcategory (
 subcategory_key BIGINT, subcategory_name STRING, category_key BIGINT,
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_product (
 product_key BIGINT, product_id STRING, sku STRING, product_name STRING,
 subcategory_key BIGINT, brand STRING, unit_price DECIMAL(18,2), cost DECIMAL(18,2),
 is_active BOOLEAN, effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
 attribute_hash STRING, _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_data_quality_rule (
 dq_rule_key BIGINT, dq_error_code STRING, dq_rule_name STRING,
 dq_rule_description STRING, source_table STRING, severity STRING,
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.fact_invoice (
 invoice_key BIGINT, invoice_id STRING, date_key INT, customer_key BIGINT,
 store_key BIGINT, payment_method STRING,
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.fact_invoice_item (
 invoice_item_key BIGINT, invoice_id STRING, line_item BIGINT, invoice_key BIGINT,
 product_key BIGINT, quantity BIGINT, unit_price DECIMAL(18,2), discount DECIMAL(18,2),
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.fact_data_quality_issue (
 dq_issue_key BIGINT, source_table STRING, source_row_id STRING, source_file_name STRING,
 dq_rule_key BIGINT, dq_error_code STRING, dq_error_description STRING, _dq_status STRING,
 _quarantine_datetime_utc TIMESTAMP, _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;

-- Generic payload preserves original quarantined attributes for remediation.
CREATE TABLE IF NOT EXISTS workspace.gold.quarantined_customers (
 source_row_id STRING, source_file_name STRING, quarantine_datetime_utc TIMESTAMP,
 dq_status STRING, dq_error_code STRING, dq_error_description STRING, source_payload_json STRING,
 _insert_datetime_utc TIMESTAMP, _update_datetime_utc TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.gold.quarantined_products
 LIKE workspace.gold.quarantined_customers;
CREATE TABLE IF NOT EXISTS workspace.gold.quarantined_stores
 LIKE workspace.gold.quarantined_customers;
CREATE TABLE IF NOT EXISTS workspace.gold.quarantined_invoices
 LIKE workspace.gold.quarantined_customers;
CREATE TABLE IF NOT EXISTS workspace.gold.quarantined_invoice_items
 LIKE workspace.gold.quarantined_customers;
