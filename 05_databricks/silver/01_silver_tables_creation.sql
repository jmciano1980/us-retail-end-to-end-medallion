-- M7 — Silver table creation
-- Target catalog/schema: workspace.silver
--
-- Silver is the first business-data-quality layer.
-- Bronze preserves the source payload; Silver validates, types and quarantines it.
--
-- Every table contains:
--   _insert_datetime_utc
--   _update_datetime_utc
--   _source_file_name
--   _source_row_id

CREATE SCHEMA IF NOT EXISTS workspace.silver;

-- Silver technical control table
CREATE TABLE IF NOT EXISTS workspace.silver._silver_file_ingestion_log (
    source_file_path STRING NOT NULL,
    source_file_name STRING NOT NULL,
    source_table STRING NOT NULL,
    processed_at_utc TIMESTAMP NOT NULL,
    status STRING NOT NULL,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING
)
USING DELTA;

-- Clean Silver tables
CREATE TABLE IF NOT EXISTS workspace.silver.customers (
    customer_id STRING NOT NULL,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    email STRING,
    phone STRING,
    street_address STRING NOT NULL,
    city STRING NOT NULL,
    state STRING NOT NULL,
    zip_code STRING NOT NULL,
    loyalty_tier STRING,
    join_date DATE,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.products (
    product_id STRING NOT NULL,
    sku STRING NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    subcategory STRING NOT NULL,
    brand STRING NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    cost DECIMAL(18,2) NOT NULL,
    is_active BOOLEAN,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.stores (
    store_id STRING NOT NULL,
    store_name STRING NOT NULL,
    city STRING NOT NULL,
    state STRING NOT NULL,
    zip_code STRING NOT NULL,
    region STRING NOT NULL,
    manager_name STRING NOT NULL,
    opened_date DATE NOT NULL,
    square_footage BIGINT,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.invoices (
    invoice_id STRING NOT NULL,
    store_id STRING NOT NULL,
    -- NULL Customer_ID is valid for an anonymous/guest retail transaction.
    customer_id STRING,
    invoice_date DATE NOT NULL,
    payment_method STRING NOT NULL,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.invoice_items (
    invoice_id STRING NOT NULL,
    line_item BIGINT NOT NULL,
    product_id STRING NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    discount DECIMAL(18,2),
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL
)
USING DELTA;

-- Quarantine tables
CREATE TABLE IF NOT EXISTS workspace.silver.quarantined_customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    street_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    loyalty_tier STRING,
    join_date DATE,
    join_date_raw STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL,
    _quarantine_datetime_utc TIMESTAMP NOT NULL,
    _dq_status STRING NOT NULL,
    _dq_error_code STRING NOT NULL,
    _dq_error_description STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.quarantined_products (
    product_id STRING,
    sku STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    unit_price DECIMAL(18,2),
    unit_price_raw STRING,
    cost DECIMAL(18,2),
    cost_raw STRING,
    is_active BOOLEAN,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL,
    _quarantine_datetime_utc TIMESTAMP NOT NULL,
    _dq_status STRING NOT NULL,
    _dq_error_code STRING NOT NULL,
    _dq_error_description STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.quarantined_stores (
    store_id STRING,
    store_name STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    region STRING,
    manager_name STRING,
    opened_date DATE,
    opened_date_raw STRING,
    square_footage BIGINT,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL,
    _quarantine_datetime_utc TIMESTAMP NOT NULL,
    _dq_status STRING NOT NULL,
    _dq_error_code STRING NOT NULL,
    _dq_error_description STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.quarantined_invoices (
    invoice_id STRING,
    store_id STRING,
    customer_id STRING,
    invoice_date DATE,
    invoice_date_raw STRING,
    payment_method STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL,
    _quarantine_datetime_utc TIMESTAMP NOT NULL,
    _dq_status STRING NOT NULL,
    _dq_error_code STRING NOT NULL,
    _dq_error_description STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.quarantined_invoice_items (
    invoice_id STRING,
    line_item BIGINT,
    line_item_raw STRING,
    product_id STRING,
    quantity BIGINT,
    quantity_raw STRING,
    unit_price DECIMAL(18,2),
    unit_price_raw STRING,
    discount DECIMAL(18,2),
    discount_raw STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING NOT NULL,
    _quarantine_datetime_utc TIMESTAMP NOT NULL,
    _dq_status STRING NOT NULL,
    _dq_error_code STRING NOT NULL,
    _dq_error_description STRING NOT NULL
)
USING DELTA;
