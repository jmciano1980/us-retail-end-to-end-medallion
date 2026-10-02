-- M6 — Bronze table creation
-- Target catalog/schema: workspace.bronze
--
-- Bronze preserves the source payload exactly as delivered by M4/M5.
-- Business columns keep the exact PostgreSQL field names.
-- PostgreSQL TEXT columns are represented as STRING in Delta.
-- No cleansing, casting, filtering, validation, or business deduplication occurs here.
--
-- Technical metadata:
--   _insert_datetime_utc : timestamp when this physical source row first enters Bronze.
--   _update_datetime_utc : timestamp of the latest technical upsert of this source row.
--   _source_file_name    : exact Parquet filename that supplied the row.
--   _source_row_id       : technical identity of the physical source row; NOT a business key.
--
-- _source_row_id is deliberately required because business keys may contain duplicates or
-- invalid values. Bronze must retain every source row, including duplicate business keys.

CREATE SCHEMA IF NOT EXISTS workspace.bronze;

CREATE TABLE IF NOT EXISTS workspace.bronze.stores (
    store_id STRING,
    store_name STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    region STRING,
    manager_name STRING,
    opened_date STRING,
    square_footage STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.bronze.products (
    product_id STRING,
    sku STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    unit_price STRING,
    cost STRING,
    is_active STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.bronze.customers (
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
    join_date STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.bronze.invoices (
    invoice_id STRING,
    store_id STRING,
    customer_id STRING,
    invoice_date STRING,
    payment_method STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.bronze.invoice_items (
    invoice_id STRING,
    line_item STRING,
    product_id STRING,
    quantity STRING,
    unit_price STRING,
    discount STRING,
    _insert_datetime_utc TIMESTAMP,
    _update_datetime_utc TIMESTAMP,
    _source_file_name STRING,
    _source_row_id STRING
)
USING DELTA;

-- Technical control table. It makes file ingestion incremental and prevents a successfully
-- ingested Parquet file from being loaded again on a subsequent Job execution.
CREATE TABLE IF NOT EXISTS workspace.bronze._bronze_file_ingestion_log (
    source_file_path STRING NOT NULL,
    source_file_name STRING NOT NULL,
    source_table STRING NOT NULL,
    processed_at_utc TIMESTAMP NOT NULL,
    status STRING NOT NULL
)
USING DELTA;
