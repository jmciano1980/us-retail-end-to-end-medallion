-- Retail Raw - LANDING ZONE - DIRTY ALLOWED
-- This schema is intentionally permissive to simulate real-world source errors.
-- No FKs, no CHECKs, all business columns nullable.
-- Cleaning happens later in Databricks Silver.

CREATE SCHEMA IF NOT EXISTS retail_raw;

-- Drop old clean tables if they exist
DROP TABLE IF EXISTS retail_raw.invoice_items;
DROP TABLE IF EXISTS retail_raw.invoices;
DROP TABLE IF EXISTS retail_raw.customers;
DROP TABLE IF EXISTS retail_raw.products;
DROP TABLE IF EXISTS retail_raw.stores;

CREATE TABLE retail_raw.stores (
    raw_id BIGSERIAL PRIMARY KEY,
    store_id INT, -- nullable, duplicate allowed
    store_name VARCHAR(150),
    region VARCHAR(50), -- will contain invalid values like 'Unknown'
    state VARCHAR(10),
    city VARCHAR(100),
    zip_code VARCHAR(20),
    manager_name VARCHAR(150),
    opened_date DATE,
    square_footage VARCHAR(50), -- intentionally varchar to allow 'abc' error
    ingestion_ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE retail_raw.products (
    raw_id BIGSERIAL PRIMARY KEY,
    product_id INT,
    sku VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price VARCHAR(50), -- varchar to allow '$-10' or null as string error
    cost VARCHAR(50),
    is_active VARCHAR(20),
    ingestion_ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE retail_raw.customers (
    raw_id BIGSERIAL PRIMARY KEY,
    customer_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    loyalty_tier VARCHAR(50),
    join_date VARCHAR(50), -- varchar to allow '2025-13-40' invalid date error
    ingestion_ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE retail_raw.invoices (
    raw_id BIGSERIAL PRIMARY KEY,
    invoice_id BIGINT,
    store_id INT, -- no FK, allows 99999 invalid
    customer_id INT, -- allows invalid
    invoice_date VARCHAR(50), -- allows invalid timestamp
    payment_method VARCHAR(50), -- allows 'Bitcoin' invalid
    subtotal VARCHAR(50),
    tax_rate VARCHAR(50),
    tax_amount VARCHAR(50),
    total_amount VARCHAR(50),
    ingestion_ts TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_raw_invoices_invoice_id ON retail_raw.invoices(invoice_id);

CREATE TABLE retail_raw.invoice_items (
    raw_id BIGSERIAL PRIMARY KEY,
    invoice_item_id BIGINT,
    invoice_id BIGINT, -- no FK, orphan items allowed
    product_id INT, -- invalid product allowed
    quantity VARCHAR(50), -- allows 0, negative, null, 'two'
    unit_price VARCHAR(50),
    discount_pct VARCHAR(50),
    line_total VARCHAR(50),
    ingestion_ts TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_raw_items_invoice_id ON retail_raw.invoice_items(invoice_id);
