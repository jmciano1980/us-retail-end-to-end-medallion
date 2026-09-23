-- Databricks notebook source
-- MAGIC %md
-- MAGIC # M4 - Bronze DDL - Manual Execution (Once)
-- MAGIC **Catalog:** `workspace` | **Schema:** `retail` | **Volume:** `raw_postgres_export`
-- MAGIC **Location:** `/Volumes/workspace/retail/raw_postgres_export/`
-- MAGIC **Run Mode:** Manual - executed once by Data Engineer to create empty Bronze Delta tables

-- MAGIC %md
-- MAGIC ## 1. Create Schema and Volume

CREATE SCHEMA IF NOT EXISTS workspace.retail
COMMENT 'Retail Medallion - Bronze/Silver/Gold';

-- Volume must already exist from 02_data_export, but ensure it exists
-- Note: VOLUME TYPE must be MANAGED - use enum in SDK, not string
CREATE VOLUME IF NOT EXISTS workspace.retail.raw_postgres_export;

-- MAGIC %md
-- MAGIC ## 2. Control Table - File Tracking for Incremental Load
-- MAGIC Tracks which parquet files have been loaded to avoid reprocessing

CREATE TABLE IF NOT EXISTS workspace.retail.bronze_file_log (
  file_name STRING COMMENT 'Parquet file name',
  file_path STRING COMMENT 'Full volume path',
  table_name STRING COMMENT 'Target bronze table',
  status STRING COMMENT 'LOADED, FAILED, SKIPPED',
  row_count BIGINT,
  file_size_bytes BIGINT,
  first_seen_at TIMESTAMP,
  last_processed_at TIMESTAMP,
  load_duration_seconds DOUBLE,
  job_run_id STRING,
  error_message STRING
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- MAGIC %md
-- MAGIC ## 3. Bronze Tables DDL - As-Is + Metadata
-- MAGIC Requirement: Data loaded as-is, nulls kept, no transforms. Metadata added.
-- MAGIC All original columns as STRING (permissive) to keep 1% dirty rows for Silver cleaning.

-- 3.1 bronze_stores
CREATE TABLE IF NOT EXISTS workspace.retail.bronze_stores (
  store_id STRING,
  store_name STRING,
  city STRING,
  state STRING,
  region STRING,
  zip_code STRING,
  country STRING,
  store_type STRING,
  open_date STRING,
  -- Metadata - Required
  _source_file_name STRING COMMENT 'Source parquet file',
  _source_file_path STRING COMMENT 'Full volume path',
  _ingest_timestamp TIMESTAMP COMMENT 'When record was inserted into Bronze',
  _update_timestamp TIMESTAMP COMMENT 'Last update - same as ingest for Bronze',
  _bronze_load_id STRING COMMENT 'Job run ID for traceability'
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.columnMapping.mode' = 'name'
);

-- 3.2 bronze_products
CREATE TABLE IF NOT EXISTS workspace.retail.bronze_products (
  product_id STRING,
  product_name STRING,
  category STRING,
  subcategory STRING,
  brand STRING,
  unit_price STRING,
  cost STRING,
  supplier_id STRING,
  weight STRING,
  product_status STRING,
  created_date STRING,
  _source_file_name STRING,
  _source_file_path STRING,
  _ingest_timestamp TIMESTAMP,
  _update_timestamp TIMESTAMP,
  _bronze_load_id STRING
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.columnMapping.mode' = 'name'
);

-- 3.3 bronze_customers
CREATE TABLE IF NOT EXISTS workspace.retail.bronze_customers (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  country STRING,
  segment STRING,
  registration_date STRING,
  _source_file_name STRING,
  _source_file_path STRING,
  _ingest_timestamp TIMESTAMP,
  _update_timestamp TIMESTAMP,
  _bronze_load_id STRING
) USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- 3.4 bronze_invoices
CREATE TABLE IF NOT EXISTS workspace.retail.bronze_invoices (
  invoice_id STRING,
  store_id STRING,
  customer_id STRING,
  invoice_date STRING,
  total_amount STRING,
  discount_amount STRING,
  tax_amount STRING,
  payment_method STRING,
  status STRING,
  created_at STRING,
  _source_file_name STRING,
  _source_file_path STRING,
  _ingest_timestamp TIMESTAMP,
  _update_timestamp TIMESTAMP,
  _bronze_load_id STRING
) USING DELTA
PARTITIONED BY (invoice_date)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- 3.5 bronze_invoice_items
CREATE TABLE IF NOT EXISTS workspace.retail.bronze_invoice_items (
  invoice_item_id STRING,
  invoice_id STRING,
  product_id STRING,
  quantity STRING,
  unit_price STRING,
  line_total STRING,
  discount STRING,
  _source_file_name STRING,
  _source_file_path STRING,
  _ingest_timestamp TIMESTAMP,
  _update_timestamp TIMESTAMP,
  _bronze_load_id STRING
) USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- MAGIC %md
-- MAGIC ## 4. Verify DDL

LIST '/Volumes/workspace/retail/raw_postgres_export/';
SHOW TABLES IN workspace.retail;
DESCRIBE TABLE EXTENDED workspace.retail.bronze_file_log;
