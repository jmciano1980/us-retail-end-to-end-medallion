-- Databricks notebook source
-- MAGIC %md
-- MAGIC # M4 - Bronze Control Queries - Verify Load

-- MAGIC %md
-- MAGIC ## 1. Check Volume Files vs Control Log

-- Files in Volume
LIST '/Volumes/workspace/retail/raw_postgres_export/';

-- Files tracked in control table
SELECT 
  table_name,
  COUNT(*) as files_loaded,
  SUM(row_count) as total_rows,
  SUM(file_size_bytes) as total_bytes,
  MIN(first_seen_at) as first_load,
  MAX(last_processed_at) as last_load
FROM workspace.retail.bronze_file_log
WHERE status = 'LOADED'
GROUP BY table_name
ORDER BY table_name;

-- Files not yet loaded (if any)
-- Compare LIST output manually with bronze_file_log

-- MAGIC %md
-- MAGIC ## 2. Row Counts - Bronze vs Expected

SELECT 'bronze_stores' as table_name, COUNT(*) as row_count, COUNT(DISTINCT store_id) as distinct_keys FROM workspace.retail.bronze_stores
UNION ALL
SELECT 'bronze_products', COUNT(*), COUNT(DISTINCT product_id) FROM workspace.retail.bronze_products
UNION ALL
SELECT 'bronze_customers', COUNT(*), COUNT(DISTINCT customer_id) FROM workspace.retail.bronze_customers
UNION ALL
SELECT 'bronze_invoices', COUNT(*), COUNT(DISTINCT invoice_id) FROM workspace.retail.bronze_invoices
UNION ALL
SELECT 'bronze_invoice_items', COUNT(*), COUNT(DISTINCT invoice_item_id) FROM workspace.retail.bronze_invoice_items;

-- Expected from M3: 50 / 2500 / 100k / 55M / ~200M
-- Your current run: 3 files / 7 MB means stores/products/customers only

-- MAGIC %md
-- MAGIC ## 3. Metadata Columns Verification - Required

SELECT _source_file_name, _ingest_timestamp, _update_timestamp, _bronze_load_id, COUNT(*) 
FROM workspace.retail.bronze_stores 
GROUP BY _source_file_name, _ingest_timestamp, _update_timestamp, _bronze_load_id
LIMIT 10;

-- Check metadata not null
SELECT 
  'bronze_stores' as tbl,
  SUM(CASE WHEN _source_file_name IS NULL THEN 1 ELSE 0 END) as null_source,
  SUM(CASE WHEN _ingest_timestamp IS NULL THEN 1 ELSE 0 END) as null_ingest,
  MIN(_ingest_timestamp) as first_ingest,
  MAX(_ingest_timestamp) as last_ingest
FROM workspace.retail.bronze_stores;

-- MAGIC %md
-- MAGIC ## 4. As-Is Check - Nulls and Dirty Data Kept

-- Should have nulls if source had nulls - Bronze must NOT clean
SELECT * FROM workspace.retail.bronze_customers WHERE customer_id IS NULL LIMIT 10;
SELECT * FROM workspace.retail.bronze_invoices WHERE total_amount IS NULL OR total_amount = '' LIMIT 10;

-- MAGIC %md
-- MAGIC ## 5. Duplicates Check - Allowed in Bronze

-- Duplicates are allowed per requirement: new files loaded even if duplicate rows
SELECT invoice_id, COUNT(*) as dup_count
FROM workspace.retail.bronze_invoices
GROUP BY invoice_id
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 20;

-- MAGIC %md
-- MAGIC ## 6. Incremental Load Check - File Already Loaded = Skip

SELECT file_name, file_path, table_name, status, row_count, last_processed_at
FROM workspace.retail.bronze_file_log
ORDER BY last_processed_at DESC;

-- Try re-running 05_bronze_loading_process.py - should SKIP all files
-- If new file added to volume, next run should load only that file

-- MAGIC %md
-- MAGIC ## 7. Delta Table Details

DESCRIBE DETAIL workspace.retail.bronze_stores;
DESCRIBE DETAIL workspace.retail.bronze_customers;
DESCRIBE HISTORY workspace.retail.bronze_invoices LIMIT 10;
