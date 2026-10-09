-- M8 Gold aggregate serving layer.
CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_sales_daily (
 date_key INT, store_key BIGINT, loyalty_tier STRING, payment_method STRING,
 invoice_count BIGINT, revenue DECIMAL(20,2), units_sold BIGINT, gross_margin DECIMAL(20,2),
 _last_updated_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_sales_product_daily (
 date_key INT, product_key BIGINT, units_sold BIGINT, revenue DECIMAL(20,2),
 gross_margin DECIMAL(20,2), _last_updated_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_sales_product_store_daily (
 date_key INT, product_key BIGINT, store_key BIGINT, loyalty_tier STRING, payment_method STRING,
 units_sold BIGINT, revenue DECIMAL(20,2), gross_margin DECIMAL(20,2),
 _last_updated_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_data_quality_daily (
 dq_date_key INT, source_table STRING, source_file_name STRING, dq_rule_key BIGINT,
 severity STRING, dq_status STRING, geography_key BIGINT, dq_issue_count BIGINT,
 affected_record_count BIGINT, _last_updated_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_data_quality_record_daily (
 evaluation_date_key INT, source_table STRING, source_file_name STRING, geography_key BIGINT,
 total_records BIGINT, valid_records BIGINT, incorrect_records BIGINT,
 _last_updated_utc TIMESTAMP
) USING DELTA;
