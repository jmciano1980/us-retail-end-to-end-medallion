-- M8 Gold aggregate serving layer — approved dashboard grains.
-- Facts and dimensions are read-only inputs and are not modified here.
CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_executive_performance_daily (
    date_key INT NOT NULL,
    store_key BIGINT NOT NULL,
    subcategory_key BIGINT NOT NULL,
    revenue DECIMAL(20,2),
    units_sold BIGINT,
    gross_margin DECIMAL(20,2),
    _last_updated_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_product_performance_monthly (
    month_date_key INT NOT NULL COMMENT 'First day of month as YYYYMMDD; FK to dim_date.date_key',
    store_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    revenue DECIMAL(20,2),
    units_sold BIGINT,
    gross_margin DECIMAL(20,2),
    _last_updated_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_store_performance_daily (
    date_key INT NOT NULL,
    store_key BIGINT NOT NULL,
    revenue DECIMAL(20,2),
    units_sold BIGINT,
    gross_margin DECIMAL(20,2),
    invoice_count BIGINT,
    _last_updated_utc TIMESTAMP
) USING DELTA;

-- Data Quality definitions and grains are preserved.
CREATE TABLE IF NOT EXISTS workspace.gold.agg_data_quality_daily (
    dq_date_key INT,
    source_table STRING,
    source_file_name STRING,
    dq_rule_key BIGINT,
    severity STRING,
    dq_status STRING,
    geography_key BIGINT,
    dq_issue_count BIGINT,
    affected_record_count BIGINT,
    _last_updated_utc TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.agg_data_quality_record_daily (
    evaluation_date_key INT,
    source_table STRING,
    source_file_name STRING,
    geography_key BIGINT,
    total_records BIGINT,
    valid_records BIGINT,
    incorrect_records BIGINT,
    _last_updated_utc TIMESTAMP
) USING DELTA;
