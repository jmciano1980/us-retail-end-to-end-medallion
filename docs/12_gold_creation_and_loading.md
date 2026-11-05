# 12 — Gold Creation and Loading (Approved Dashboard Aggregations)

## 1. Scope and non-negotiable boundaries

This release changes only the Gold aggregation layer and BI documentation.

- RAW, BRONZE and SILVER are unchanged.
- Gold dimensions and atomic facts are unchanged and remain read-only inputs to the aggregate build.
- The three commercial dashboards each have exactly one dedicated commercial aggregate.
- Both existing Data Quality aggregates and their grains/semantics are preserved.
- The revised Executive mockup attached on 5 November 2026 supersedes the previous Executive mockup.

## 2. Final aggregate inventory

| Table | Exact grain | Dashboard |
|---|---|---|
| `agg_executive_performance_daily` | `date_key × store_key × subcategory_key` | Executive |
| `agg_product_performance_monthly` | `month_date_key × store_key × product_key` | Product |
| `agg_store_performance_daily` | `date_key × store_key` | Store & Regional |
| `agg_data_quality_daily` | Existing DQ issue grain | Data Quality |
| `agg_data_quality_record_daily` | Existing DQ record snapshot grain | Data Quality |

The Executive grain is exactly Date × Store × Subcategory. Loyalty Tier / Customer Segment is intentionally excluded. Do not add `product_key`, replace `subcategory_key`, or introduce customer attributes into this aggregate.

The Product `month_date_key` is the first day of the calendar month as YYYYMMDD (e.g. `20260201`) and must reference `dim_date.date_key`. Do not use `ROUND(date_key / 100)`.

## 3. Files and execution order

| Order | File | Purpose |
|---:|---|---|
| 1 | `05_databricks/gold/01_create_gold_schema_tables.sql` | Existing Gold dimensions, facts and DQ detail tables — unchanged |
| 2 | `05_databricks/gold/02_create_gold_agg_tables.sql` | Create the three commercial aggregates and preserve both DQ aggregates |
| 3 | `05_databricks/gold/03_load_gold_schema_tables.py` | Existing Gold base loader — unchanged; do not rerun solely for this change |
| 4 | `05_databricks/gold/04_load_gold_agg_tables.py` | Full rebuild of three commercial and two DQ aggregates |
| 5 | `05_databricks/gold/05_reconciliation.py` | Manual, read-only reconciliation of base model and all aggregates |

## 4. DDL deployment

Run `02_create_gold_agg_tables.sql` after the existing Gold base schema is present.

The script uses `CREATE TABLE IF NOT EXISTS`; this does not alter an existing table's schema. The revised commercial table names are new names. The existing DQ aggregate table definitions are retained.

Do not drop or truncate Gold facts, dimensions, DQ issue facts or quarantine tables. Existing retired commercial aggregate tables may remain temporarily; do not drop them until the revised outputs pass reconciliation and Power BI no longer references the retired tables.


### 4.1 One-time schema migration if the earlier draft was already deployed

An earlier draft used `product_key` in `agg_executive_performance_daily`. The approved schema now uses `subcategory_key`. Because `CREATE TABLE IF NOT EXISTS` does not change an existing table, inspect the table before running the loader:

```sql
DESCRIBE TABLE workspace.gold.agg_executive_performance_daily;
```

If its schema still contains the retired `loyalty_tier` column, or its grain differs from Date × Store × Subcategory, perform a controlled one-time replacement of **this aggregate table only** (the aggregate is reproducible from the unchanged facts and dimensions):

```sql
DROP TABLE workspace.gold.agg_executive_performance_daily;
```

Then rerun `02_create_gold_agg_tables.sql` and the aggregate loader. Do not drop or truncate any fact, dimension, Data Quality issue, quarantine or Silver table. If the table already has the approved `subcategory_key` schema, do not drop it.

## 5. Loader behavior — `04_load_gold_agg_tables.py`

The script is a Databricks Python script task and a manually executable script in the supported Spark environment. It is not a notebook cell intended to receive injected kernel arguments.

The loader:

1. Sets Spark session time zone to UTC.
2. Performs base fact safeguards: no unresolved store/product surrogate keys and unique invoice/invoice-item natural grains.
3. Retains the existing `dim_date` coverage helper required by the current DQ aggregation process. It appends missing calendar dates needed for DQ issue dates and the current DQ evaluation date; it does not rebuild or overwrite existing date rows.
4. Builds one invoice header context per `invoice_key`, including date and store. Customer attributes are not used by the commercial aggregates.
5. Joins invoice lines to `dim_product` by the resolved `product_key` to obtain cost and subcategory.
6. Calculates line revenue, cost and gross margin once.
7. Builds Executive aggregate at exactly Date × Store × Subcategory.
8. Builds Product aggregate at Month-start Date × Store × Product.
9. Builds Store aggregate at Date × Store. `invoice_count` is calculated from distinct invoice headers, separately from invoice lines.
10. Rebuilds the two existing DQ aggregates without changing their grains or intended measures.
11. Checks duplicate aggregate grains before each write and prints row counts.

The full rebuild is deliberate for deterministic correctness. Only the five aggregate outputs are overwritten. Facts, dimensions, Silver tables and quarantine tables are not overwritten. The job must be single-writer; set maximum concurrent runs to 1.

### 5.1 Commercial measures

```text
line_revenue = quantity * unit_price - COALESCE(discount, 0)
line_cost = quantity * COALESCE(dim_product.cost, 0)
line_gross_margin = line_revenue - line_cost
```

### 5.2 Executive KPI calculations

| KPI | Calculation |
|---|---|
| Total Revenue | `SUM(revenue)` |
| Units Sold | `SUM(units_sold)` |
| Gross Margin % | `SUM(gross_margin) / SUM(revenue)` |
| Revenue Growth | Selected-period revenue versus immediately preceding equivalent-length period |

Total Invoices and Average Invoice Value have been removed from the Executive mockup and requirements. They must not be reintroduced as KPIs from this aggregate grain.

The approved Executive grain supports the reviewed visuals: daily revenue trend, revenue by category, revenue by region and Top 10 Stores by Revenue. It does not support product-level visuals.

### 5.3 Ratio and comparison rules

- Average Selling Price, where required on the Product dashboard, is `SUM(revenue) / SUM(units_sold)`.
- Gross Margin % is `SUM(gross_margin) / SUM(revenue)`.
- Revenue Growth compares sums of revenue over equivalent date windows.
- Handle zero denominators and missing comparison periods explicitly.
- Never average precomputed row percentages or averages.

## 6. Power BI relationships and source restrictions

- Executive: `date_key → dim_date`, `store_key → dim_store`, `subcategory_key → dim_subcategory`. Category is reached through `dim_subcategory → dim_category`. No Loyalty Tier / Customer Segment filter is supported.
- Product: `month_date_key → dim_date`, `store_key → dim_store`, `product_key → dim_product`.
- Store & Regional: `date_key → dim_date`, `store_key → dim_store`.
- Data Quality: existing DQ aggregate relationships and issue/quarantine detail sources.

Geography is reached through the existing Store-to-Geography relationship. Do not create direct Geography-to-fact relationships. Do not relate a commercial aggregate to atomic facts.

Each commercial dashboard must query only its assigned commercial aggregate plus shared dimensions. The Executive dashboard must not query invoice-level data for its KPIs or visuals.

## 7. Databricks deployment and execution

Upload the revised SQL and Python files to the existing Gold workspace folder, for example `/workspace/retail/gold`.

Example Databricks CLI imports (verify syntax against the installed CLI version with `--help`):

```bash
databricks workspace import /workspace/retail/gold/02_create_gold_agg_tables.sql \
  --file 05_databricks/gold/02_create_gold_agg_tables.sql \
  --format AUTO --language SQL --profile retail-dev

databricks workspace import /workspace/retail/gold/04_load_gold_agg_tables.py \
  --file 05_databricks/gold/04_load_gold_agg_tables.py \
  --format SOURCE --language PYTHON --profile retail-dev

databricks workspace import /workspace/retail/gold/05_reconciliation.py \
  --file 05_databricks/gold/05_reconciliation.py \
  --format SOURCE --language PYTHON --profile retail-dev
```

Execution sequence:

1. Execute the revised aggregate DDL once.
2. Do not rerun `03_load_gold_schema_tables.py` solely because aggregate definitions changed.
3. Run `04_load_gold_agg_tables.py` as a single-writer task (maximum concurrent runs = 1).
4. Review completion status, aggregate row counts and any warnings.
5. Run `05_reconciliation.py` manually after the aggregate build. It is read-only and must not be configured to mutate data.
6. Proceed to Power BI only after all reconciliation checks pass.

## 8. Cardinality baselines

The user-provided SQL profiles are baselines, not fixed expected counts:

| Aggregate | Baseline |
|---|---:|
| Product: Month × Store × Product | ~4,500,000 |
| Store: Date × Store | ~54,800 |
| Executive: Date × Store × Subcategory | ~493,200 |

The final Executive grain excludes Loyalty Tier. The loader must print actual row counts. Record counts and elapsed run time during acceptance; investigate unexpected growth rather than changing the approved grain to force a target count.

## 9. Reconciliation — `05_reconciliation.py`

The script is manual and read-only. It does not insert, update, delete, merge or overwrite any table.

It retains the base-model safeguards:
- Silver invoice and invoice-item counts versus Gold facts.
- Invoice and invoice-item natural-grain uniqueness.
- SCD2 interval validity, one current row per natural key and no overlapping intervals.
- Populated Silver customer/store/product identifiers must not map silently to surrogate key `-1`.
- Temporal SCD2 natural-key resolution at transaction date.
- Gold fact date-key referential integrity.
- Data Quality issue count and DQ record snapshot checks.

It reconciles the commercial aggregates by full outer comparison at the exact grain:
- Executive: Date × Store × Subcategory; Revenue, Units Sold and Gross Margin.
- Product: Month-start Date × Store × Product; Revenue, Units Sold and Gross Margin.
- Store: Date × Store; Revenue, Units Sold, Gross Margin and distinct invoice count.

It also verifies uniqueness of all five aggregate grains, the Product month-start date FK, DQ issue totals, DQ record arithmetic (`total_records = valid_records + incorrect_records`) and the DQ snapshot population against Silver clean plus quarantine records.

A passing reconciliation is necessary but not sufficient for Power BI acceptance. It does not prove that Power BI is using the intended aggregate.

## 10. Power BI acceptance checklist

1. Validate every visual and slicer in the three approved mockups.
2. Confirm the Executive KPI cards are exactly Total Revenue, Units Sold, Gross Margin % and Revenue Growth.
3. Confirm the Executive Top 10 Stores visual and the other reviewed Executive visuals work from its single aggregate.
4. Confirm the Product month slicer resolves to first-of-month date keys in `dim_date`.
5. Compare displayed totals to reconciliation outputs under identical filters.
6. Use Power BI Performance Analyzer and Databricks query history to verify routine commercial report interactions read their assigned aggregate and dimensions, not `fact_invoice` or `fact_invoice_item`.
7. Confirm the Data Quality dashboard continues to use the preserved DQ aggregates and record-level remediation sources.

Do not declare the no-fact-scan objective achieved until query evidence confirms it.
