# 11 — Gold Aggregation Design (Approved Grain Revision)

## 1. Objective and boundaries

This design defines exactly one commercial Gold aggregate per commercial dashboard. The Data Quality dashboard retains its two existing aggregates and their existing grains.

No changes are made to RAW, BRONZE, SILVER, Gold dimensions or Gold atomic facts. The aggregates are rebuilt from `fact_invoice` and `fact_invoice_item` using already-resolved Gold surrogate keys. Power BI uses one designated commercial aggregate plus shared dimensions for each commercial dashboard.

## 2. Final aggregate inventory

| Dashboard | Table | Exact grain |
|---|---|---|
| Executive Performance | `agg_executive_performance_daily` | `date_key × store_key × subcategory_key` |
| Product Performance | `agg_product_performance_monthly` | `month_date_key × store_key × product_key` |
| Store & Regional Performance | `agg_store_performance_daily` | `date_key × store_key` |
| Data Quality | `agg_data_quality_daily` | Existing issue-date × source × file × rule × severity × status × geography grain |
| Data Quality | `agg_data_quality_record_daily` | Existing evaluation-date × source × file × geography grain |

There are three commercial aggregates, not four. The two DQ aggregates are preserved.

## 3. Executive Performance aggregate

### 3.1 Exact grain

`date_key × store_key × subcategory_key`

This is the approved grain. Loyalty Tier / Customer Segment is deliberately excluded. Do not substitute or add `product_key`, or add customer attributes to this grain.

### 3.2 Measures

- `revenue DECIMAL(20,2)` — sum of net invoice-line revenue.
- `units_sold BIGINT` — sum of line quantity.
- `gross_margin DECIMAL(20,2)` — net revenue less line-level product cost.
- `_last_updated_utc TIMESTAMP` — aggregate build timestamp.

### 3.3 KPI calculations

- Total Revenue = `SUM(revenue)`.
- Units Sold = `SUM(units_sold)`.
- Gross Margin % = `SUM(gross_margin) / SUM(revenue)`.
- Revenue Growth = selected-period revenue compared with the immediately preceding equivalent-length period. Calculate from additive revenue, not from stored row percentages.

These are all supported directly by the approved grain. Total Invoices and Average Invoice Value are explicitly excluded from the Executive dashboard because exact distinct invoice counting cannot be derived additively at this grain.

### 3.4 Visual coverage

The reviewed Executive mockup (5 November 2026) contains:
1. Revenue Trend — daily current period versus previous equivalent-length period.
2. Revenue by Product Category — group through `dim_subcategory → dim_category`.
3. Revenue by Region — group through `dim_store → dim_geography`.
4. Top 10 Stores by Revenue — group by store.

No product-level visual is supported or required in this reviewed mockup. Do not add Top 10 Products, product-level scatter or product-level profitability visuals to this Executive dashboard.

### 3.5 Filter and dimension behavior

- Date uses `dim_date.date_key`.
- Store and geography use `dim_store`, with geography reached through its existing relationship to `dim_geography`.
- Category and Subcategory use `dim_subcategory` and `dim_category`.
- Customer and Loyalty Tier attributes are not carried in this aggregate. The Executive dashboard has no Customer Segment / Loyalty Tier slicer; supporting it would require a different aggregate grain.

## 4. Product Performance aggregate

### 4.1 Exact grain

`month_date_key × store_key × product_key`

`month_date_key` is the first calendar day of the month encoded as YYYYMMDD and is a valid FK to `dim_date.date_key`. For example, February 2026 is `20260201`. Never use `ROUND(date_key / 100)`. Derive month start using calendar date functions (`trunc(full_date, 'month')`) and format as `yyyyMMdd`.

### 4.2 Measures

- `revenue DECIMAL(20,2)`
- `units_sold BIGINT`
- `gross_margin DECIMAL(20,2)`
- `_last_updated_utc TIMESTAMP`

### 4.3 Visual coverage

Supports the Product mockup:
- Monthly Revenue Trend by Product Category.
- Revenue by Region and Category.
- Top 10 Products by Revenue.
- Revenue vs. Gross Margin (one point per product in the selected context; bubble size uses Units Sold).
- Top 10 Subcategories by Revenue.
- Top 5 Products by Store.
- Selected Product Revenue by Store.

Category and Subcategory are reached through `dim_product`; geography through `dim_store → dim_geography`.

### 4.4 Filters

Month/date range, Region, State, Store, Category, Subcategory and Product. Loyalty Tier is not part of this mockup and is not part of this aggregate grain.

## 5. Store & Regional Performance aggregate

### 5.1 Exact grain

`date_key × store_key`

### 5.2 Measures

- `revenue DECIMAL(20,2)`
- `units_sold BIGINT`
- `gross_margin DECIMAL(20,2)`
- `invoice_count BIGINT` — distinct invoice headers at Date × Store, computed separately from invoice lines.
- `_last_updated_utc TIMESTAMP`

### 5.3 Visual coverage

Supports the Store & Regional mockup:
- Revenue by State.
- Monthly Revenue by Region.
- Top 10 Stores by Revenue.
- Bottom 10 Stores by Revenue.
- Total Revenue, Active Stores, Revenue per Store and Revenue Growth KPIs.

Region, State and City are obtained through the existing Store-to-Geography dimension relationship. Active Stores is calculated from the current active store population in `dim_store`, not merely stores with sales in the selected period.

## 6. Data Quality aggregates — preserved

### 6.1 `agg_data_quality_daily`

Preserve the existing grain:
`dq_date_key × source_table × source_file_name × dq_rule_key × severity × dq_status × geography_key`.

Preserve `dq_issue_count` (issue instances) and `affected_record_count` (distinct affected source records within that complete grain), plus the existing metadata. Do not sum affected-record counts across different rules to calculate globally distinct affected records.

### 6.2 `agg_data_quality_record_daily`

Preserve the existing grain:
`evaluation_date_key × source_table × source_file_name × geography_key`.

Preserve `total_records`, `valid_records`, `incorrect_records` and existing metadata. Enforce `total_records = valid_records + incorrect_records`. The existing per-file snapshot behavior is retained. The Data Quality dashboard specification and remediation grid are not redesigned by this change.

The DQ remediation grid continues to use `fact_data_quality_issue` and relevant `quarantined_*` tables.

## 7. Shared calculation and join rules

Build commercial aggregates from `fact_invoice` (header context) joined to `fact_invoice_item` (line measures), then join Gold dimensions using their resolved surrogate keys. Never join facts to all historical SCD2 dimension versions by natural key alone.

```text
line_revenue = quantity * unit_price - COALESCE(discount, 0)
line_cost = quantity * COALESCE(dim_product.cost, 0)
line_gross_margin = line_revenue - line_cost
```

The Executive aggregate maps `product_key` from the line to `dim_product.subcategory_key` and then groups by the approved subcategory grain. Product key is not retained in the Executive aggregate.

Use invoice header `date_key` and `store_key`. Customer attributes are not needed for the approved commercial aggregate grains. `customer_key = -1` remains the valid generic “No customer assigned” member in the Gold dimensional model, but it does not affect these aggregates. Store and product keys of -1 for populated source identifiers must fail validation; do not silently aggregate unresolved references.

## 8. Cardinality and performance profile

The user's SQL profiles supplied these baselines:
- Product: approximately 4.5M distinct Month × Store × Product combinations.
- Store: approximately 54.8K distinct Date × Store combinations.
- Executive: approximately 493.2K Date × Store × Subcategory combinations. The approved grain has no Loyalty Tier expansion.

These are profiling baselines, not hard-coded expected row counts. The loader must print actual row counts and elapsed time for each commercial aggregate. Compare actual cardinality to these baselines and investigate unexpected growth.

Do not partition by high-cardinality product/store keys by default. Choose clustering/optimization only after measuring data size and query patterns.

## 9. Refresh strategy

The initial implementation is a deterministic full rebuild. Each aggregate is computed and checked for duplicate grain before it is written. The job must be single-writer (`max concurrent runs = 1`). Only the five aggregate output tables are overwritten; Gold facts and dimensions are read-only inputs. The existing date-dimension coverage helper used by the DQ process is retained from the current pipeline so DQ issue and snapshot dates can join to `dim_date`.

Do not introduce incremental loading until full-rebuild outputs reconcile. A future incremental strategy must handle inserts, updates, deletes, late-arriving facts and SCD2 corrections, recompute all affected groups, and commit watermarks only after validation.

## 10. Reconciliation and acceptance

`05_reconciliation.py` remains a manual, read-only script. It must retain the existing base fact row-count checks, natural-grain uniqueness, SCD2 interval checks, populated natural-key-to-surrogate-key protections, temporal dimension resolution, date FK coverage and DQ reconciliation.

Required aggregate comparisons:
1. Executive: reconstruct Date × Store × Subcategory from Gold facts and compare Revenue, Units Sold and Gross Margin with a full outer key comparison.
2. Product: reconstruct Month-start Date × Store × Product and compare Revenue, Units Sold and Gross Margin.
3. Store: reconstruct Date × Store measures and compare distinct invoice-header count separately.
4. All five aggregate grains are unique.
5. DQ issue instances reconcile to `fact_data_quality_issue`.
6. DQ record snapshots satisfy total = valid + incorrect and reconcile to Silver clean + quarantine populations.
7. `month_date_key` exists in `dim_date`.
8. No populated Silver customer/store/product natural key is silently mapped to -1.

## 11. Power BI model acceptance

Each commercial report must use only its one designated aggregate plus shared dimensions. The Executive dashboard must not query invoice-level data for any KPI. Verify every visual and slicer using Power BI Performance Analyzer and Databricks query history. Do not declare the no-fact-scan requirement achieved until query evidence confirms it.
