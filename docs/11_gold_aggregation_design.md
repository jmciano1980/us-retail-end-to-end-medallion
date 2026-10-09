# 11 — Gold Aggregations Design

## 1. Purpose

This document defines the proposed Gold aggregation layer for the US Retail end-to-end medallion data engineering project.

The objective is to let Power BI serve the four dashboards defined in `10_bi_requirements_and_dashboard_design.md` from compact, daily-grain aggregate tables and dimensions, without routinely scanning the 55-million-row atomic sales facts.

The Gold atomic facts remain the authoritative analytical detail layer:

- `workspace.gold.fact_invoice`
- `workspace.gold.fact_invoice_item`

They remain available for reconciliation, investigation, and exceptional analytical use. They are **not intended to be the normal query source for the Executive, Store/Regional, or Product dashboards**.

The Data Quality dashboard has a deliberate exception: its remediation grid needs individual issue and quarantined-record details. That detail must come from the DQ issue and quarantine datasets, not from the 55M-row sales facts.

This is the initial design baseline. Before production acceptance, the implementation must pass the coverage, reconciliation, cardinality, refresh, and Power BI query-validation checks defined below.

## 2. Design principles

1. **Daily is the lowest summarized time grain.** Aggregates retain daily records so Power BI can support Year → Quarter → Month → Day analysis without scanning atomic sales facts.
2. **Design for dashboard requirements, not arbitrary combinations.** Each aggregate must support a documented set of KPIs, visuals, filters, and drill-downs.
3. **Preserve filter context.** A filter can affect an aggregate-backed visual only if the required dimensional context is preserved in its grain or through a valid relationship that does not require the atomic facts.
4. **Store additive measures.** Revenue, units, invoice counts (only where semantically valid), and margin amounts are additive at their defined grain. Average invoice value, growth rates, margin percentages, and quality percentages are calculated from additive components.
5. **Do not duplicate descriptive dimensions unnecessarily.** Use dimension keys and existing Gold dimensions for labels and hierarchies.
6. **Keep the atomic facts authoritative.** Aggregates are derived data products and must reconcile to the facts.
7. **Do not overcount.** Invoices can contain multiple products and a source record can have multiple DQ issues. Measures must reflect those different grains.
8. **Validate before promising zero fact scans.** The intended query path must be confirmed with Power BI query diagnostics and Databricks query plans/logs. Having aggregates available does not automatically guarantee Power BI will use them.

## 3. Aggregate inventory

The proposed baseline contains five aggregate tables.

| # | Table | Grain | Primary purpose |
|---|---|---|---|
| 1 | `workspace.gold.agg_sales_daily` | Date × Store × Loyalty Tier × Payment Method | Executive and Store/Regional performance |
| 2 | `workspace.gold.agg_sales_product_daily` | Date × Product | General Product dashboard analysis |
| 3 | `workspace.gold.agg_sales_product_store_daily` | Date × Product × Store × Loyalty Tier × Payment Method | Product analysis with store/geography/segment filters; filtered category analysis |
| 4 | `workspace.gold.agg_data_quality_daily` | DQ date × Source Table × Source File × DQ Rule × Severity × Status × applicable Geography | DQ issue analysis |
| 5 | `workspace.gold.agg_data_quality_record_daily` | Evaluation date × Source Table × Source File × applicable Geography | DQ denominators, valid/incorrect record counts, distinct affected records |

`applicable Geography` means a `geography_key` is included when the affected source record can be mapped to geography and the dashboard needs to filter by it. If this key is included, it is part of the table grain. Rows with no applicable geography use the agreed unknown geography member. Do not infer geography for source records where no defensible mapping exists.

The two DQ aggregates have distinct purposes:
- `agg_data_quality_daily` counts issue instances and groups them by DQ attributes.
- `agg_data_quality_record_daily` counts evaluated source records and provides denominators for quality percentages.

The fifth table is not a replacement for the individual-record remediation grid.

## 4. Aggregate 1 — `agg_sales_daily`

### 4.1 Grain

One row per:

`date_key × store_key × loyalty_tier × payment_method`

This grain supports daily analysis while preserving the Loyalty Tier and Payment Method filters required by the Executive and Store/Regional dashboards.

### 4.2 Proposed schema

| Column | Type | Description |
|---|---|---|
| `date_key` | INT | FK to `dim_date`; must use the project's actual date-key convention |
| `store_key` | BIGINT | FK to `dim_store` |
| `loyalty_tier` | STRING | Customer loyalty tier; define a consistent unknown/not-applicable representation |
| `payment_method` | STRING | Payment method from the invoice header |
| `invoice_count` | BIGINT | Distinct invoice count within this exact grain |
| `revenue` | DECIMAL(18,2) | Net sales revenue |
| `units_sold` | BIGINT | Total quantity sold |
| `gross_margin` | DECIMAL(18,2) | Revenue less product cost |
| `_last_updated_utc` | TIMESTAMP | Aggregate maintenance timestamp |

The table does not repeat region, state, or city. These attributes are obtained through `store_key → dim_store → dim_geography`, in accordance with the accepted star schema.

### 4.3 Example records

Illustrative values only; date keys and data must match the actual Gold dimensions.

| date_key | store_key | loyalty_tier | payment_method | invoice_count | revenue | units_sold | gross_margin |
|---:|---:|---|---|---:|---:|---:|---:|
| 20260901 | 12 | Gold | Credit Card | 420 | 52400.00 | 1850 | 13100.00 |
| 20260901 | 12 | Silver | Cash | 180 | 14200.00 | 610 | 3200.00 |
| 20260901 | 18 | Gold | Credit Card | 365 | 44100.00 | 1520 | 10800.00 |

### 4.4 Dashboard usage

- **Executive:** revenue, invoice count, average invoice value, revenue growth, daily trends, revenue versus invoice volume, Top 10 Cities by Revenue, and geography rollups where filter semantics are preserved.
- **Store/Regional:** revenue, growth, average invoice value, revenue per store, map, regional trend, Top/Bottom 10 Stores, and revenue versus invoice volume.
- **Product:** not the primary source for product-level measures.
- **Data Quality:** not used.

### 4.5 Measure rules

- `Average Invoice Value = SUM(revenue) / SUM(invoice_count)` at the selected filter context.
- `Revenue Growth % = (current comparable-period revenue - prior comparable-period revenue) / prior comparable-period revenue`, with explicit handling when prior-period revenue is zero or missing.
- `invoice_count` must be computed as a distinct count of invoice headers at this grain. The design assumes each invoice has exactly one payment method and one customer loyalty-tier assignment for reporting. If either assumption is false, define an explicit allocation or counting policy before implementation.
- `units_sold` and `gross_margin` are additive only under consistent line-level calculation and currency semantics.

## 5. Aggregate 2 — `agg_sales_product_daily`

### 5.1 Grain

One row per:

`date_key × product_key`

### 5.2 Proposed schema

| Column | Type | Description |
|---|---|---|
| `date_key` | INT | FK to `dim_date` |
| `product_key` | BIGINT | FK to `dim_product`; identifies the appropriate SCD2 version for the transaction date |
| `units_sold` | BIGINT | Units sold |
| `revenue` | DECIMAL(18,2) | Net sales revenue |
| `gross_margin` | DECIMAL(18,2) | Revenue less product cost |
| `_last_updated_utc` | TIMESTAMP | Aggregate maintenance timestamp |

### 5.3 Example records

| date_key | product_key | units_sold | revenue | gross_margin |
|---:|---:|---:|---:|---:|
| 20260901 | 101 | 420 | 12600.00 | 3780.00 |
| 20260901 | 102 | 275 | 8250.00 | 1925.00 |
| 20260902 | 101 | 390 | 11700.00 | 3510.00 |

### 5.4 Dashboard usage

- **Product:** primary source for product revenue, units sold, average selling price, gross margin, gross margin percentage, product revenue trends, Top 10 Products, and margin rankings when no Store/geography/Loyalty Tier filter is applied.
- **Executive:** not the default source for invoice KPIs. Product/category visuals that must respond to store, geography, or customer-segment filters use `agg_sales_product_store_daily`.
- **Store/Regional and Data Quality:** not used as primary sources.

### 5.5 Measure rules

- `Average Selling Price = SUM(revenue) / SUM(units_sold)`, with appropriate handling for zero units.
- `Gross Margin % = SUM(gross_margin) / SUM(revenue)`, with appropriate handling for zero revenue.
- Revenue and gross margin must use the project's approved line-revenue and cost definitions.
- The product dimension hierarchy provides Category → Subcategory → Product, and brand attributes, subject to correct SCD2 key resolution.

## 6. Aggregate 3 — `agg_sales_product_store_daily`

### 6.1 Grain

One row per:

`date_key × product_key × store_key × loyalty_tier × payment_method`

This is the primary aggregate for product measures that must respond to store, geography, loyalty-tier, and payment-method filters.

### 6.2 Proposed schema

| Column | Type | Description |
|---|---|---|
| `date_key` | INT | FK to `dim_date` |
| `product_key` | BIGINT | FK to the applicable `dim_product` SCD2 version |
| `store_key` | BIGINT | FK to `dim_store` |
| `loyalty_tier` | STRING | Customer loyalty tier; consistent unknown/not-applicable value required |
| `payment_method` | STRING | Payment method |
| `units_sold` | BIGINT | Units sold |
| `revenue` | DECIMAL(18,2) | Net sales revenue |
| `gross_margin` | DECIMAL(18,2) | Revenue less product cost |
| `_last_updated_utc` | TIMESTAMP | Aggregate maintenance timestamp |

### 6.3 Example records

| date_key | product_key | store_key | loyalty_tier | payment_method | units_sold | revenue | gross_margin |
|---:|---:|---:|---:|---|---:|---:|---:|
| 20260901 | 101 | 12 | Gold | Credit Card | 35 | 1050.00 | 315.00 |
| 20260901 | 101 | 12 | Silver | Cash | 18 | 540.00 | 162.00 |
| 20260901 | 102 | 18 | Gold | Credit Card | 22 | 660.00 | 154.00 |

### 6.4 Dashboard usage

- **Executive:** category/subcategory revenue visuals where applicable filters must affect those visuals.
- **Store/Regional:** not required for the documented core visuals, but available for future product analysis by location.
- **Product:** supports product/category/subcategory/brand analysis filtered by store, city/state/region, loyalty tier, and payment method.
- **Data Quality:** not used.

### 6.5 Critical design constraints

- Do not add a simple additive invoice count to this table. An invoice can contain multiple products, and summing product-level invoice counts would overcount invoices.
- The row count may be materially larger than `agg_sales_product_daily`. Estimate cardinality using distinct combinations of date, product, store, loyalty tier, and payment method before choosing physical partitioning and file layout.
- If this table is too large, optimize the storage and serving strategy, but do not silently remove required slicer behavior. Any change to which visuals a slicer affects must be explicitly documented and approved.
- Revenue and margin must reconcile to the atomic facts for identical filter context.

## 7. Aggregate 4 — `agg_data_quality_daily`

### 7.1 Grain

One row per:

`dq_date_key × source_table × source_file_name × dq_rule_key × severity × dq_status × geography_key (when applicable)`

The geography key is included when the source record has a defensible geographic mapping and the dashboard requirement needs geographical DQ analysis. Its inclusion changes the grain.

### 7.2 Proposed schema

| Column | Type | Description |
|---|---|---|
| `dq_date_key` | INT | FK to `dim_date`; represents the agreed issue/quarantine reporting date |
| `source_table` | STRING | Source entity/table |
| `source_file_name` | STRING | Source file |
| `dq_rule_key` | BIGINT | FK to `dim_data_quality_rule` |
| `severity` | STRING | Severity associated with the rule/issue |
| `dq_status` | STRING | Issue status |
| `geography_key` | BIGINT | Optional FK to `dim_geography`, required only when applicable |
| `dq_issue_count` | BIGINT | Count of issue instances |
| `affected_record_count` | BIGINT | Distinct affected source records within this exact grain |
| `_last_updated_utc` | TIMESTAMP | Aggregate maintenance timestamp |

### 7.3 Example records

| dq_date_key | source_table | source_file_name | dq_rule_key | severity | dq_status | dq_issue_count | affected_record_count |
|---:|---|---|---:|---|---|---:|---:|
| 20260901 | invoices | invoices_2026_09_01.parquet | 3 | Error | Quarantined | 120 | 95 |
| 20260901 | customers | customers_2026_09_01.parquet | 7 | Error | Quarantined | 80 | 76 |
| 20260902 | products | products_2026_09_02.parquet | 5 | Warning | Open | 24 | 22 |

### 7.4 Dashboard usage

- **Data Quality:** issue trend, issues by source table, error rule, severity, status, Pareto, source-file analysis, and geographical analysis where context is available.
- **Executive, Store/Regional, Product:** not used.

### 7.5 Counting rules

- `dq_issue_count` counts issue instances, not distinct records.
- `affected_record_count` is distinct only within the table's complete grain. It cannot be summed across rules to calculate globally distinct affected records.
- For a global distinct affected-record KPI, use a precomputed distinct-record aggregate or calculate it from a record-grain summary. Do not sum issue counts.

## 8. Aggregate 5 — `agg_data_quality_record_daily`

### 8.1 Grain

One row per:

`evaluation_date_key × source_table × source_file_name × geography_key (when applicable)`

### 8.2 Proposed schema

| Column | Type | Description |
|---|---|---|
| `evaluation_date_key` | INT | FK to `dim_date`; represents the agreed evaluation/snapshot date |
| `source_table` | STRING | Source entity/table |
| `source_file_name` | STRING | Evaluated source file |
| `geography_key` | BIGINT | Optional FK to `dim_geography` when relevant |
| `total_records` | BIGINT | Records evaluated in the defined population |
| `valid_records` | BIGINT | Distinct records classified as valid |
| `incorrect_records` | BIGINT | Distinct records classified as incorrect |
| `_last_updated_utc` | TIMESTAMP | Aggregate maintenance timestamp |

### 8.3 Example records

| evaluation_date_key | source_table | source_file_name | total_records | valid_records | incorrect_records |
|---:|---|---|---:|---:|---:|
| 20260901 | invoices | invoices_2026_09_01.parquet | 10000 | 9880 | 120 |
| 20260901 | customers | customers_2026_09_01.parquet | 5000 | 4920 | 80 |
| 20260902 | products | products_2026_09_02.parquet | 2500 | 2476 | 24 |

### 8.4 Dashboard usage

- **Data Quality:** Data Quality %, Incorrect Records %, record counts, affected-record analysis, and quality trends by source and file.
- **Executive, Store/Regional, Product:** not used.

### 8.5 Historical correctness

To support historical DQ trends, preserve daily evaluation/snapshot history or an equivalent event-based history. Do not overwrite prior counts with the latest status if doing so would destroy the history required by the dashboard.

Define the reporting population consistently so records are not counted multiple times across repeated evaluations or reprocessing runs.

## 9. Dashboard-to-aggregate coverage

| Dashboard requirement | Primary aggregate(s) |
|---|---|
| Executive revenue, invoices, average invoice, growth, trends | `agg_sales_daily` |
| Executive Top 10 Cities and geography rollups | `agg_sales_daily` via store/geography dimensions |
| Executive revenue by category/subcategory with applicable filters | `agg_sales_product_store_daily` |
| Store/Regional KPIs, regional trend, map, Top/Bottom 10 Stores | `agg_sales_daily` via store/geography dimensions |
| Product KPIs, trends, Top 10 Products, margin analysis without location/loyalty filters | `agg_sales_product_daily` |
| Product analysis with store, geography, loyalty-tier, or payment filters | `agg_sales_product_store_daily` |
| DQ issue trends, rule, source table/file, severity, Pareto | `agg_data_quality_daily` |
| DQ %, incorrect records %, source record counts | `agg_data_quality_record_daily` |
| DQ remediation grid | `fact_data_quality_issue` and relevant `quarantined_*` tables at record level |

The actual Power BI report must implement each visual using the appropriate aggregate-backed measures. The table inventory alone does not guarantee the report will avoid fact scans.

## 10. Shared Gold dimensions and relationships

Use the existing Gold dimensions from `09_gold_model_design.md`, including:

- `dim_date`
- `dim_geography`
- `dim_store`
- `dim_customer`
- `dim_category`
- `dim_subcategory`
- `dim_product`
- `dim_data_quality_rule`

Relationships must follow the accepted star-schema design. Geography is reached through the store/customer dimensions for sales analysis; do not add a direct geography relationship to the sales facts or duplicate city/state/region/ZIP attributes in aggregate tables.

For product analysis, resolve the correct `dim_product` SCD2 version for the transaction date. Similarly, store/customer SCD2 versions must be resolved as appropriate to the transaction date when their attributes are required.

For DQ aggregates, only use a geography key when it is grounded in the affected record. Do not infer missing geography or attach a business dimension simply because the source table has a similar name.

## 11. Business measure definitions

### Sales

Use the project's approved line-revenue definition:

`line_revenue = quantity × unit_price - COALESCE(discount, 0)`

Invoice revenue is the sum of line revenue for the invoice. Gross margin is revenue minus the corresponding product cost, using a documented cost basis.

### Additive measures

- Revenue: additive across supported dimensions.
- Units sold: additive across supported dimensions.
- Gross margin amount: additive if cost semantics and currency are consistent.
- Invoice count: additive only in an aggregate whose grain assigns each invoice to exactly one group for the selected dimensions.

### Ratios

- Average Invoice Value = total revenue / total invoices.
- Average Selling Price = total revenue / total units sold.
- Gross Margin % = total gross margin / total revenue.
- Revenue Growth % = (current-period revenue - comparable prior-period revenue) / comparable prior-period revenue.
- Data Quality % = valid records / total evaluated records.
- Incorrect Records % = incorrect records / total evaluated records.

Implement explicit behavior for zero denominators, missing prior periods, unknown members, and currencies if applicable.

## 12. Incremental refresh and maintenance

The aggregate build process must:

1. Identify the source Gold fact/dimension records changed since the previous successful run.
2. Recompute all affected aggregate groups, including groups impacted by updates or deletions.
3. Upsert aggregate rows using the complete grain as the merge key.
4. Prevent duplicate rows at the declared grain.
5. Commit a successful processing watermark only after validation.
6. Reconcile aggregate measures with source facts for affected periods and selected full-period checks.
7. Preserve DQ historical snapshots/event semantics.
8. Record job execution metadata and failures.

If SCD2 attributes change, aggregates that use the corresponding dimension version must follow the agreed effective-date logic. A late-arriving dimension correction may require rebuilding affected historical groups.

## 13. Performance and cardinality validation

Before implementation, estimate expected row counts using the actual source data or representative profiling.

In particular, profile the number of distinct combinations for:

`date_key × product_key × store_key × loyalty_tier × payment_method`

This determines the size of `agg_sales_product_store_daily`. Do not assume that an aggregate is small solely because it is summarized.

Also estimate:
- `agg_sales_daily`: distinct date/store/loyalty/payment combinations.
- `agg_sales_product_daily`: distinct date/product combinations.
- DQ aggregate row counts by the complete declared grains.

Select partitioning, clustering, file sizing, and refresh strategy based on measured volume and query patterns rather than arbitrary partitioning on high-cardinality keys.

## 14. Power BI acceptance criteria — no routine sales fact scans

The design is accepted only after all the following tests pass:

1. Every KPI, visual, slicer, time hierarchy, drill-down, and applicable filter in `10_bi_requirements_and_dashboard_design.md` maps to an aggregate and dimensions.
2. Every relevant filter changes the expected visuals correctly; no measure silently ignores a required filter.
3. Revenue, invoice count, units, and gross margin reconcile with the atomic Gold facts at matching grains and filter contexts.
4. Ratios are calculated from additive components, not averaged from precomputed ratios.
5. Power BI query diagnostics and Databricks query history/plans confirm that routine Executive, Store/Regional, and Product dashboard interactions read aggregate tables and dimensions, not `fact_invoice` or `fact_invoice_item`.
6. DQ analytical visuals read the DQ aggregates and dimensions.
7. The DQ remediation grid reads only the relevant issue/quarantine detail datasets and does not scan the sales facts.
8. The largest aggregate's measured row count and refresh cost are acceptable.

**Important:** Power BI will not automatically choose these aggregates just because they exist. The semantic model, relationships, measures, and possibly aggregation mappings must be configured and verified. If a required slicer or visual cannot be served by an aggregate, the design is not complete.

## 15. Open design validations before production

The following are explicit validation tasks, not reasons to defer the design:

- Confirm that each invoice has one payment method and one reporting loyalty tier, or define an allocation/counting policy.
- Confirm the SCD2 effective-date joins for product, store, and customer dimensions.
- Decide how unknown/no-customer invoices are represented in the loyalty-tier grain without treating them as data-quality failures.
- Define whether DQ counts represent first evaluation, latest status, or daily snapshots, and ensure historical trends use consistent populations.
- Confirm how the optional geography filter is implemented for DQ issues.
- Profile aggregate cardinality and storage/refresh cost, especially for `agg_sales_product_store_daily`.
- Validate all report interactions and query plans before claiming the no-fact-scan target has been met.

## 16. Final architecture

The intended serving path is:

```text
Gold atomic facts and DQ issue/quarantine data
                    |
          Gold aggregation jobs
                    |
       +------------+-------------+
       |            |             |
  Sales daily   Product daily   Product × Store daily
       |
       +-------------------------------+
                                       |
                            Power BI business dashboards

DQ issue data ------> DQ daily aggregate ------> DQ analytical visuals
DQ evaluation data -> DQ record aggregate ----> DQ KPIs/trends
DQ issue/quarantine detail -------------------> DQ remediation grid
```

The aggregate layer is a serving layer optimized for BI. It does not replace the atomic facts or the DQ detail datasets, and it must be reconciled to them.

## 17. Next step

Implement the aggregate creation and loading process documented in `12_gold_creation_and_loading.md` only after the schemas, measure semantics, SCD2 handling, and cardinality checks in this document have been validated.

The target is clear: routine Power BI interactions for the three sales dashboards must be served from the summarized Gold layer and dimensions, while the Data Quality remediation grid accesses only its necessary record-level DQ datasets.
