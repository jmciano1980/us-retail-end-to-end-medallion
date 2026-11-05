# 10 — BI Requirements and Dashboard Design

## 1. Purpose and scope

This document defines the four Power BI dashboards for the US Retail platform. The three commercial dashboards follow the supplied November 2026 mockups. The Data Quality dashboard specification in Sections 18–24 is retained without redesign.

The implementation boundary is explicit:
- RAW, BRONZE and SILVER are unchanged.
- Gold dimensions and atomic facts are unchanged and remain authoritative.
- The three commercial dashboards consume exactly one dedicated Gold aggregate each, plus the existing Gold dimensions.
- The Data Quality dashboard retains its two existing aggregate tables and its record-level issue/quarantine sources.
- Routine commercial dashboard interactions must not scan `fact_invoice` or `fact_invoice_item`.

## 2. Shared BI principles

1. Commercial dashboards are summarized, decision-oriented reports, not invoice-detail grids.
2. Each dashboard has one primary aggregate table. Do not combine commercial aggregate tables to serve one report.
3. All ratios are calculated from additive components; never average precomputed percentages or averages.
4. Existing Gold dimensions supply labels, hierarchies and descriptive attributes. Do not duplicate geography or product descriptions in aggregates.
5. Date filtering is mandatory. The Executive and Store dashboards are daily-grain; the Product dashboard is monthly-grain.
6. A Power BI aggregate's existence does not guarantee its use. Relationships, measures and storage/aggregation mappings must be validated with Performance Analyzer and Databricks query history.
7. The Gold fact tables remain the reconciliation source, not the normal dashboard source.

## 3. Shared dimensions and filters

Use the existing dimensions and relationships from `09_gold_model_design.md`:
- `dim_date`
- `dim_store` and its existing relationship to `dim_geography`
- `dim_product`, `dim_subcategory`, `dim_category`
- `dim_customer` where required by the dashboard

Do not create a direct Geography-to-sales-fact relationship and do not duplicate City, State, ZIP or Region in aggregate tables.

---

# 4. Dashboard 1 — Executive Performance

## 4.1 Purpose

Provide a concise, high-level view of overall business performance. The reviewed Executive mockup attached on 5 November 2026 is the authoritative visual reference and supersedes all previous Executive mockups.

## 4.2 Headline KPIs

1. **Total Revenue** — sum of net invoice-line revenue.
2. **Units Sold** — sum of line quantity.
3. **Gross Margin %** — total gross margin divided by total revenue.
4. **Revenue Growth** — selected-period revenue compared with the immediately preceding equivalent-length period.

All four KPIs are supported by the approved Executive aggregate grain. Ratios and growth are calculated from additive revenue, units and gross-margin measures; do not average row-level ratios.

## 4.3 Filters

- Mandatory: date range.
- Geography: Region and State.
- Store.
- Product: Category and Subcategory.

Filters must propagate through the existing Gold dimensions and the Executive aggregate. Customer Segment / Loyalty Tier is not an Executive filter because it is not retained in this aggregate. Do not introduce invoice-level filters or payment method.

## 4.4 Visuals

1. **Revenue Trend** — daily revenue for the selected period, compared with the immediately preceding equivalent-length period.
2. **Revenue by Product Category** — revenue by category, shown as a horizontal bar chart.
3. **Revenue by Region** — geographic revenue map, using the existing Store-to-Geography relationship.
4. **Top 10 Stores by Revenue** — ranked horizontal bar chart.

The Top 10 Stores visual is explicitly included in the reviewed mockup. Do not replace it with Top 10 Products, a product scatter plot or Top 10 Subcategories.

## 4.5 Grain and serving table

Primary table: `workspace.gold.agg_executive_performance_daily`.

**Exact approved grain:** `date_key × store_key × subcategory_key`.

This grain is mandatory. Loyalty Tier is deliberately excluded; do not add Product Key or customer-segment attributes. Category is reached through the existing product hierarchy (`dim_subcategory → dim_category`). Store geography is reached through `dim_store → dim_geography`.

## 4.6 Measure behavior

- Revenue, Units Sold and Gross Margin are additive at the approved grain.
- Gross Margin % = `SUM(gross_margin) / SUM(revenue)`.
- Revenue Growth compares the selected period with the immediately preceding equivalent-length period, using additive revenue.
- Revenue by Category groups the aggregate through `dim_subcategory → dim_category`.
- Top 10 Stores groups the aggregate by `store_key`.
- No invoice count or average invoice value is required or permitted as an Executive KPI in this design.
- The aggregate does not retain product-level identity. Do not attempt product-level visuals from it.

# 5. Dashboard 2 — Store & Regional Performance

## 5.1 Purpose

Analyze geographic performance and compare individual stores. The supplied mockup is the visual reference.

## 5.2 Headline KPIs

1. **Total Revenue** — selected period and geography.
2. **Active Stores** — count of active/current stores in the selected geographic scope, sourced from `dim_store` (not inferred from stores with sales in a particular day).
3. **Revenue per Store** — Total Revenue / Active Stores.
4. **Revenue Growth** — selected period compared with the previous comparable period.

## 5.3 Filters

- Mandatory: date range, with Year/Month hierarchy.
- Region, State, City and Store.

## 5.4 Visuals

1. Revenue by State map, with geographic drill-down where supported by the existing dimensions.
2. Monthly Revenue by Region line chart.
3. Top 10 Stores by Revenue.
4. Bottom 10 Stores by Revenue.

## 5.5 Grain and serving table

Primary table: `workspace.gold.agg_store_performance_daily`.

**Grain:** `date_key × store_key`.

The store dimension supplies City, State and Region. Geography remains reached through the existing `dim_store → dim_geography` relationship.

## 5.6 Measure behavior

- Revenue, Units Sold and Gross Margin are additive.
- `invoice_count` is calculated from distinct invoice headers at Date × Store, not from invoice-item rows.
- Revenue per Store uses the selected-scope active-store count. A store with no sales in a selected date window must not silently disappear from the denominator if it remains an active store.
- Revenue growth is calculated from additive revenue for comparable periods.

---

# 6. Dashboard 3 — Product Performance

## 6.1 Purpose

Analyze product, category and subcategory revenue and profitability. The supplied mockup is the visual reference.

## 6.2 Headline KPIs

1. **Product Revenue** — sum of net invoice-line revenue.
2. **Units Sold**.
3. **Average Selling Price** = Total Revenue / Total Units Sold.
4. **Gross Margin %** = Total Gross Margin / Total Revenue.

## 6.3 Filters

- Mandatory: date range, Year and Month. The report's trend is monthly.
- Region, State and Store.
- Category, Subcategory and Product.

## 6.4 Visuals

1. Revenue Trend by Product Category (monthly lines).
2. Revenue by Region and Category (map/region comparison, as represented in the mockup).
3. Top 10 Products by Revenue.
4. Revenue vs. Gross Margin scatter; bubble size represents Units Sold.
5. Top 10 Subcategories by Revenue.
6. Top 5 Products by Store comparison (heatmap/table).
7. Product Revenue by Store for the selected product (bar chart).

## 6.5 Grain and serving table

Primary table: `workspace.gold.agg_product_performance_monthly`.

**Exact approved grain:** `month_date_key × store_key × product_key`.

`month_date_key` is the first calendar day of the month encoded as YYYYMMDD (for example, 20260101), and is a valid FK to `dim_date.date_key`. Do not use `ROUND(date_key / 100)`: rounding is not a safe month derivation. The ETL must derive the month start with calendar date functions.

Category and Subcategory are resolved through `dim_product`; Region, State and City are resolved through `dim_store → dim_geography`.

## 6.6 Measure behavior

- Revenue, Units Sold and Gross Margin are additive.
- Average Selling Price and Gross Margin % are ratios of summed components.
- Product ranking and store comparisons are based on the selected monthly date window.
- The table intentionally does not contain Loyalty Tier because it is not a filter in the supplied Product mockup. Do not introduce a loyalty segmentation requirement without revisiting this grain.

---

# 18. Dashboard 4 — Data Quality
## 18.1 Purpose

The Data Quality Dashboard is an operational analytical dashboard.

Its purpose is twofold:

1. Measure the overall quality of the data.
2. Identify the exact source records requiring correction.

Unlike the first three dashboards, this dashboard may contain a detailed grid.

The dashboard should answer:

> How much incorrect data exists?

> What types of errors are occurring?

> Which source tables are affected?

> Is data quality improving or deteriorating?

> Which exact records need correction?

---

# 19. Data Quality KPIs

Four headline KPIs are recommended.

## KPI 1 — Data Quality %

```text
Data Quality % =
Valid Records / Total Records
```

---

## KPI 2 — Incorrect Records %

```text
Incorrect Records % =
Quarantined Records / Total Records
```

---

## KPI 3 — Total DQ Issues

Number of detected data-quality issues.

---

## KPI 4 — Affected Records

Number of distinct source records affected by one or more DQ issues.

This distinction is important.

For example:

```text
100 affected records
300 DQ issues
```

means that multiple errors can affect the same record.

---

# 20. Data Quality Filters

### Mandatory

- Date / Month / Date range

### Source

- Source Table
- Source File

### DQ

- Error Code
- Error Rule
- Severity
- DQ Status

### Business dimensions where available

- Region
- State
- Store
- Product
- Customer

---

# 21. Data Quality Visualizations

## 21.1 Data Quality Trend

Line chart showing:

**Data Quality % over time**

Purpose:

> Is data quality improving or deteriorating?

---

## 21.2 DQ Issues by Source Table

Bar chart showing issues by:

- customers
- products
- stores
- invoices
- invoice_items

Purpose:

> Which source dataset produces the most quality problems?

---

## 21.3 DQ Issues by Error Rule

Bar chart showing:

- missing required field
- invalid date
- invalid numeric value
- duplicate business key
- invalid reference
- other defined rules

The visualization should use:

`dim_data_quality_rule`

as the analytical reference.

---

## 21.4 DQ Severity

Visualization showing issues by:

- Critical
- High
- Medium
- Low

This allows users to prioritize remediation.

---

## 21.5 DQ Pareto Analysis

A Pareto chart should be considered to identify the small number of rules responsible for most DQ issues.

This answers:

> Which problems are responsible for the majority of data-quality failures?

---

## 21.6 DQ by Geography

Where sufficient lineage exists, DQ issues can be analyzed by:

- Region
- State
- Store

This may identify operational areas generating disproportionately high-quality problems.

This visualization should be interpreted carefully because geographic concentration does not necessarily imply that geography itself is the root cause.

---

# 22. Data Quality Remediation Grid

The DQ dashboard is the only primary dashboard that should intentionally contain a detailed grid.

The purpose of the grid is remediation.

It should not simply reproduce the quarantine tables.

It should provide an actionable remediation queue.

Recommended columns include:

| Field | Purpose |
|---|---|
| Source Table | Identifies affected dataset |
| Source Row ID | Technical lineage |
| Source File Name | Source-file traceability |
| Business Identifier | Customer / Product / Store / Invoice identifier |
| Invoice Number | Required when applicable |
| Error Code | Machine-readable DQ rule |
| Error Description | Human-readable explanation |
| Severity | Remediation priority |
| DQ Status | Current status |
| Quarantine Date | When the problem was detected |

Additional business attributes may be displayed depending on the source table.

---

# 23. Invoice Remediation Example

For an invoice problem, the dashboard should provide at minimum:

```text
Source Table
Invoice Number
Invoice Date
Store
Customer
Error Code
Error Description
Severity
Source Row ID
Source File Name
DQ Status
```

Example:

```text
Source Table:       invoices
Invoice Number:     INV-12345
Invoice Date:       2026-09-30
Store:              Store 104
Error Code:         INVALID_STORE_REFERENCE
Description:        Store ID does not exist in the valid Silver store dataset
Severity:           High
DQ Status:          Open
```

The user should be able to use the invoice number directly to locate the corresponding record in the source system.

---

# 24. DQ Detail Navigation

The Data Quality Dashboard should support filtering the remediation grid from the analytical visuals.

Example:

```text
Click "INVALID_PRODUCT_REFERENCE"
        ↓
Grid shows only records with that error
```

Or:

```text
Click "Store 104"
        ↓
Grid shows DQ problems associated with Store 104
```

Or:

```text
Click "Invoices"
        ↓
Grid shows invoice-related DQ issues
```

This makes the dashboard an operational remediation tool rather than merely a reporting screen.
---

# 25. Dashboard Comparison

| Requirement | Executive | Store / Regional | Product | Data Quality |
|---|---:|---:|---:|---:|
| 3–4 KPIs | Yes | Yes | Yes | Yes |
| Mandatory time filter | Yes | Yes | Yes | Yes |
| Trend analysis | Yes | Yes | Yes | Yes |
| Top 10 analysis | Yes | Yes | Yes | Optional |
| Bottom 10 analysis | Optional | Yes | Optional | Optional |
| Geographic map | Optional | Yes | Yes | Optional |
| Drill-down | Yes | Yes | Yes | Yes |
| Detailed grid | No | No | No | Yes |
| Individual source record | No | No | No | Yes |
| Remediation workflow | No | No | No | Yes |

---
# 26. Common Analytical Hierarchies

## Time

```text
Year
  ↓
Quarter
  ↓
Month
  ↓
Day
```

The Product dashboard is served at month-start grain; the Executive and Store dashboards are served at daily grain.

## Geography

```text
Region
  ↓
State
  ↓
City
  ↓
Store
```

## Product

```text
Category
  ↓
Subcategory
  ↓
Product
```

## Data Quality

```text
Source Table
  ↓
DQ Rule
  ↓
Source Record
```

---
# 27. Gold Base Tables Required

Commercial dashboards use the existing Gold dimensional model and the dedicated aggregate tables defined in `11_gold_aggregation_design.md`. The existing atomic facts `fact_invoice` and `fact_invoice_item` remain the reconciliation foundation and are not the routine commercial dashboard source.

The Data Quality dashboard additionally uses `dim_data_quality_rule`, `fact_data_quality_issue`, both DQ aggregates, and relevant quarantine datasets for remediation details.

---
# 28. Measure Definitions and Data Integrity

### Commercial measures

```text
Net line revenue = quantity * unit_price - COALESCE(discount, 0)
Gross margin = net line revenue - (quantity * product cost)
Average selling price = SUM(revenue) / SUM(units_sold)
Gross margin % = SUM(gross_margin) / SUM(revenue)
Revenue growth % = (current comparable-period revenue - prior-period revenue) / prior-period revenue
```

Use explicit handling for zero denominators and missing comparison periods. Do not average row-level ratios.

A NULL `customer_id` is a valid anonymous/no-customer condition and maps to the generic Gold customer member (`customer_key = -1`, displayed as “No customer assigned”). It must remain distinguishable from an invalid customer reference quarantined by Silver.

### Data Quality measures

```text
Total Records
Valid Records
Incorrect Records
Data Quality %
Incorrect Records %
Total DQ Issues
Affected Records
```

The Data Quality definitions and remediation behavior above remain separate from commercial KPI logic.

---
# 29. Aggregation Requirements

The requirements defined here drive `11_gold_aggregation_design.md`. For each dashboard, the aggregation design specifies its single commercial aggregate, grain, dimensions, additive measures, refresh strategy, cardinality profile and reconciliation method. The Data Quality dashboard retains its two existing DQ aggregates.

The aggregate inventory must be driven by actual dashboard visuals and filter behavior, not by creating arbitrary combinations. The three commercial dashboards must not combine multiple commercial aggregates.

---
# 30. Performance Principle

The objective is not to create the largest possible number of aggregate tables. The objective is to create only the aggregates that materially improve Power BI performance, repeated analytical workloads, high-volume fact analysis, common visuals and predictable refresh behavior.

The three commercial aggregates must serve their respective mockups without routine scans of `fact_invoice` or `fact_invoice_item`. Validate actual row counts and query plans; aggregation tables do not automatically guarantee that Power BI uses them.

---
# 31. BI Data Integrity

Power BI must not silently correct or reinterpret data. Business rules belong in Silver and Gold conformance, not in report-specific workarounds.

A NULL `customer_id` represents an anonymous transaction and maps to the generic Gold member `customer_key = -1`, displayed as “No customer assigned”. It must remain analytically distinguishable from an invalid customer reference, which is a data-quality problem.

---
# 32. Final dashboard-to-aggregate architecture

| Dashboard | Single primary aggregate | Grain |
|---|---|---|
| Executive Performance | `agg_executive_performance_daily` | Date × Store × Subcategory |
| Store & Regional Performance | `agg_store_performance_daily` | Date × Store |
| Product Performance | `agg_product_performance_monthly` | Month × Store × Product |
| Data Quality | `agg_data_quality_daily` and `agg_data_quality_record_daily` | Existing DQ issue and record snapshot grains |

The first three dashboards must use their one assigned aggregate plus dimensions. The DQ remediation grid remains record-level and is intentionally not forced into an aggregate.

---
# 33. Dashboard Design Summary

## Executive Performance

Focus: Total Revenue, Units Sold, Gross Margin %, Revenue Growth, daily revenue trend, revenue by product category, revenue by region and Top 10 Stores by Revenue. No detailed transaction grid.

## Store & Regional Performance

Focus: revenue, active stores, revenue per store, growth, state map, regional trends and Top/Bottom 10 stores. No detailed transaction grid.

## Product Performance

Focus: product revenue, units sold, average selling price, gross margin, monthly category trends, region/category analysis, Top 10 products, Top 10 subcategories, store comparisons and selected-product store performance. No detailed transaction grid.

## Data Quality

Focus and remediation requirements remain as defined in Sections 18–24: data-quality percentage, incorrect-record percentage, DQ issues, affected records, error types, severity, trends, source tables and remediation grid. Detailed records are intentionally permitted.

---
# 34. Power BI Acceptance Criteria

1. Every visual and slicer in the three commercial mockups is supported by its assigned aggregate and existing dimensions. The reviewed Executive mockup dated 5 November 2026 is authoritative.
2. No commercial dashboard visual requires a second commercial aggregate.
3. Revenue, units and gross margin reconcile to Gold invoice-line facts at matching filter context.
4. Store invoice counts reconcile to distinct invoice headers.
5. Active Stores uses the current store dimension population, not merely stores with activity in the selected period.
6. Monthly product keys are valid first-of-month `dim_date` keys.
7. Power BI Performance Analyzer and Databricks query history confirm routine commercial interactions do not scan the atomic sales facts.
8. DQ visuals continue to use the existing DQ aggregates and DQ remediation datasets.
