# BI Requirements and Dashboard Design

## 1. Purpose

This document defines the Business Intelligence requirements and dashboard design for the US Retail End-to-End Data Engineering Platform.

The purpose of this stage is to define:

- the business questions that the BI layer must answer
- the dashboards required
- the KPIs and measures
- the dimensions and filters
- the required analytical granularity
- the visualizations
- the drill-down paths
- the treatment of time
- the distinction between analytical dashboards and operational data-quality remediation
- the requirements that will drive the design of Gold aggregate tables

This document is part of **M8 — Gold**.

It is intentionally defined before the Gold aggregation layer is implemented.

The aggregation design must be derived from actual BI requirements rather than creating aggregate tables arbitrarily.

---

# 2. BI Design Principles

The Power BI layer is intended to provide business-oriented analytical information.

It is not intended to reproduce the operational source system.

## 2.1 Decision-oriented analytics

The first three dashboards must help users answer business questions such as:

- What happened?
- How is the business performing?
- Where is performance coming from?
- Which products or categories are driving results?
- How is performance changing over time?
- Which areas require attention?

They should not require users to inspect millions of individual transactions.

---

## 2.2 No operational invoice grids in business dashboards

The following dashboards must not be designed as invoice-detail reports:

- Executive Dashboard
- Store / Regional Dashboard
- Product Dashboard

These dashboards should focus on:

- KPIs
- trends
- comparisons
- rankings
- distributions
- aggregated information
- drill-down analysis

A detailed invoice investigation is an operational use case and should not dominate the analytical dashboards.

If individual transaction investigation is required in the future, it should be implemented as a dedicated drill-through or detail page rather than as the main dashboard experience.

---

## 2.3 Data Quality is different

The Data Quality Dashboard is intentionally different.

Its purpose is not only to measure data quality.

It must also support remediation.

Therefore, the Data Quality Dashboard may contain a detailed grid showing the individual records that require correction.

The grid must provide enough information for the user to identify the corresponding record in the source system without performing additional investigation.

For example, an invoice-quality problem should expose the invoice number.

---

# 3. Standard Dashboard Structure

The first three dashboards should follow a common visual and analytical standard.

Each should contain:

1. **3–4 headline KPIs**
2. **Mandatory time filtering**
3. **Relevant dimensional filters**
4. **Summarized information**
5. **Time-series analysis**
6. **Trend comparisons**
7. **Top 10 / Bottom 10 analysis**
8. **Appropriate drill-downs**
9. **No operational transaction grid**

The Data Quality Dashboard may deviate from this standard where required for remediation.

---

# 4. Common Time Requirements

Time is a mandatory analytical dimension for summarized business information.

Users must be able to filter by:

- Year
- Month
- Date range

Where appropriate, analytical visuals should support:

```text
Year
  ↓
Quarter
  ↓
Month
  ↓
Day
```

Time-series visuals should support comparison against an appropriate previous period.

Examples:

- current month vs previous month
- current quarter vs previous quarter
- current year vs previous year
- selected period vs previous comparable period

The exact comparison logic will be implemented in the Power BI semantic model.

---

# 5. Dashboard 1 — Executive Dashboard

## 5.1 Purpose

The Executive Dashboard provides a concise overview of overall business performance.

The dashboard should allow a management user to understand the current state of the business within seconds.

The dashboard should answer:

> How is the business performing?

> Is revenue growing or declining?

> Is performance driven by transaction volume or transaction value?

> Which cities and product categories are contributing most to revenue?

---

## 5.2 Headline KPIs

The dashboard should contain four headline KPIs.

### KPI 1 — Total Revenue

Definition:

```text
Total Revenue =
SUM(invoice line revenue)
```

Where:

```text
line revenue =
quantity × unit_price
- COALESCE(discount, 0)
```

---

### KPI 2 — Total Invoices

Number of valid invoices in the selected period.

---

### KPI 3 — Average Invoice Value

```text
Average Invoice Value =
Total Revenue / Total Invoices
```

This provides context for revenue growth.

---

### KPI 4 — Revenue Growth %

Revenue change compared with the previous comparable period.

```text
Revenue Growth % =
(Current Revenue - Previous Revenue)
/
Previous Revenue
```

---

## 5.3 Mandatory Filters

### Time

- Year
- Month
- Date range

### Optional dimensions

- Region
- State
- Store
- Product Category
- Customer Loyalty Tier
- Payment Method

The dashboard should avoid excessive filters.

Only dimensions that support meaningful executive analysis should be exposed prominently.

---

# 6. Executive Dashboard — Visualizations

## 6.1 Revenue Trend

Primary time-series visualization.

Displays:

- revenue over time
- selected time granularity
- previous-period comparison where appropriate

Recommended visual:

**Line chart**

Possible hierarchy:

```text
Year
 ↓
Quarter
 ↓
Month
 ↓
Day
```

---

## 6.2 Revenue vs Invoice Volume

Visualization comparing:

- total revenue
- number of invoices

The objective is to determine whether revenue changes are associated primarily with:

- transaction volume
- transaction value

This visualization may use a combination chart or two synchronized trend lines.

---

## 6.3 Top 10 Cities by Revenue

Horizontal bar chart.

Purpose:

> Which cities generate the largest amount of revenue?

The visualization should support filtering by the selected time period.

---

## 6.4 Revenue by Product Category

Horizontal bar chart.

Purpose:

> Which product categories contribute most to revenue?

The visualization should support drill-down:

```text
Category
   ↓
Subcategory
```

---

## 6.5 Optional Executive Visualization

Where sufficient dashboard space exists, an additional visual may show:

**Revenue by Region**

This provides a high-level geographical comparison without turning the Executive Dashboard into a geographic dashboard.

---

# 7. Executive Dashboard — Drill-Down

Primary analytical hierarchies:

### Time

```text
Year
 ↓
Quarter
 ↓
Month
 ↓
Day
```

### Product

```text
Category
 ↓
Subcategory
 ↓
Product
```

### Geography

```text
Region
 ↓
State
 ↓
City
```

The Executive Dashboard should remain high-level even when drill-down is available.

---

# 8. Dashboard 2 — Store / Regional Performance

## 8.1 Purpose

The Store / Regional Dashboard analyzes performance geographically and by physical store.

It should answer:

> Which regions are performing best?

> Which states and cities are driving revenue?

> Which stores are outperforming or underperforming?

> How is regional performance changing over time?

---

# 9. Store / Regional KPIs

The dashboard should contain four headline KPIs.

### KPI 1 — Total Revenue

Revenue for the selected geographic scope and time period.

---

### KPI 2 — Revenue Growth %

Change compared with the previous comparable period.

---

### KPI 3 — Average Invoice Value

Average revenue per invoice.

---

### KPI 4 — Revenue per Store

```text
Revenue per Store =
Total Revenue / Number of Active Stores
```

This provides a normalized measure of store performance.

---

# 10. Store / Regional Filters

### Mandatory

- Year
- Month
- Date range

### Geographic

- Region
- State
- City
- Store

### Optional

- Customer Loyalty Tier
- Payment Method

---

# 11. Store / Regional Visualizations

## 11.1 Geographic Map

A geographic map is mandatory for this dashboard.

The primary map should display:

**Revenue by State**

The geographical hierarchy should support:

```text
Region
 ↓
State
 ↓
City
 ↓
Store
```

The map should allow users to identify geographical concentration of revenue.

---

## 11.2 Regional Revenue Trend

Line chart showing revenue over time by region.

Users should be able to compare multiple regions.

Example:

```text
Region A ─────────────╮
                      ╰────
Region B ───────╮
                ╰──────────
Region C ───────────╮
                    ╰─────
```

---

## 11.3 Top 10 Stores by Revenue

Horizontal bar chart.

Purpose:

> Which stores generate the highest revenue?

---

## 11.4 Bottom 10 Stores by Revenue

Horizontal bar chart.

Purpose:

> Which stores require management attention?

Displaying only the Top 10 would hide potentially important underperformers.

The dashboard should allow switching between:

- Top 10
- Bottom 10

or display both when space allows.

---

## 11.5 Revenue vs Invoice Volume

A scatter plot can be used to compare stores or regions.

Axes:

```text
X = Number of Invoices
Y = Revenue
```

Optional bubble size:

```text
Store Count
```

This identifies:

- high-volume / high-revenue areas
- high-volume / low-revenue areas
- low-volume / high-value areas
- low-volume / low-revenue areas

---

# 12. Store / Regional Drill-Down

Primary hierarchy:

```text
Region
 ↓
State
 ↓
City
 ↓
Store
```

This allows users to start at a regional level and progressively identify the source of performance differences.

---

# 13. Dashboard 3 — Product Performance

## 13.1 Purpose

The Product Dashboard analyzes product sales, product hierarchy and profitability.

It should answer:

> Which products generate the most revenue?

> Which categories and subcategories are driving the business?

> Which products generate the best margins?

> Are high-revenue products also high-margin products?

---

# 14. Product KPIs

Four headline KPIs are recommended.

### KPI 1 — Product Revenue

Revenue generated by products during the selected period.

---

### KPI 2 — Units Sold

Total quantity sold.

---

### KPI 3 — Average Selling Price

```text
Average Selling Price =
Total Revenue / Total Units Sold
```

---

### KPI 4 — Gross Margin %

```text
Gross Margin =
Revenue - Product Cost
```

```text
Gross Margin % =
Gross Margin / Revenue
```

Product cost is derived from the product cost attribute and quantity sold.

---

# 15. Product Filters

### Mandatory

- Year
- Month
- Date range

### Product hierarchy

- Category
- Subcategory
- Product
- Brand

### Additional

- Region
- State
- Store
- Customer Loyalty Tier

---

# 16. Product Visualizations

## 16.1 Product Revenue Trend

Line chart showing product revenue over time.

Users should be able to filter by:

- Category
- Subcategory
- Brand
- Product

---

## 16.2 Top 10 Categories by Revenue

Horizontal bar chart.

Hierarchy:

```text
Category
 ↓
Subcategory
```

---

## 16.3 Top 10 Products by Revenue

Horizontal bar chart.

Purpose:

> Which individual products generate the most revenue?

---

## 16.4 Top / Bottom Products by Margin

The dashboard should allow users to identify products with strong or weak profitability.

Possible views:

- Top 10 products by Gross Margin %
- Bottom 10 products by Gross Margin %

---

## 16.5 Revenue vs Gross Margin

Recommended scatter plot.

Axes:

```text
X = Revenue
Y = Gross Margin %
```

This identifies four analytical groups:

```text
High Revenue / High Margin
        → Strong performers

High Revenue / Low Margin
        → Important products requiring investigation

Low Revenue / High Margin
        → Potential growth opportunities

Low Revenue / Low Margin
        → Potential rationalization candidates
```

---

# 17. Product Drill-Down

Primary hierarchy:

```text
Category
 ↓
Subcategory
 ↓
Product
```

The dashboard should support progressive analysis without displaying invoice-level detail.

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
| 3–4 KPIs | ✅ | ✅ | ✅ | ✅ |
| Mandatory time filter | ✅ | ✅ | ✅ | ✅ |
| Trend analysis | ✅ | ✅ | ✅ | ✅ |
| Top 10 analysis | ✅ | ✅ | ✅ | Optional |
| Bottom 10 analysis | Optional | ✅ | ✅ | Optional |
| Geographic map | Optional | ✅ | ❌ | Optional |
| Drill-down | ✅ | ✅ | ✅ | ✅ |
| Detailed grid | ❌ | ❌ | ❌ | ✅ |
| Individual source record | ❌ | ❌ | ❌ | ✅ |
| Remediation workflow | ❌ | ❌ | ❌ | ✅ |

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

The dashboards will consume the Gold dimensional model defined in:

`09_gold_model_design.md`

The principal business tables are:

```text
dim_date
dim_geography
dim_customer
dim_store
dim_category
dim_subcategory
dim_product

fact_invoice
fact_invoice_item
```

The Data Quality Dashboard additionally uses:

```text
dim_data_quality_rule
fact_data_quality_issue
```

and the quarantine datasets where detailed remediation attributes are required.

---

# 28. Measures and Derived Metrics

The following analytical measures are required or expected.

## Sales

```text
Total Revenue
Total Invoices
Average Invoice Value
Revenue Growth %
```

## Store / Regional

```text
Revenue
Revenue Growth %
Average Invoice Value
Revenue per Store
```

## Product

```text
Product Revenue
Units Sold
Average Selling Price
Gross Margin
Gross Margin %
```

## Data Quality

```text
Total Records
Valid Records
Incorrect Records
Data Quality %
Incorrect Records %
Total DQ Issues
Affected Records
```

The final Power BI measure definitions must be reconciled against Gold calculations.

---

# 29. Aggregation Requirements

The dashboard requirements defined here will drive the next design step:

`11_gold_aggregations_design.md`

Aggregate tables must not be created simply because aggregation is technically possible.

For each dashboard requirement, the next stage must determine:

1. Can the Gold base model answer the requirement efficiently?
2. If not, what aggregate is required?
3. What is the required grain?
4. Which dimensions are required?
5. Which measures are required?
6. What is the refresh strategy?
7. How will the aggregate be reconciled against the base facts?

The base Gold dimensional model remains the authoritative analytical foundation.

---

# 30. Performance Principle

The objective is not to create the largest possible number of aggregate tables.

The objective is to create only the aggregates that provide meaningful benefits for:

- Power BI query performance
- repeated analytical workloads
- high-volume fact analysis
- common dashboard visualizations
- predictable refresh behavior

The project should prefer the Gold base model where it provides adequate performance and flexibility.

---

# 31. BI Data Integrity

Power BI must not silently correct or reinterpret data.

Business rules belong in:

```text
Silver
    ↓
Data Quality / Conformance
    ↓
Gold
```

Power BI should consume the resulting trusted analytical model.

For example:

```text
NULL customer_id
```

represents an anonymous transaction and must remain analytically distinguishable from:

```text
Invalid customer reference
```

The first is a valid business condition.

The second is a data-quality problem.

---

# 32. Final Dashboard Architecture

The final BI layer will contain four primary dashboards:

```text
                         POWER BI
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
          ▼                 ▼                 ▼
     EXECUTIVE       STORE / REGIONAL     PRODUCT
          │                 │                 │
          │                 │                 │
          └─────────────────┼─────────────────┘
                            │
                            ▼
                     DATA QUALITY
                            │
                            ▼
                  Remediation Detail
```

The first three dashboards are analytical and summarized.

The Data Quality Dashboard is analytical plus operational/remediation-oriented.

---

# 33. Dashboard Design Summary

## Executive

**Question:**

> How is the business performing?

Focus:

- revenue
- transaction volume
- average invoice value
- growth
- time trends
- top cities
- category performance

No detailed transaction grid.

---

## Store / Regional

**Question:**

> Where is the business performing well or poorly?

Focus:

- geographic performance
- regional trends
- state/city performance
- store rankings
- top/bottom stores
- revenue vs transaction volume

Includes a geographic map.

No detailed transaction grid.

---

## Product

**Question:**

> Which products and categories drive revenue and profitability?

Focus:

- revenue
- units
- selling price
- margin
- category hierarchy
- top/bottom products
- revenue vs margin

No detailed transaction grid.

---

## Data Quality

**Question:**

> What is wrong with the data, how significant is it, and exactly which records must be corrected?

Focus:

- data-quality percentage
- incorrect-record percentage
- DQ issues
- affected records
- error types
- severity
- trends
- source tables
- remediation grid

Detailed records are intentionally permitted.

---

# 34. Next Step

The BI requirements defined in this document are now the input to:

```text
11_gold_aggregations_design.md
```

The next stage must translate these dashboard requirements into a precise Gold aggregation strategy.

For every proposed aggregate, the design must specify:

- business purpose
- source fact(s)
- grain
- dimensions
- measures
- required time granularity
- refresh strategy
- incremental-loading strategy
- reconciliation method
- Power BI consumer

No aggregate table should be implemented until its analytical purpose has been established.