# Gold Model Design

## 1. Purpose

The Gold layer is the analytical and business-facing layer of the US Retail Medallion architecture.

Its purpose is to provide a stable, business-oriented dimensional model that:

* Represents validated Silver data in an analytical structure.
* Provides conformed dimensions for reporting and analytics.
* Preserves historical dimension changes through Slowly Changing Dimension Type 2 (SCD2) where required.
* Provides clear and controlled fact-table grains.
* Supports historical analysis.
* Provides a dedicated Data Quality model for technical monitoring and remediation.
* Serves as the foundation for the subsequent Gold aggregation layer and Power BI dashboards.

The Gold layer does not replace the Silver layer.

Silver remains the authoritative cleaned and conformed representation of the source data, while Gold restructures that information for analytical consumption.

---

# 2. Gold Layer Principles

The following principles govern the Gold model.

### 2.1 Business-oriented model

Gold tables are designed around analytical use cases rather than around the physical structure of the PostgreSQL source.

### 2.2 Surrogate keys

Gold dimensions use surrogate keys as primary keys.

Surrogate keys are independent of the source-system identifiers and allow the model to maintain multiple historical versions of the same business entity.

### 2.3 Natural/business keys

Source identifiers remain in the dimensions as business/natural keys.

Examples:

* `customer_id`
* `store_id`
* `product_id`
* `invoice_id`

Natural keys are used to identify business entities during loading and SCD2 processing.

### 2.4 Historical preservation

Customer, store and product dimensions use SCD Type 2.

When a tracked attribute changes, the previous dimension version remains available and a new version is created.

### 2.5 Fact-table grain must remain explicit

Each fact table represents a clearly defined business event.

* `fact_invoice`: one row per invoice/header.
* `fact_invoice_item`: one row per invoice line.

Attributes that belong to the invoice header must not be duplicated in the invoice-item fact.

### 2.6 No unnecessary denormalization

Attributes should belong to the dimension that owns them.

For example, geographic attributes belong to `dim_geography`, not to `dim_customer` or `dim_store`.

### 2.7 Source data is not corrected in Gold

Silver is responsible for data-quality validation and quarantine.

Gold consumes valid Silver records and transforms their structure into an analytical model.

Gold must not silently repair invalid source data.

---

# 3. Gold Base Model

The Gold business model consists of:

### Dimensions

1. `dim_date`
2. `dim_geography`
3. `dim_customer`
4. `dim_store`
5. `dim_category`
6. `dim_subcategory`
7. `dim_product`

### Facts

8. `fact_invoice`
9. `fact_invoice_item`

### Data Quality model

10. `dim_data_quality_rule`
11. `fact_data_quality_issue`

### Quarantine detail tables

12. `quarantined_customers`
13. `quarantined_products`
14. `quarantined_stores`
15. `quarantined_invoices`
16. `quarantined_invoice_items`

---

# 4. Core Dimensional Model

The core analytical relationships are:

```text
                    dim_date
                       |
                       |
                       v
dim_customer ----> fact_invoice <---- dim_store
      |                 |
      |                 |
      v                 v
dim_geography    fact_invoice_item
                       |
                       v
                  dim_product
                       |
                       v
                dim_subcategory
                       |
                       v
                  dim_category
```

`dim_geography` is a shared dimension referenced by both `dim_customer` and `dim_store`.

There is only one physical `dim_geography` table.

The fact tables do not contain `geography_key`.

---

# 5. Dimension: dim_date

## Grain

One row per calendar date.

## Primary key

`date_key`

The recommended representation is an integer in `YYYYMMDD` format.

Example:

```text
20261006
```

## Natural key

`full_date`

## SCD

Not SCD2.

Calendar dates are static reference data.

## Main attributes

```text
date_key
full_date
day
day_of_week
day_name
week_of_year
month
month_name
quarter
year
year_month
year_quarter
is_weekend
is_month_start
is_month_end
```

---

# 6. Dimension: dim_geography

## Purpose

`dim_geography` centralizes geographic attributes used by customer and store dimensions.

This avoids duplicating geographic attributes across multiple dimensions and provides a reusable geographic hierarchy for analytical reporting.

## Grain

One row per unique geographic location.

## Primary key

`geography_key`

## Natural key

```text
(region, state, city, zip_code)
```

## Attributes

```text
geography_key
region
state
city
zip_code
_insert_datetime_utc
_update_datetime_utc
```

## SCD

Not SCD2 initially.

The geographic entity itself is treated as reference data.

Historical changes in a customer's or store's geography are represented by a new SCD2 version of the corresponding customer or store dimension.

## Default member

```text
geography_key = -1
```

represents an unknown or unavailable geography.

---

# 7. Dimension: dim_customer

## Grain

One row per customer version.

A single business customer can therefore have multiple rows because of SCD2 history.

## Primary key

`customer_key`

## Natural/business key

`customer_id`

## Foreign keys

```text
geography_key → dim_geography.geography_key
```

## Attributes

```text
customer_key
customer_id
first_name
last_name
email
phone
street_address
loyalty_tier
join_date
geography_key

effective_from
effective_to
is_current

_insert_datetime_utc
_update_datetime_utc
```

Geographic attributes such as region, state, city and ZIP are not duplicated here.

They are obtained through:

```text
dim_customer.geography_key
        ↓
dim_geography
```

## SCD2 merge key

```text
customer_id
```

## SCD2 tracked attributes

The following attributes are candidates for change detection:

```text
first_name
last_name
email
phone
street_address
loyalty_tier
join_date
geography_key
```

A change in `geography_key` therefore creates a new customer version.

## Default member

Customer key `-1` is reserved for invoices without an assigned customer.

```text
customer_key = -1
customer_id  = 'NO_CUSTOMER'
first_name   = 'No customer assigned'
last_name    = 'No customer assigned'
```

This is an accepted business condition, not a Data Quality error.

---

# 8. Dimension: dim_store

## Grain

One row per store version.

## Primary key

`store_key`

## Natural/business key

`store_id`

## Foreign keys

```text
geography_key → dim_geography.geography_key
```

## Attributes

```text
store_key
store_id
store_name
manager_name
opened_date
square_footage
geography_key

effective_from
effective_to
is_current

_insert_datetime_utc
_update_datetime_utc
```

Geographic attributes such as region, state, city and ZIP are not duplicated here.

They are obtained through:

```text
dim_store.geography_key
        ↓
dim_geography
```

## SCD2 merge key

```text
store_id
```

## SCD2 tracked attributes

```text
store_name
manager_name
opened_date
square_footage
geography_key
```

A change in geography therefore creates a new store version.

---

# 9. Dimension: dim_category

## Grain

One row per product category.

## Primary key

`category_key`

## Natural key

`category_name`

## Attributes

```text
category_key
category_name
_insert_datetime_utc
_update_datetime_utc
```

## SCD

Not SCD2 initially.

---

# 10. Dimension: dim_subcategory

## Grain

One row per subcategory within a category.

## Primary key

`subcategory_key`

## Natural key

```text
(category_name, subcategory_name)
```

## Foreign key

```text
category_key → dim_category.category_key
```

## Attributes

```text
subcategory_key
subcategory_name
category_key
_insert_datetime_utc
_update_datetime_utc
```

## SCD

Not SCD2 initially.

---

# 11. Dimension: dim_product

## Grain

One row per product version.

## Primary key

`product_key`

## Natural/business key

`product_id`

## Foreign key

```text
subcategory_key → dim_subcategory.subcategory_key
```

## Attributes

```text
product_key
product_id
sku
product_name
subcategory_key
brand
unit_price
cost
is_active

effective_from
effective_to
is_current

_insert_datetime_utc
_update_datetime_utc
```

## SCD2 merge key

```text
product_id
```

## SCD2 tracked attributes

```text
sku
product_name
subcategory_key
brand
unit_price
cost
is_active
```

A change in product classification, such as a move to another subcategory, creates a new product version.

---

# 12. Fact: fact_invoice

## Grain

One row per invoice/header.

## Primary key

`invoice_key`

## Natural/business key

`invoice_id`

## Foreign keys

```text
date_key
customer_key
store_key
```

References:

```text
date_key     → dim_date.date_key
customer_key → dim_customer.customer_key
store_key    → dim_store.store_key
```

## Attributes

```text
invoice_key
invoice_id
date_key
customer_key
store_key
payment_method

_insert_datetime_utc
_update_datetime_utc
```

## Important design decision

`fact_invoice` does not contain:

```text
geography_key
```

Customer and store geography are reached through their respective dimensions.

```text
fact_invoice
    |
    +-- customer_key → dim_customer → geography_key → dim_geography
    |
    +-- store_key    → dim_store    → geography_key → dim_geography
```

This keeps the fact table at invoice grain and avoids duplicating dimensional relationships.

---

# 13. Fact: fact_invoice_item

## Grain

One row per invoice line.

## Primary key

`invoice_item_key`

## Natural/business key

```text
(invoice_id, line_item)
```

## Foreign keys

```text
invoice_key
product_key
```

References:

```text
invoice_key → fact_invoice.invoice_key
product_key → dim_product.product_key
```

## Attributes

```text
invoice_item_key
invoice_key
product_key
line_item
quantity
unit_price
discount

_insert_datetime_utc
_update_datetime_utc
```

## Important design decision

`fact_invoice_item` does not contain:

```text
date_key
customer_key
store_key
geography_key
```

Those relationships belong to the invoice header.

The invoice line reaches them through:

```text
fact_invoice_item
       |
       └── invoice_key
               ↓
         fact_invoice
               |
        ┌──────┼──────┐
        ↓      ↓      ↓
       date  customer store
```

---

# 14. Anonymous / No-Customer Invoices

Anonymous transactions are valid business transactions.

The source Silver invoice may contain:

```text
customer_id = NULL
```

Gold represents this using the customer default member:

```text
customer_key = -1
```

The corresponding dimension member is:

```text
customer_key = -1
customer_id = 'NO_CUSTOMER'
first_name = 'No customer assigned'
last_name = 'No customer assigned'
```

This does not represent a quarantined record and does not indicate a Data Quality problem.

---

# 15. SCD Type 2 Strategy

SCD2 is used for:

```text
dim_customer
dim_store
dim_product
```

The general model is:

```text
effective_from
effective_to
is_current
```

## Effective-date interval

The validity interval follows the convention:

```text
[effective_from, effective_to)
```

Meaning:

```text
effective_from <= date < effective_to
```

The current version uses an open-ended future boundary represented by:

```text
9999-12-31 23:59:59.999999
```

with:

```text
is_current = true
```

## Change processing

For a given natural key:

### New entity

Insert a new dimension version.

### Existing entity with no tracked changes

Keep the current version unchanged.

### Existing entity with tracked changes

1. Close the existing version.
2. Set `effective_to` to the new version's effective start.
3. Set `is_current = false`.
4. Insert the new version.
5. Set the new version's `is_current = true`.

The historical version is never overwritten.

---

# 16. SCD2 Change Detection

SCD2 change detection should compare only business attributes that are designated as tracked attributes.

Technical metadata must not participate in change detection.

Surrogate keys must not participate in change detection.

Effective dates must not participate in change detection.

An attribute hash can be used to efficiently detect changes.

Conceptually:

```text
Natural key
     +
Tracked business attributes
     ↓
Attribute hash
     ↓
Compare against current dimension version
```

A different hash indicates a business change.

The implementation must normalize NULL values consistently before calculating hashes.

---

# 17. Historical Fact-to-Dimension Resolution

Facts must resolve the correct SCD2 dimension version rather than always using the current dimension row.

For example, an invoice for customer `C001` must resolve to the customer version valid for the invoice date.

Conceptually:

```text
customer_id = invoice.customer_id

AND

customer.effective_from <= invoice_date

AND

invoice_date < customer.effective_to
```

The same principle applies to:

* Store
* Product

For invoice items, the invoice date is obtained through `fact_invoice`.

This ensures that historical transactions remain associated with the correct historical product, customer and store versions.

---

# 18. Data Quality Model

The Gold Data Quality model is separate from the commercial star schema.

## dim_data_quality_rule

### Grain

One row per Data Quality rule.

### Primary key

`dq_rule_key`

### Natural key

`dq_error_code`

### Attributes

```text
dq_rule_key
dq_error_code
dq_rule_name
dq_rule_description
source_table
severity

_insert_datetime_utc
_update_datetime_utc
```

---

# 19. fact_data_quality_issue

## Grain

One row per Data Quality issue detected for a quarantined source record.

## Primary key

`dq_issue_key`

## Logical natural key

```text
(source_table, source_row_id, dq_error_code)
```

## Foreign key

```text
dq_rule_key → dim_data_quality_rule.dq_rule_key
```

## Attributes

```text
dq_issue_key
source_table
source_row_id
source_file_name
dq_rule_key
dq_error_code
dq_error_description
_dq_status
_quarantine_datetime_utc
_insert_datetime_utc
_update_datetime_utc
```

This model allows a source record to be associated with multiple DQ issues when required.

---

# 20. Gold Quarantine Tables

The following tables preserve the actual quarantined records:

```text
quarantined_customers
quarantined_products
quarantined_stores
quarantined_invoices
quarantined_invoice_items
```

These tables are technical/remediation datasets.

They are not dimensions and do not participate in the commercial star schema.

Their purpose is to preserve:

* The problematic source record.
* Original/raw values where applicable.
* The quarantine metadata.
* The Data Quality rule and error information.
* The information required by technical users to investigate the problem.

The Gold Data Quality dashboard will use these datasets together with `fact_data_quality_issue` and `dim_data_quality_rule`.

---

# 21. Primary Keys, Foreign Keys and Natural Keys

| Table                     | Primary Key        | Natural / Business Key                       | Foreign Keys                            |
| ------------------------- | ------------------ | -------------------------------------------- | --------------------------------------- |
| `dim_date`                | `date_key`         | `full_date`                                  | —                                       |
| `dim_geography`           | `geography_key`    | `(region,state,city,zip_code)`               | —                                       |
| `dim_customer`            | `customer_key`     | `customer_id`                                | `geography_key`                         |
| `dim_store`               | `store_key`        | `store_id`                                   | `geography_key`                         |
| `dim_category`            | `category_key`     | `category_name`                              | —                                       |
| `dim_subcategory`         | `subcategory_key`  | `(category_name,subcategory_name)`           | `category_key`                          |
| `dim_product`             | `product_key`      | `product_id`                                 | `subcategory_key`                       |
| `fact_invoice`            | `invoice_key`      | `invoice_id`                                 | `date_key`, `customer_key`, `store_key` |
| `fact_invoice_item`       | `invoice_item_key` | `(invoice_id,line_item)`                     | `invoice_key`, `product_key`            |
| `dim_data_quality_rule`   | `dq_rule_key`      | `dq_error_code`                              | —                                       |
| `fact_data_quality_issue` | `dq_issue_key`     | `(source_table,source_row_id,dq_error_code)` | `dq_rule_key`                           |

---

# 22. Surrogate Key Convention

Gold dimensions use surrogate integer keys.

The value:

```text
-1
```

is reserved for the default/unknown member where applicable.

In particular:

```text
dim_customer:
-1 = No customer assigned
```

The same convention can be used for unknown/default dimension members where the business model requires one.

Surrogate keys must never be derived from source-system identifiers.

---

# 23. Referential Integrity Expectations

Gold should only consume valid Silver records.

Expected relationships include:

```text
fact_invoice.date_key
    → dim_date.date_key

fact_invoice.customer_key
    → dim_customer.customer_key

fact_invoice.store_key
    → dim_store.store_key

dim_customer.geography_key
    → dim_geography.geography_key

dim_store.geography_key
    → dim_geography.geography_key

fact_invoice_item.invoice_key
    → fact_invoice.invoice_key

fact_invoice_item.product_key
    → dim_product.product_key

dim_product.subcategory_key
    → dim_subcategory.subcategory_key

dim_subcategory.category_key
    → dim_category.category_key

fact_data_quality_issue.dq_rule_key
    → dim_data_quality_rule.dq_rule_key
```

Unexpected referential-integrity failures during Gold loading must not be silently ignored.

---

# 24. Revenue Calculation

The Silver source model does not contain an invoice-level revenue field.

Revenue is therefore derived from invoice lines.

The base calculation is:

```text
line_revenue =
    quantity * unit_price
    - discount
```

Invoice revenue is the sum of its invoice-line revenue.

This calculation will be implemented in the analytical/aggregation layer rather than artificially adding a revenue column to `fact_invoice`.

---

# 25. Gold Aggregation Layer

The dimensional model described in this document is the **Gold Base Model**.

It is intentionally separated from the future Gold aggregation/serving layer.

Aggregation tables will not be created arbitrarily.

The next design phase will first define:

* Power BI dashboards.
* Business questions.
* KPIs.
* Filters.
* Drill-down requirements.
* Required dimensions.
* Required measures.
* Required analytical grains.

Based on those requirements, the project will determine which aggregated Gold tables are necessary.

The process will therefore be:

```text
Gold Base Model
      ↓
BI / Dashboard Requirements
      ↓
Aggregation Requirements
      ↓
Gold Aggregation Tables
      ↓
Power BI
```

This prevents unnecessary duplication and ensures that the serving layer is designed around actual analytical requirements.

---

# 26. Final Gold Architecture

The final conceptual architecture is:

```text
                           ┌───────────────┐
                           │   dim_date    │
                           └───────┬───────┘
                                   │
                                   ▼
┌────────────────┐          ┌───────────────┐          ┌────────────────┐
│ dim_customer   │─────────▶│ fact_invoice  │◀─────────│   dim_store    │
│                │          │               │          │                │
│ geography_key ─┼───┐      │ customer_key  │          │ geography_key ─┼──┐
└────────────────┘   │      │ store_key     │          └────────────────┘  │
                     │      └───────┬───────┘                              │
                     │              │                                      │
                     │              ▼                                      │
                     │      ┌──────────────────┐                           │
                     │      │fact_invoice_item │                           │
                     │      │                  │                           │
                     │      │ invoice_key      │                           │
                     │      │ product_key      │                           │
                     │      └────────┬─────────┘                           │
                     │               │                                     │
                     │               ▼                                     │
                     │       ┌───────────────┐                             │
                     │       │ dim_product   │                             │
                     │       └───────┬───────┘                             │
                     │               │                                     │
                     │               ▼                                     │
                     │       ┌────────────────┐                            │
                     │       │dim_subcategory │                            │
                     │       └───────┬────────┘                            │
                     │               │                                     │
                     │               ▼                                     │
                     │       ┌───────────────┐                             │
                     │       │ dim_category  │                             │
                     │       └───────────────┘                             │
                     │                                                     │
                     └────────────────┐        ┌───────────────────────────┘
                                      ▼        ▼
                               ┌──────────────────┐
                               │  dim_geography   │
                               │                  │
                               │ region           │
                               │ state            │
                               │ city             │
                               │ zip_code         │
                               └──────────────────┘
```

The Data Quality model is independent:

```text
┌──────────────────────────┐
│ dim_data_quality_rule    │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│ fact_data_quality_issue  │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│ Gold quarantine tables   │
└──────────────────────────┘
```

---

# 27. Next Development Step

The Gold conceptual model is now considered stable.

The next phase is **not yet Gold implementation**.

The next step is to define the Power BI analytical requirements:

1. Define the dashboards.
2. Define the KPIs for each dashboard.
3. Define dimensions, filters and drill-downs.
4. Define required analytical grains.
5. Determine which requirements can be served directly by the Gold Base Model.
6. Identify the requirements that justify pre-aggregated Gold tables.
7. Design the aggregation tables.
8. Then finalize the physical Gold implementation and loading processes.

This ensures that the Gold serving layer is driven by actual analytical requirements rather than by assumptions about future reports.
