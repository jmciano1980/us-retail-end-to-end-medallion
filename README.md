# US Retail End-to-End Data Engineering Platform

End-to-end data engineering portfolio project simulating a large US retail company, from an operational PostgreSQL source system through a Databricks Medallion architecture and ultimately to analytical models and Power BI dashboards.

The project is intentionally built with imperfect synthetic source data so that data quality, quarantine, lineage, reconciliation, dimensional modeling, historical tracking and analytical consumption can be demonstrated rather than hidden.

---

## 1. Project Status

| Milestone | Description | Status |
|---|---|---|
| M1 | Infrastructure / PostgreSQL | ✅ Completed |
| M2 | Synthetic data generation | ✅ Completed |
| M3 | PostgreSQL source loading | ✅ Completed |
| M4 | PostgreSQL → Parquet extraction | ✅ Completed |
| M5 | Parquet → Databricks RAW upload | ✅ Completed |
| M6 | RAW → Bronze ingestion | ✅ Completed |
| M7 | Bronze → Silver / Data Quality / Quarantine | ✅ Completed |
| M8 | Gold analytical model | 🚧 In progress |
| M9 | Power BI dashboards | ⬜ Planned |
| M10 | Final documentation / portfolio packaging | ⬜ Planned |

### Current implementation boundary

```text
PostgreSQL
    ↓
M4 — Dated Parquet extraction
    ↓
M5 — Databricks RAW Volume
    ↓
M6 — Bronze Delta
    ↓
M7 — Silver / Data Quality / Quarantine
    ↓
M8 — Gold analytical model
    ↓
M9 — Power BI
    ↓
M10 — Final documentation / portfolio packaging
```

The project is currently entering **M8 — Gold analytical model**.

---

## 2. Business Scenario

The project simulates a US retail company operating approximately:

- 50 stores
- 2,500 products
- 100,000 customers
- approximately 55 million invoices
- multiple invoice line items per invoice

The synthetic source contains intentional data-quality problems, including:

- NULL values
- missing required attributes
- invalid dates
- invalid numeric values
- invalid quantities
- invalid monetary values
- duplicate business identifiers
- invalid foreign-key references
- incomplete transactions

These problems are deliberately preserved through extraction, RAW and Bronze.

A NULL `customer_id` on an invoice is intentionally treated as a legitimate anonymous / unidentified customer transaction, not as a Silver data-quality error.

A supplied non-NULL `customer_id` that does not identify exactly one valid customer is treated as a referential-integrity error and is quarantined.

---

## 3. Medallion Architecture

```text
                         PostgreSQL
                              │
                              ▼
                    M4 — Parquet Extraction
                              │
                              ▼
                    M5 — Databricks RAW
                              │
                              ▼
                     M6 — Bronze Delta
                              │
                              ▼
                 M7 — Data Quality / DQ
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
          Silver / Valid             Quarantine
          Typed / Conformed          Invalid / DQ
                 │                         │
                 └────────────┬────────────┘
                              ▼
                    M8 — Gold Analytics
                              │
                              ▼
                       M9 — Power BI
                              │
                              ▼
                 M10 — Portfolio Finalization
```

---

# 4. Layer Responsibilities

## PostgreSQL

PostgreSQL is the simulated operational System of Record.

Source tables:

```text
retail_raw.stores
retail_raw.products
retail_raw.customers
retail_raw.invoices
retail_raw.invoice_items
```

The source schema is intentionally permissive so that downstream layers can demonstrate data-quality handling.

---

## Parquet / M4

Parquet is an extraction and transport format, not a transformation layer.

M4 preserves:

- source column names
- NULL values
- invalid values
- duplicate business identifiers
- referential-integrity problems
- source row content
- source semantics

No business correction or deduplication is performed during extraction.

Technical controls include:

- repeatable-read snapshot
- server-side cursor
- batch extraction
- multiple Parquet parts
- row-count reconciliation
- extraction manifests
- atomic file publication
- extraction-date-based filenames

Example:

```text
customers_2026_09_29_part-00001.parquet
invoice_items_2026_09_29_part-00001.parquet
```

---

## RAW / M5

RAW is the immutable Databricks landing area.

Validated Volume:

```text
/Volumes/workspace/retail/raw
```

M5 preserves extracted Parquet filenames and does not apply business transformations.

RAW does not:

- clean data
- change business values
- deduplicate
- merge files
- apply business rules
- overwrite existing files by default

---

## Bronze / M6

Bronze is a structured Delta representation of the source.

Bronze preserves source business values and adds technical metadata:

```text
_insert_datetime_utc
_update_datetime_utc
_source_file_name
_source_row_id
```

The technical source-row identity is not a business key.

This is required because Bronze must preserve duplicate and invalid business identifiers.

Bronze does not perform business cleansing.

---

## Silver / M7

Silver is the first business-data-quality and conformance layer.

Responsibilities include:

- required-field validation
- type casting
- date validation
- numeric validation
- business-key duplicate detection
- referential-integrity validation
- invoice-level transaction validation
- quarantine of invalid records
- lineage preservation
- reconciliation
- conformance

Bronze is never modified by M7.

Valid records enter trusted Silver tables.

Invalid records are retained in quarantine tables with their source lineage and DQ information.

---

# 5. M6 Bronze Objects

M6 creates:

```text
workspace.bronze.stores
workspace.bronze.products
workspace.bronze.customers
workspace.bronze.invoices
workspace.bronze.invoice_items
workspace.bronze._bronze_file_ingestion_log
```

M6 code:

```text
05_databricks/bronze/
├── 01_create_bronze_tables.sql
└── 02_load_bronze_tables.py
```

Documentation:

```text
docs/07_bronze_creation_and_loading.md
```

The Bronze loader runs as a Databricks Python script task.

---

# 6. M7 Silver Objects

M7 creates the Silver schema:

```text
workspace.silver
```

## Clean tables

```text
workspace.silver._silver_file_ingestion_log
workspace.silver.customers
workspace.silver.invoice_items
workspace.silver.invoices
workspace.silver.products
workspace.silver.stores
```

## Quarantine tables

```text
workspace.silver.quarantined_customers
workspace.silver.quarantined_invoice_items
workspace.silver.quarantined_invoices
workspace.silver.quarantined_products
workspace.silver.quarantined_stores
```

Silver and quarantine business tables preserve:

```text
_insert_datetime_utc
_update_datetime_utc
_source_file_name
_source_row_id
```

Quarantine tables additionally contain:

```text
_quarantine_datetime_utc
_dq_status
_dq_error_code
_dq_error_description
```

---

# 7. M7 Data Quality Rules

## Customers

Required:

```text
customer_id
first_name
last_name
street_address
city
state
zip_code
```

Additional rules:

- `customer_id` must be unique
- `join_date`, when populated, must be a valid date
- blank strings are treated as missing values

---

## Products

Required:

```text
product_id
sku
product_name
category
subcategory
brand
unit_price
cost
```

Additional rules:

- `product_id` must be unique
- `unit_price` and `cost` must be numeric
- `unit_price >= 0`
- `cost >= 0`

---

## Stores

Required:

```text
store_id
store_name
city
state
zip_code
region
manager_name
opened_date
```

Additional rules:

- `store_id` must be unique
- `opened_date` must be valid
- `square_footage`, when populated, must be numeric and non-negative

---

## Invoices

Required:

```text
invoice_id
store_id
invoice_date
payment_method
```

Customer identification is optional at transaction time.

Therefore:

```text
customer_id = NULL
```

is accepted and represents an anonymous / unidentified customer.

A supplied non-NULL customer ID must reference exactly one valid customer.

Additional rules:

- `invoice_id` must be unique
- `invoice_date` must be valid
- `store_id` must reference exactly one valid store
- an invoice must contain at least one valid item

---

## Invoice Items

Required:

```text
invoice_id
line_item
product_id
quantity
unit_price
```

Additional rules:

- `line_item` must be a positive integer
- `quantity` must be a positive integer
- `product_id` must reference exactly one valid product
- `unit_price` must be numeric and non-negative
- `discount`, when populated, must be numeric and non-negative
- `(invoice_id, line_item)` must be unique

---

# 8. Invoice Atomicity

Invoice integrity is enforced at transaction level.

An invoice enters clean Silver only when:

```text
Valid header
AND
Valid store reference
AND
Customer_ID is NULL
    OR
Valid customer reference
AND
At least one item exists
AND
Every item is valid
```

If one invoice item is invalid:

```text
Invalid item
     ↓
Invoice header → Quarantine
All invoice items → Quarantine
```

If the invoice header is invalid:

```text
Invalid header
     ↓
Invoice header → Quarantine
All invoice items → Quarantine
```

This prevents partially valid transactions from entering the trusted Silver dataset.

---

# 9. Referential Integrity

M7 validates:

```text
invoices.store_id
        →
workspace.silver.stores.store_id
```

```text
invoices.customer_id
        →
workspace.silver.customers.customer_id
```

Only supplied customer IDs are validated.

NULL customer IDs represent anonymous transactions and are accepted.

```text
invoice_items.product_id
        →
workspace.silver.products.product_id
```

A reference is valid only when the target identifier corresponds to exactly one valid entity.

A duplicated target identifier is therefore treated as ambiguous and invalid.

---

# 10. Silver Type Standardization

Silver converts Bronze source strings into business-oriented types:

```text
customers.join_date          DATE
products.unit_price          DECIMAL(18,2)
products.cost                DECIMAL(18,2)
stores.opened_date           DATE
stores.square_footage        BIGINT
invoices.invoice_date        DATE
invoice_items.line_item      BIGINT
invoice_items.quantity       BIGINT
invoice_items.unit_price     DECIMAL(18,2)
invoice_items.discount       DECIMAL(18,2)
```

Malformed values are quarantined rather than silently converted or discarded.

---

# 11. M7 DQ Diagnostic and Dry Run

The Silver PySpark loader supports:

```text
--dry-run
```

The dry run produces diagnostics without writing Silver or quarantine data.

The diagnostic output includes:

- Bronze counts
- Silver-valid counts
- quarantine counts
- quarantine rates
- root DQ reasons
- bad invoice-item counts
- invoices affected by bad items
- accepted anonymous transactions
- invalid supplied customer references
- invoice/item quarantine reconciliation
- representative samples
- direct-error rates versus final quarantine rates

The distinction between root errors and cascade quarantine is important.

For example:

```text
NULL customer_id
    → valid anonymous transaction
```

whereas:

```text
customer_id = supplied value
but customer does not exist
    → DQ error
    → quarantine
```

---

# 12. M7 Reconciliation

M7 maintains traceability from Bronze to one of two outcomes:

```text
Bronze record
     │
     ├── Valid
     │      ↓
     │    Silver
     │
     └── Invalid
            ↓
        Quarantine
```

For ordinary entity tables:

```text
Bronze rows = Silver rows + Quarantine rows
```

Invoice-level reconciliation also respects transaction atomicity.

If an invoice is invalid, all related invoice detail records follow the invoice into quarantine.

---

# 13. M7 Idempotency and Lineage

Bronze provides the physical source-row identity:

```text
_source_row_id
```

M7 uses this technical identity to prevent duplicate Silver records during Job retries.

Technical lineage remains available through:

```text
_source_file_name
_source_row_id
_insert_datetime_utc
_update_datetime_utc
```

Quarantine records retain the same source lineage.

---

# 14. Data Quality Analytics

Quarantine data is intentionally retained.

It will support a future Gold data-quality model and Power BI Data Quality dashboard.

Potential analytical metrics include:

- total records processed
- valid records
- quarantined records
- acceptance rate
- quarantine rate
- errors by rule
- errors by source table
- errors by store
- errors by state
- error trends over time
- most frequent data-quality violations

---

# 15. Gold / M8

M8 is the analytical modeling stage.

Gold consumes trusted Silver data and creates business-oriented analytical structures.

Gold does not replace Silver data-quality processing.

The Gold layer will provide:

- dimensional modeling
- fact tables
- conformed dimensions
- historical dimension tracking where required
- analytical aggregates
- data-quality analytical structures
- structures optimized for Power BI consumption

The Gold model is documented and implemented through the following sequence.

### M8 documentation and implementation sequence

```text
09_gold_model_design.md
        ↓
10_bi_requirements_and_dashboard_design.md
        ↓
11_gold_aggregations_design.md
        ↓
12_gold_creation_and_loading.md
```

These documents are **not separate milestones**.

They are four sequential design and implementation steps within **M8 — Gold**.

---

## 15.1 M8 Step 1 — Gold Model Design

Documentation:

```text
docs/09_gold_model_design.md
```

This document defines the conceptual Gold model, including:

### Business dimensions

```text
dim_date
dim_geography
dim_customer
dim_store
dim_category
dim_subcategory
dim_product
```

### Business facts

```text
fact_invoice
fact_invoice_item
```

### Data-quality model

```text
dim_data_quality_rule
fact_data_quality_issue
```

### Quarantine analytical datasets

```text
quarantined_customers
quarantined_products
quarantined_stores
quarantined_invoices
quarantined_invoice_items
```

The Gold model follows dimensional-modeling principles and uses surrogate keys for analytical dimensions.

Unknown/default dimension members use:

```text
-1
```

---

## 15.2 Geography Model

Geography is modeled as a shared dimension:

```text
dim_customer
      │
      ▼
dim_geography
      ▲
      │
dim_store
```

Facts do not directly reference geography.

The natural geography key is:

```text
(region, state, city, zip_code)
```

Customer-specific address information remains part of the customer dimension.

---

## 15.3 Product Hierarchy

The product hierarchy is:

```text
dim_category
      │
      ▼
dim_subcategory
      │
      ▼
dim_product
      │
      ▼
fact_invoice_item
```

The product dimension references the subcategory dimension.

---

## 15.4 Fact Grain

### `fact_invoice`

Grain:

```text
One row per invoice
```

Contains invoice-level analytical relationships such as:

```text
date_key
customer_key
store_key
payment_method
```

### `fact_invoice_item`

Grain:

```text
One row per invoice line
```

Contains:

```text
invoice_item_key
invoice_key
product_key
quantity
unit_price
discount
```

The invoice-item fact does not duplicate invoice-level dimension keys.

---

## 15.5 Revenue

Revenue is derived from invoice items:

```text
line_revenue =
    quantity * unit_price
    - COALESCE(discount, 0)
```

Invoice revenue is the sum of its valid invoice lines.

Revenue is not artificially added to `fact_invoice` when it can be derived from the invoice-item fact.

---

## 15.6 SCD Type 2

SCD Type 2 will be applied where historical dimension tracking is analytically meaningful.

Initial SCD2 dimensions:

```text
dim_customer
dim_store
dim_product
```

SCD2 dimensions contain:

```text
effective_from
effective_to
is_current
```

The active version uses:

```text
effective_to = 9999-12-31 23:59:59.999999
is_current = true
```

When a tracked attribute changes:

```text
Current version
      ↓
Close old version
      ↓
Insert new version
```

Historical versions remain immutable.

Facts must resolve the correct historical dimension version using the transaction date.

---

## 15.7 Anonymous Customers

Anonymous invoices are valid business transactions.

For an invoice where:

```text
customer_id IS NULL
```

Gold assigns:

```text
customer_key = -1
```

The default customer member represents:

```text
NO_CUSTOMER
No customer assigned
```

This is not a DQ error.

It allows sales analytics to include anonymous transactions while distinguishing them from identified-customer transactions.

---

# 16. BI Requirements / M8 Step 2

Documentation:

```text
docs/10_bi_requirements_and_dashboard_design.md
```

Before creating Gold aggregate tables, the project will define the analytical requirements for Power BI.

This document will specify:

- business questions
- dashboards
- KPIs
- dimensions
- filters
- drill-down paths
- time analysis
- required measures
- required data granularity
- source Gold tables
- performance considerations

The initial dashboard scope is expected to include:

### Executive / Sales Overview

Business performance and sales trends.

### Store / Regional Performance

Performance by store and geography.

### Product Performance

Product, category and subcategory analysis.

### Data Quality

Quality metrics and quarantine/error analysis.

The exact aggregate requirements will be derived from these dashboard requirements rather than created arbitrarily.

---

# 17. Gold Aggregations / M8 Step 3

Documentation:

```text
docs/11_gold_aggregations_design.md
```

Aggregate tables will be designed only after the BI requirements have been established.

The purpose of this step is to determine:

- which dashboards can query the Gold base model directly
- which analytical workloads require pre-aggregation
- the required grain of each aggregate
- required dimensions
- required measures
- refresh strategy
- incremental-processing strategy
- reconciliation strategy

The project will avoid unnecessary aggregate tables.

The base dimensional model remains the authoritative analytical foundation.

---

# 18. Gold Creation and Loading / M8 Step 4

Documentation:

```text
docs/12_gold_creation_and_loading.md
```

This document will define the physical implementation of the Gold layer.

It will cover:

- Gold schema creation
- dimension creation
- fact creation
- surrogate-key generation
- default/unknown members
- SCD Type 2 processing
- fact-to-dimension historical resolution
- Gold aggregate creation
- data-quality model creation
- incremental loading
- idempotency
- reconciliation
- validation
- Databricks Job execution

The implementation will consume Silver rather than Bronze.

---

# 19. M9 — Power BI

M9 is the Power BI implementation stage.

Documentation:

```text
docs/13_powerbi_implementation.md
```

M9 will cover:

- Power BI connection to Gold
- semantic model configuration
- relationships
- measures
- KPI definitions
- dashboard construction
- filters and slicers
- drill-downs
- validation against Gold
- performance considerations

Planned dashboards:

```text
1. Executive / Sales Overview
2. Store / Regional Performance
3. Product Performance
4. Data Quality
```

The Power BI dashboards consume the Gold analytical layer.

Power BI should not independently recreate business rules that belong in Silver or Gold.

---

# 20. M10 — Final Documentation / Portfolio Packaging

M10 is the final portfolio stage.

It will consolidate:

- architecture documentation
- implementation documentation
- validation evidence
- reconciliation results
- screenshots
- Power BI dashboards
- design decisions
- technical trade-offs
- project limitations
- future improvements
- portfolio presentation material

The objective is to make the repository understandable and reviewable by a technical recruiter, data engineer or hiring manager.

---

# 21. Repository Structure

The repository follows this structure:

```text
us-retail-end-to-end-medallion/
│
├── 01_infra/
│   └── postgres/
│
├── 02_data_gen/
│
├── 03_source/
│
├── 04_extraction/
│   └── export_postgres_to_parquet.py
│
├── 05_databricks/
│   ├── raw/
│   │   └── upload_parquet_to_databricks.py
│   │
│   ├── bronze/
│   │   ├── 01_create_bronze_tables.sql
│   │   └── 02_load_bronze_tables.py
│   │
│   ├── silver/
│   │   ├── 01_silver_tables_creation.sql
│   │   └── 02_silver_databricks_load.py
│   │
│   └── gold/
│       └── ...
│
├── data/
│   └── samples/
│
├── docs/
│   ├── ...
│   ├── 07_bronze_creation_and_loading.md
│   ├── 08_silver_creation_and_loading.md
│   ├── 09_gold_model_design.md
│   ├── 10_bi_requirements_and_dashboard_design.md
│   ├── 11_gold_aggregations_design.md
│   ├── 12_gold_creation_and_loading.md
│   └── 13_powerbi_implementation.md
│
├── docker-compose.yml
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

# 22. Databricks Workspace Structure

```text
workspace
│
├── bronze
│   ├── stores
│   ├── products
│   ├── customers
│   ├── invoices
│   ├── invoice_items
│   └── _bronze_file_ingestion_log
│
├── silver
│   ├── stores
│   ├── products
│   ├── customers
│   ├── invoices
│   ├── invoice_items
│   ├── quarantined_stores
│   ├── quarantined_products
│   ├── quarantined_customers
│   ├── quarantined_invoices
│   ├── quarantined_invoice_items
│   └── _silver_file_ingestion_log
│
└── gold
    ├── dim_date
    ├── dim_geography
    ├── dim_customer
    ├── dim_store
    ├── dim_category
    ├── dim_subcategory
    ├── dim_product
    ├── fact_invoice
    ├── fact_invoice_item
    ├── dim_data_quality_rule
    ├── fact_data_quality_issue
    └── analytical aggregates
```

---

# 23. Validation and Reconciliation

Each layer transition must provide measurable evidence that data has not been silently lost.

Examples include:

### PostgreSQL → Parquet

```text
Source row count
=
Extracted row count
```

### Parquet → RAW

```text
Files published
=
Files expected
```

### RAW → Bronze

```text
Source file rows
=
Bronze rows attributed to the file
```

### Bronze → Silver

For ordinary entity tables:

```text
Bronze rows
=
Silver rows
+
Quarantine rows
```

For invoices, reconciliation must account for transaction atomicity.

### Silver → Gold

Gold validation will include:

- row-count reconciliation
- referential-integrity validation
- dimension uniqueness
- fact-grain validation
- SCD2 validity
- aggregate reconciliation
- revenue reconciliation
- data-quality reconciliation

---

# 24. Architectural Principles

## Source Preservation

Source data is preserved before business transformation.

## Separation of Concerns

Each layer has a defined responsibility.

```text
RAW / Bronze
    ↓
Technical ingestion and preservation

Silver
    ↓
Business data quality and conformance

Gold
    ↓
Business-oriented analytical modeling

Power BI
    ↓
Analytics and visualization
```

## Data Quality as a First-Class Concern

Invalid records are not silently discarded.

They are:

```text
identified
classified
quarantined
measured
```

## Transaction Integrity

Invoices are treated as atomic business transactions at Silver level.

## Traceability

Silver and quarantine records remain traceable to their Bronze source rows.

## Reconciliation

Important pipeline boundaries provide measurable evidence that records and files were not silently lost.

## Reproducibility

Infrastructure and processing are implemented as code and documented execution procedures.

## Business-Oriented Analytics

Gold exposes trusted, business-oriented structures rather than raw source structures.

## Explainable Architecture

Every technology and design pattern should have a clear architectural purpose.

---

# 25. Technology Stack

## Data Generation

- Python
- Faker
- Python standard libraries

## Source System

- PostgreSQL
- Docker
- Ubuntu / WSL2

## Data Platform

- Databricks
- Apache Spark / PySpark
- Delta Lake
- Unity Catalog
- Unity Catalog Volumes
- Parquet

## Analytics

- Power BI

## Development

- Git
- GitHub
- VS Code
- DBeaver

---

# 26. Development Roadmap

The project roadmap is organized by **milestone**, while the documentation files inside a milestone describe the implementation sequence.

## Phase 1 — Infrastructure

### M1

- WSL2 / Ubuntu
- Docker
- PostgreSQL
- Database initialization

**Status: Completed**

---

## Phase 2 — Synthetic Data Generation

### M2

- Generate stores
- Generate products
- Generate customers
- Generate invoices
- Generate invoice items
- Introduce intentional data-quality issues
- Generate large-scale dataset

**Status: Completed**

---

## Phase 3 — PostgreSQL Source

### M3

- Create source tables
- Load generated data
- Preserve source-data-quality issues
- Validate source data

**Status: Completed**

---

## Phase 4 — PostgreSQL → Parquet

### M4

- Define extraction contract
- Implement extraction script
- Batch extraction
- Generate Parquet
- Reconcile PostgreSQL vs Parquet
- Extraction metadata
- Dated extraction filenames

**Status: Completed**

---

## Phase 5 — Databricks RAW / Bronze

### M5 — RAW Upload

- Upload Parquet files
- Preserve filenames
- Use Unity Catalog Volume
- Protect existing files from accidental overwrite
- Support controlled/date-scoped upload

**Status: Completed**

### M6 — Bronze

- Create Bronze Delta tables
- Preserve source business values
- Add ingestion metadata
- Preserve duplicate business keys
- Implement technical source-row identity
- Implement incremental file processing
- Execute and validate Bronze Job

**Status: Completed**

---

## Phase 6 — Silver / Data Quality

### M7

- Define required-field rules
- Define type standardization
- Define duplicate rules
- Define referential-integrity rules
- Define invoice atomicity rules
- Create Silver tables
- Create quarantine tables
- Implement PySpark quality load
- Implement lineage preservation
- Implement reconciliation output
- Implement dry-run diagnostics
- Execute Databricks Job
- Validate acceptance criteria

**Status: Completed**

Documentation:

```text
docs/08_silver_creation_and_loading.md
```

---

# Phase 7 — Gold

## M8

M8 is the complete Gold analytical-model milestone.

The implementation sequence is:

```text
09_gold_model_design.md
        ↓
10_bi_requirements_and_dashboard_design.md
        ↓
11_gold_aggregations_design.md
        ↓
12_gold_creation_and_loading.md
```

### Step 1 — Gold Model Design

Documentation:

```text
docs/09_gold_model_design.md
```

Scope:

- Define dimensional model
- Define fact grains
- Define dimensions
- Define shared/conformed dimensions
- Define surrogate keys
- Define unknown/default members
- Define SCD Type 2 dimensions
- Define data-quality model
- Define quarantine analytical structures
- Define Gold referential-integrity expectations

### Step 2 — BI Requirements and Dashboard Design

Documentation:

```text
docs/10_bi_requirements_and_dashboard_design.md
```

Scope:

- Define business questions
- Define dashboards
- Define KPIs
- Define dimensions
- Define filters
- Define drill-downs
- Define analytical grains
- Identify Power BI performance requirements
- Determine which analytical requirements can use Gold base tables directly

### Step 3 — Gold Aggregations Design

Documentation:

```text
docs/11_gold_aggregations_design.md
```

Scope:

- Identify required aggregate tables
- Define aggregate grain
- Define dimensions
- Define measures
- Define refresh strategy
- Define incremental strategy
- Define reconciliation
- Avoid unnecessary aggregates

Aggregate tables are derived from actual BI requirements.

### Step 4 — Gold Creation and Loading

Documentation:

```text
docs/12_gold_creation_and_loading.md
```

Scope:

- Create Gold schema
- Create dimensions
- Create facts
- Implement surrogate keys
- Implement SCD Type 2
- Resolve historical dimension versions
- Create analytical aggregates
- Create data-quality model
- Implement incremental loading
- Implement idempotency
- Validate Gold against Silver
- Execute Databricks Jobs

**Status: In progress**

---

# Phase 8 — Power BI

## M9

Documentation:

```text
docs/13_powerbi_implementation.md
```

Scope:

- Connect Power BI to Gold
- Build semantic model
- Create measures
- Build executive dashboard
- Build store/regional dashboard
- Build product dashboard
- Build data-quality dashboard
- Validate KPIs against Gold
- Validate filters and drill-downs
- Validate report performance

**Status: Planned**

---

# Phase 9 — Final Documentation / Portfolio Packaging

## M10

Scope:

- Final architecture documentation
- Final technical documentation
- Validation evidence
- Reconciliation evidence
- Power BI screenshots
- Design decisions
- Trade-offs
- Lessons learned
- Future improvements
- Portfolio presentation

**Status: Planned**

---

# 27. Current Milestone

```text
M1   Infrastructure                 ✅
M2   Data Generation                ✅
M3   PostgreSQL Source              ✅
M4   PostgreSQL → Parquet           ✅
M5   Databricks RAW                 ✅
M6   Bronze                         ✅
M7   Silver / DQ / Quarantine       ✅
M8   Gold                           🚧
M9   Power BI                       ⬜
M10  Final Documentation            ⬜
```

### Current development focus

```text
M8 — Gold
    │
    ├── 09_gold_model_design.md
    │
    ├── 10_bi_requirements_and_dashboard_design.md
    │
    ├── 11_gold_aggregations_design.md
    │
    └── 12_gold_creation_and_loading.md
```

After M8 is completed:

```text
M9 — Power BI
    │
    └── 13_powerbi_implementation.md
```

The five Gold/BI documentation files above are **documentation steps within M8/M9**, not additional project milestones.