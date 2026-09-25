# US Retail End-to-End Data Engineering Platform

An end-to-end data engineering portfolio project simulating a large US retail company, from an operational PostgreSQL source system through a Medallion architecture in Databricks and ultimately to Power BI analytical and data-quality dashboards.

The project is designed to demonstrate practical data engineering and solution architecture skills, including:

* Synthetic data generation at scale
* Relational database design
* Batch data extraction
* Parquet-based data ingestion
* Databricks / Apache Spark / Delta Lake
* Medallion architecture
* Data quality and error management
* Data cleansing and transformation
* Data quarantine
* Dimensional modeling
* Slowly Changing Dimensions (SCD Type 2)
* Analytical data modeling
* Power BI
* Data lineage and reconciliation

The project deliberately introduces approximately **1% of erroneous or incomplete source data** to demonstrate how a modern data platform can preserve, identify, quarantine, correct, and report data-quality issues.

---

# 1. Project Status

The project is being developed incrementally.

| Phase | Description                        | Status      |
| ----- | ---------------------------------- | ----------- |
| M1    | Infrastructure / PostgreSQL        | ✅ Completed |
| M2    | Synthetic data generation          | ✅ Completed |
| M3    | PostgreSQL source loading          | ✅ Completed |
| M4    | PostgreSQL → Parquet extraction    | 🔜 Next     |
| M5    | Databricks RAW / Bronze            | ⬜ Planned   |
| M6    | Silver / Data Quality / Quarantine | ⬜ Planned   |
| M7    | Gold analytical model              | ⬜ Planned   |
| M8    | Power BI dashboards                | ⬜ Planned   |

The current repository intentionally stops after the synthetic data has been generated and loaded into PostgreSQL.

The downstream extraction, Databricks, data-quality, analytical, and BI layers will be rebuilt from this point.

---

# 2. Business Scenario

The project simulates the data platform of a US retail company operating:

* **50 stores**
* **2,500 products**
* **100,000 customers**
* Approximately **55 million invoices**
* Approximately **200 million invoice line items**

The source data is synthetic and intentionally contains both valid and erroneous records.

Examples of intentional data-quality problems include:

* NULL customer references
* NULL monetary values
* Invalid quantities
* Invalid discounts
* Invalid product references
* Referential integrity problems
* Duplicate business identifiers
* Other incomplete or inconsistent records

These errors are intentional and form an important part of the project.

The objective is **not** to remove or correct these problems during source ingestion.

Instead, the architecture preserves the source data and handles business-data-quality problems downstream, primarily in the Silver layer.

---

# 3. High-Level Architecture

The final architecture will follow a Medallion design:

```text
                    ┌─────────────────────┐
                    │ Python Data         │
                    │ Generation          │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ CSV Source Data     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ PostgreSQL          │
                    │ Operational Source   │
                    │ System of Record    │
                    └──────────┬──────────┘
                               │
                         Extraction
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Parquet Files       │
                    │ Source Extract      │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Databricks RAW      │
                    │ Immutable Landing   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Databricks BRONZE   │
                    │ Structured Source   │
                    └──────────┬──────────┘
                               │
                       Data Quality
                       & Transformation
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
        ┌─────────────────┐        ┌─────────────────┐
        │ SILVER          │        │ QUARANTINE      │
        │ Clean / Valid   │        │ Invalid Records │
        │ Conformed Data  │        │ & DQ Details    │
        └────────┬────────┘        └────────┬────────┘
                 │                          │
                 ▼                          ▼
        ┌─────────────────┐        ┌─────────────────┐
        │ GOLD            │        │ GOLD DQ         │
        │ Analytics       │        │ Error Analytics │
        └────────┬────────┘        └────────┬────────┘
                 │                          │
                 └─────────────┬────────────┘
                               ▼
                    ┌─────────────────────┐
                    │ Power BI            │
                    │ Business & Data     │
                    │ Quality Dashboards  │
                    └─────────────────────┘
```

---

# 4. Layer Responsibilities

## PostgreSQL — Operational Source System

PostgreSQL represents the simulated operational source system.

It contains the generated source data, including intentionally introduced data-quality problems.

Typical source tables are:

```text
stores
products
customers
invoices
invoice_items
```

The PostgreSQL schema is intentionally permissive.

Business validation is not performed at this stage because the downstream pipeline must be able to detect and handle source-system errors.

---

## Parquet — Source Extraction

PostgreSQL will be extracted into Parquet files.

Parquet is an **extraction and transport format**, not a transformation layer.

The extraction process must preserve the source data:

* NULL values remain NULL
* Invalid values remain invalid
* Referential problems remain present
* No business rules are applied
* No records are intentionally removed

The extraction process will also implement technical controls such as:

* Batch processing
* Keyset pagination where appropriate
* Row-count reconciliation
* Source-to-file reconciliation
* Extraction metadata
* Protection against skipped records
* Protection against duplicated records

---

# 5. RAW Layer

The RAW layer represents the immutable landing zone in Databricks.

Its purpose is:

> **Preserve what arrived from the source system.**

RAW should therefore contain the extracted source data with minimal processing.

No business-data corrections are performed in RAW.

For example:

```text
PostgreSQL
customer_id = NULL
       ↓
Parquet
customer_id = NULL
       ↓
RAW
customer_id = NULL
```

Technical metadata may be added, such as:

```text
_ingestion_timestamp
_source_file
_extraction_batch_id
```

---

# 6. Bronze Layer

Bronze provides a structured Delta representation of the source data.

The Bronze layer preserves the business content of RAW while establishing a consistent technical schema.

Typical Bronze responsibilities include:

* Schema enforcement
* Data type standardization
* Column naming conventions
* Ingestion metadata
* Source lineage
* Technical reconciliation

Bronze does **not** correct business-data-quality problems.

For example:

```text
PostgreSQL
quantity = -3
       ↓
Parquet
quantity = -3
       ↓
RAW
quantity = -3
       ↓
BRONZE
quantity = -3
```

The correction or rejection decision belongs to Silver.

---

# 7. Silver Layer

Silver is where business-data-quality validation and transformation occur.

Bronze data will be evaluated against defined data-quality rules.

Valid records will be transformed into clean, conformed Silver datasets.

Invalid records will be routed to quarantine structures.

```text
                    BRONZE
                       │
              Data Quality Rules
                       │
             ┌─────────┴─────────┐
             │                   │
             ▼                   ▼
          VALID               INVALID
             │                   │
             ▼                   ▼
          SILVER             QUARANTINE
```

Potential data-quality categories include:

* Missing values
* Invalid formats
* Invalid business values
* Referential integrity errors
* Duplicate records
* Business-rule violations

Each quarantined record should retain enough information to understand why it was rejected.

Example quarantine attributes:

```text
invoice_id
line_item
error_category
error_code
error_description
source_table
source_file
extraction_batch_id
quarantine_timestamp
```

This allows the project to demonstrate both data cleansing and data-quality management.

---

# 8. Gold Layer

The Gold layer will provide business-oriented analytical datasets.

The main analytical model will follow a dimensional/star-schema approach.

Expected structure:

```text
                  dim_date
                     │
                     │
dim_store ───── fact_sales ───── dim_product
                     │
                     │
                dim_customer
```

Potential Gold tables include:

```text
dim_date
dim_store
dim_product
dim_customer
fact_sales
```

The main fact table will use **invoice line item** as its grain.

Potential measures include:

* Sales
* Quantity
* Discount
* Cost
* Gross Profit
* Gross Margin
* Transaction Count
* Average Basket Value

SCD Type 2 will be implemented where the source data and business model support meaningful historical changes.

---

# 9. Data Quality / Error Analytics

A key objective of the project is to make data quality measurable instead of simply hiding invalid records.

Quarantined data will eventually feed a dedicated analytical model.

This model will support a **Data Quality / Error Correction dashboard** in Power BI.

Potential metrics include:

* Total records processed
* Valid records
* Invalid records
* Acceptance percentage
* Error percentage
* Errors by type
* Errors by source table
* Errors by store
* Errors by state
* Error trends over time
* Most frequent violated data-quality rules

The final numbers will depend on the generated dataset and the validation rules implemented in Silver.

---

# 10. Power BI

Power BI will consume the Gold analytical models and the Gold data-quality model.

Expected dashboards include:

### Executive Dashboard

Potential KPIs:

* Revenue
* Gross Profit
* Transactions
* Average Basket Value
* Sales Growth
* Gross Margin

### Store / Regional Dashboard

Potential analysis:

* Sales by region
* Sales by state
* Store performance
* Transaction volume
* Average basket
* Margin

### Product Dashboard

Potential analysis:

* Sales by category
* Sales by subcategory
* Brand performance
* Product profitability
* Quantity sold
* Discount impact

### Data Quality Dashboard

Potential analysis:

* Error rate
* Quarantined records
* Errors by category
* Errors by rule
* Errors by source table
* Errors by store
* Error trends

---

# 11. Technology Stack

## Data Generation

* Python
* Faker
* Python standard libraries

## Source Database

* PostgreSQL
* Docker
* Ubuntu / WSL2

## Data Lake / Processing

* Databricks
* Apache Spark / PySpark
* Delta Lake
* Parquet

## Analytics

* Power BI

## Development / Version Control

* Git
* GitHub
* VS Code
* DBeaver

---

# 12. Repository Structure

The repository is organized according to the development stages of the platform.

```text
us-retail-end-to-end-medallion/
│
├── 01_infra/
│   └── postgres/
│
├── 02_data_gen/
│   └── generate_retail_data.py
│
├── 03_source/
│   └── ...
│
├── data/
│   └── samples/
│
├── docs/
│   └── ...
│
├── docker-compose.yml
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

The repository is intentionally being developed incrementally.

Future stages such as Parquet extraction, Databricks processing, Gold modeling, and Power BI will be added as the project progresses.

---

# 13. Development Roadmap

## Phase 1 — Infrastructure

* [x] Configure WSL2 / Ubuntu
* [x] Configure Docker
* [x] Deploy PostgreSQL
* [x] Configure database initialization
* [x] Document infrastructure decisions

## Phase 2 — Synthetic Data Generation

* [x] Generate stores
* [x] Generate products
* [x] Generate customers
* [x] Generate invoices
* [x] Generate invoice items
* [x] Introduce intentional data-quality issues
* [x] Generate large-scale dataset
* [x] Commit representative samples

## Phase 3 — PostgreSQL Source

* [x] Create permissive source tables
* [x] Load generated data
* [x] Preserve source data-quality issues
* [x] Validate source data
* [x] Verify data using database tools

## Phase 4 — PostgreSQL → Parquet

* [ ] Define extraction contract
* [ ] Implement extraction scripts
* [ ] Implement batch extraction
* [ ] Implement correct keyset pagination where required
* [ ] Generate Parquet files
* [ ] Reconcile PostgreSQL vs Parquet
* [ ] Validate row counts
* [ ] Validate business keys
* [ ] Document extraction process

## Phase 5 — Databricks RAW / Bronze

* [ ] Define storage structure
* [ ] Upload Parquet files
* [ ] Create RAW layer
* [ ] Create Bronze Delta tables
* [ ] Add ingestion metadata
* [ ] Add source lineage
* [ ] Implement technical reconciliation
* [ ] Validate RAW vs Bronze

## Phase 6 — Silver / Data Quality

* [ ] Define data-quality rules
* [ ] Implement type standardization
* [ ] Clean valid records
* [ ] Implement quarantine tables
* [ ] Classify errors
* [ ] Implement referential-integrity checks
* [ ] Implement duplicate detection
* [ ] Document correction rules
* [ ] Reconcile Silver vs Bronze

## Phase 7 — Gold

* [ ] Design dimensional model
* [ ] Create dimensions
* [ ] Create fact tables
* [ ] Implement SCD Type 2 where appropriate
* [ ] Create analytical aggregates
* [ ] Create Gold data-quality model
* [ ] Validate Gold against Silver

## Phase 8 — Power BI

* [ ] Connect Power BI to Gold
* [ ] Build executive dashboard
* [ ] Build regional/store dashboard
* [ ] Build product dashboard
* [ ] Build data-quality dashboard
* [ ] Validate KPIs
* [ ] Document semantic model

---

# 14. Architectural Principles

The project follows several explicit architectural principles.

### Source preservation

Source data is preserved before business transformation.

### Separation of concerns

Each layer has a clearly defined responsibility.

### Data quality as a first-class concern

Invalid records are not silently discarded.

### Traceability

Records should be traceable from source through the analytical platform.

### Reconciliation

Important transitions between layers should be measurable and reconcilable.

### Reproducibility

Infrastructure and processing should be reproducible through code and configuration.

### Business-oriented analytics

Gold models should expose data in a form that supports real business analysis rather than simply exposing technical source structures.

---

# 15. Current Milestone

The project is currently at:

```text
M1  Infrastructure             ✅
M2  Data Generation            ✅
M3  PostgreSQL Source          ✅

M4  PostgreSQL → Parquet       ← NEXT
M5  Databricks RAW / Bronze
M6  Silver / DQ / Quarantine
M7  Gold
M8  Power BI
```

The next implementation step is **M4 — PostgreSQL → Parquet extraction**.

No downstream business transformations should be introduced before the extraction contract and source-preservation rules are established.
