# US Retail End-to-End Data Engineering Platform

An end-to-end data engineering portfolio project simulating a large US retail company, from an operational PostgreSQL source system through a Medallion architecture in Databricks and ultimately to Power BI analytical dashboards.

The project is designed to demonstrate practical data engineering skills including:

* Data generation at scale
* Relational database design
* Batch data extraction
* Parquet-based data ingestion
* Databricks / Delta Lake
* Medallion architecture
* Data quality and error management
* Data cleansing and transformation
* Dimensional modeling
* SCD Type 2
* Analytical data modeling
* Power BI
* Data lineage and reconciliation

The project deliberately introduces approximately **1% of erroneous or incomplete source data** in order to demonstrate how a modern data platform can identify, quarantine, correct, and report data-quality issues.

---

## Project Status

The project is being developed incrementally.

### Current status

| Phase | Description                        | Status       |
| ----- | ---------------------------------- | -----------  |
| M1    | Infrastructure / PostgreSQL        | ✅ Completed |
| M2    | Synthetic data generation          | ✅ Completed |
| M3    | PostgreSQL source loading          | ✅ Completed |
| M4    | PostgreSQL → Parquet extraction    | 🔜 Next      |
| M5    | Databricks RAW / Bronze            | ⬜ Planned   |
| M6    | Silver / Data Quality / Quarantine | ⬜ Planned   |
| M7    | Gold analytical model              | ⬜ Planned   |
| M8    | Power BI dashboards                | ⬜ Planned   |

The current repository intentionally stops after the data has been generated and loaded into PostgreSQL.

The downstream ingestion and Databricks implementation will be rebuilt from this point as part of the next development phase.

---

# 1. Business Scenario

The project simulates the data platform of a US retail company operating:

* **50 stores**
* **2,500 products**
* **100,000 customers**
* Approximately **55 million invoice transactions**
* Multiple invoice line items per transaction

The simulated source system contains both valid and intentionally erroneous data.

Examples of intentional data-quality problems include:

* NULL customer references
* NULL monetary values
* Invalid quantities
* Invalid discounts
* Referential integrity problems
* Other incomplete or inconsistent records

These errors are intentional and form an important part of the project.

The goal is **not** to remove the errors during ingestion.

Instead, the architecture will preserve the source data and handle the errors later in the Silver layer.

---

# 2. High-Level Architecture

The final architecture will follow a Medallion design:

```text
                    ┌─────────────────────┐
                    │  Python Data        │
                    │  Generation         │
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
                    │ Operational Source  │
                    │ / System of Record  │
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
        │ Analytics       │        │ Error Metrics   │
        └────────┬────────┘        └────────┬────────┘
                 │                          │
                 └─────────────┬────────────┘
                               ▼
                    ┌─────────────────────┐
                    │ Power BI            │
                    │ Analytical &        │
                    │ Data Quality        │
                    │ Dashboards          │
                    └─────────────────────┘
```

---

# 3. Layer Responsibilities

## PostgreSQL — Source System

PostgreSQL represents the operational retail database.

It is the simulated **System of Record**.

It contains the original business data, including the intentionally introduced data-quality problems.

The source system contains tables such as:

```text
stores
products
customers
invoices
invoice_items
```

No attempt is made to make this source system analytically perfect.

---

## Parquet — Source Extraction

The PostgreSQL database will be extracted into Parquet files.

Parquet is considered an **extraction/transport format**, not a transformation layer.

The extraction process must preserve the source data.

Therefore:

* NULL values remain NULL
* invalid values remain invalid
* referential problems remain present
* no business rules are applied
* no records are intentionally removed

The extraction process will also include technical controls such as:

* deterministic extraction
* batch processing
* keyset pagination where appropriate
* row-count reconciliation
* extraction metadata
* protection against skipped or duplicated records

---

# 4. RAW Layer

The RAW layer represents the immutable landing zone in Databricks.

Its purpose is:

> Preserve what arrived from the source system.

RAW should therefore contain the source data with minimal processing.

No business-data corrections are performed in RAW.

For example, if PostgreSQL contains:

```text
customer_id = NULL
```

RAW will also contain:

```text
customer_id = NULL
```

Similarly, invalid monetary values, quantities, discounts, and other intentional errors remain unchanged.

Technical metadata may be added, such as:

```text
_ingestion_timestamp
_source_file
_extraction_batch_id
```

---

# 5. Bronze Layer

Bronze provides a structured Delta representation of the source data.

The Bronze layer will preserve the business content of RAW while establishing a consistent technical schema.

Typical Bronze responsibilities include:

* schema enforcement
* data type standardization
* column naming conventions
* ingestion metadata
* source lineage
* technical reconciliation

Bronze will **not** correct business-data-quality problems.

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

The actual correction or rejection decision belongs to Silver.

---

# 6. Silver Layer

Silver is where data quality and business transformation will occur.

The Bronze data will be evaluated against defined data-quality rules.

Valid records will be transformed into clean, conformed Silver datasets.

Invalid records will be routed to quarantine structures.

Conceptually:

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

Example:

```text
invoice_id
line_item
error_category
error_code
error_description
source_table
source_file
ingestion_batch_id
quarantine_timestamp
```

This allows the project to demonstrate not only data cleansing, but also **data observability and data-quality management**.

---

# 7. Gold Layer

The Gold layer will provide business-oriented analytical datasets.

The main analytical model is expected to follow a dimensional/star-schema design.

A possible structure is:

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

Slowly Changing Dimensions, including SCD Type 2 where appropriate, will be implemented where the source data supports meaningful historical changes.

---

# 8. Data Quality / Error Analytics

One of the goals of the project is to make data quality measurable rather than simply hiding invalid records.

Quarantined data will eventually feed a dedicated analytical model.

This will allow Power BI to provide a **Data Quality / Error Correction dashboard**.

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
* Most frequent data-quality rules violated

Example:

```text
Total Records          55,000,000
Valid Records          54,450,000
Quarantined Records       550,000
Data Quality Rate          99.0%
```

The exact numbers will depend on the final generated dataset and validation rules.

---

# 9. Power BI

Power BI will consume the Gold layer.

The final project is expected to contain several analytical perspectives.

### Executive Dashboard

Potential KPIs:

* Revenue
* Gross Profit
* Transactions
* Average Basket Value
* Sales Growth
* Gross Margin

Potential visualizations:

* Sales trend
* Regional performance
* Store performance
* Product/category performance

### Store / Regional Dashboard

Potential analysis:

* Sales by region
* Sales by state
* Store performance
* Sales per square foot
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

# 10. Technology Stack

### Data Generation

* Python
* Faker
* Pandas / Python standard libraries

### Source Database

* PostgreSQL
* Docker
* Ubuntu / WSL

### Data Lake / Processing

* Databricks
* Apache Spark / PySpark
* Delta Lake
* Parquet

### Analytics

* Power BI

### Development / Version Control

* Git
* GitHub

---

# 11. Repository Structure

The repository is organized into stages of the data platform.

```text
us-retail-end-to-end-medallion/
│
├── 01_infra/
│   └── postgres/
│
├── 02_data_gen/
│   └── ...
│
├── 03_raw_layer/
│   └── ...
│
├── 04_databricks/
│   └── ...
│
├── data/
│   └── samples/
│
├── docs/
│   └── ...
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

The repository is being developed incrementally. Some directories represent future stages that have not yet been implemented.

---

# 12. Development Roadmap

## Phase 1 — Infrastructure

* [x] Configure WSL / Ubuntu
* [x] Configure Docker
* [x] Deploy PostgreSQL
* [x] Configure database initialization

## Phase 2 — Synthetic Data Generation

* [x] Generate stores
* [x] Generate products
* [x] Generate customers
* [x] Generate invoices
* [x] Generate invoice items
* [x] Introduce intentional data-quality issues
* [x] Generate large-scale dataset

## Phase 3 — PostgreSQL Source

* [x] Create source tables
* [x] Load generated data
* [x] Validate source data
* [x] Verify data using database tools

## Phase 4 — PostgreSQL → Parquet

* [ ] Design extraction contract
* [ ] Implement extraction scripts
* [ ] Implement batch/keyset extraction
* [ ] Generate Parquet files
* [ ] Reconcile PostgreSQL vs Parquet
* [ ] Validate row counts and keys
* [ ] Document extraction process

## Phase 5 — Databricks RAW / Bronze

* [ ] Upload Parquet files
* [ ] Create RAW layer
* [ ] Create Bronze Delta tables
* [ ] Add ingestion metadata
* [ ] Implement technical reconciliation
* [ ] Validate RAW vs Bronze

## Phase 6 — Silver / Data Quality

* [ ] Define data-quality rules
* [ ] Clean and standardize data
* [ ] Implement quarantine tables
* [ ] Classify errors
* [ ] Implement referential-integrity checks
* [ ] Implement business-rule validation
* [ ] Document data-quality framework

## Phase 7 — Gold

* [ ] Design dimensional model
* [ ] Create dimensions
* [ ] Create fact tables
* [ ] Implement SCD Type 2 where appropriate
* [ ] Create analytical datasets
* [ ] Create data-quality Gold model

## Phase 8 — Power BI

* [ ] Build semantic model
* [ ] Create DAX measures
* [ ] Build Executive dashboard
* [ ] Build Store / Regional dashboard
* [ ] Build Product dashboard
* [ ] Build Data Quality dashboard

## Phase 9 — Finalization

* [ ] Architecture diagram
* [ ] Data lineage documentation
* [ ] Data-quality documentation
* [ ] Performance documentation
* [ ] End-to-end testing
* [ ] Final GitHub cleanup
* [ ] Portfolio presentation

---

# 13. Design Principles

The project follows several principles.

### Preserve source data

The source system contains intentionally imperfect data.

The ingestion layers must preserve that data rather than silently correcting it.

### Separate ingestion from transformation

Extraction and landing should not be responsible for business-data cleansing.

### Make data quality explicit

Invalid data should be identified, classified, and quarantined rather than silently discarded.

### Maintain lineage

It should be possible to understand where a Gold record originated.

### Reconcile every major boundary

Important ingestion stages should provide evidence that data has not been lost or duplicated.

### Build for scale

The project intentionally operates on tens of millions of records to demonstrate techniques appropriate for larger datasets.

### Prefer explainable architecture

Technologies and patterns should be included because they solve a real problem in the architecture, not simply because they are fashionable.

---

# 14. Current Milestone

The current implementation intentionally stops here:

```text
Python
  ↓
CSV
  ↓
PostgreSQL
  ↓
[ CURRENT CHECKPOINT ]
```

The next milestone is:

```text
PostgreSQL
     ↓
Parquet Extraction
```

The extraction process will be designed and validated before any Databricks RAW or Bronze implementation is recreated.

---

# 15. Portfolio Objective

This project is intended as a practical demonstration of data engineering and solution architecture skills.

The objective is to demonstrate the ability to design and implement a complete data platform rather than only isolated technologies.

The final solution will cover:

```text
Source System
     ↓
Data Extraction
     ↓
Data Lake / RAW
     ↓
Bronze
     ↓
Silver + Data Quality
     ↓
Gold
     ↓
Business Intelligence
```

The completed project will be suitable as a portfolio project for demonstrating data engineering, analytics engineering, and data platform architecture experience.
