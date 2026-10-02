# US Retail End-to-End Data Engineering Platform

An end-to-end data engineering portfolio project simulating a large US retail company, from an operational PostgreSQL source system through a Medallion architecture in Databricks and ultimately to Power BI analytical dashboards.

The project is designed to demonstrate practical data engineering skills including:

- Data generation at scale
- Relational database design
- Batch data extraction
- Parquet-based source transport
- Databricks / Delta Lake
- Medallion architecture
- Data quality and error management
- Data cleansing and transformation
- Dimensional modeling
- SCD Type 2
- Analytical data modeling
- Power BI
- Data lineage and reconciliation
- Incremental file ingestion

The project deliberately introduces erroneous or incomplete source data in order to demonstrate how a modern data platform can identify, quarantine, correct, and report data-quality issues.

---

## Project Status

The project is being developed incrementally.

### Current status

| Phase | Description | Status |
|---|---|---|
| M1 | Infrastructure / PostgreSQL | ✅ Completed |
| M2 | Synthetic data generation | ✅ Completed |
| M3 | PostgreSQL source loading | ✅ Completed |
| M4 | PostgreSQL → Parquet extraction | ✅ Completed |
| M5 | Parquet → Databricks RAW upload | ✅ Completed |
| M6 | Databricks RAW / Bronze ingestion | ⬜ Planned |
| M7 | Silver / Data Quality / Quarantine | ⬜ Planned |
| M8 | Gold analytical model | ⬜ Planned |
| M9 | Power BI dashboards | ⬜ Planned |
| M10 | Final documentation | ⬜ Planned |

The PostgreSQL source extraction into dated Parquet files is complete.

The current implementation boundary is now:

```text
PostgreSQL
    ↓
Dated Parquet Extraction
    ↓
Databricks Unity Catalog Volume
    ↓
RAW
```

---

# 1. Business Scenario

The project simulates the data platform of a US retail company operating:

- **50 stores**
- **2,500 products**
- **100,000 customers**
- Approximately **55 million invoice transactions**
- Multiple invoice line items per transaction

The simulated source system contains both valid and intentionally erroneous data.

Examples include:

- NULL customer references
- NULL monetary values
- Invalid quantities
- Invalid discounts
- Referential-integrity problems
- Other incomplete or inconsistent records

These errors are intentional and form an important part of the project.

The goal is **not** to remove those errors during extraction or RAW ingestion.

---

# 2. Architecture

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
                     │ Operational Source  │
                     │ / System of Record  │
                     └──────────┬──────────┘
                                │
                         M4 Extraction
                                │
                                ▼
                     ┌─────────────────────┐
                     │ Parquet Files       │
                     │ Source Extract      │
                     │ Dated / Immutable   │
                     └──────────┬──────────┘
                                │
                         M5 Upload
                                │
                                ▼
                     ┌─────────────────────┐
                     │ Databricks RAW      │
                     │ Unity Catalog       │
                     │ Volume / Landing    │
                     └──────────┬──────────┘
                                │
                         Incremental
                          ingestion
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

Source tables include:

```text
stores
products
customers
invoices
invoice_items
```

The source is intentionally imperfect.

---

## Parquet — Source Extraction

PostgreSQL is extracted into Parquet files.

Parquet is an **extraction/transport format**, not a transformation layer.

The extraction process preserves the source condition:

- NULL values remain NULL
- Invalid values remain invalid
- Referential problems remain present
- No business rules are applied
- No intentional record removal
- No deduplication

Technical controls include:

- Consistent database snapshot
- Batch processing
- Pagination/read control appropriate to the source
- Row-count reconciliation
- Extraction metadata
- Protection against skipped or duplicated records

### Extraction filename contract

Every Parquet file contains the source table and extraction date:

```text
<table>_<YYYY_MM_DD>_part-<NNNNN>.parquet
```

Example:

```text
invoice_items_2026_09_29_part-00180.parquet
```

The extraction date is part of the file identity.

A later execution therefore creates a new file identity:

```text
invoice_items_2026_09_30_part-00180.parquet
```

This is important for downstream incremental file discovery.

---

## RAW — Databricks Landing

RAW represents the immutable landing zone in Databricks.

The purpose of RAW is:

> Preserve what arrived from the source system.

The M5 uploader copies the M4 Parquet files to a Unity Catalog Volume without renaming or transforming them.

RAW should therefore contain the source data with minimal processing.

Technical metadata may be stored separately under:

```text
_metadata/
```

Business-data corrections do not belong in RAW.

---

## Bronze Layer

Bronze provides a structured Delta representation of the source data.

Typical Bronze responsibilities include:

- Schema enforcement
- Data type standardization
- Column naming conventions
- Ingestion metadata
- Source lineage
- Technical reconciliation

Bronze does not correct business-data-quality problems.

---

## Silver Layer

Silver is where data quality and business transformation will occur.

The Bronze data will be evaluated against defined data-quality rules.

Valid records will be transformed into clean, conformed Silver datasets.

Invalid records will be routed to quarantine structures.

Potential data-quality categories include:

- Missing values
- Invalid formats
- Invalid business values
- Referential-integrity errors
- Duplicate records
- Business-rule violations

---

## Gold Layer

The Gold layer will provide business-oriented analytical datasets.

The main analytical model is expected to follow a dimensional/star-schema design.

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

- Sales
- Quantity
- Discount
- Cost
- Gross Profit
- Gross Margin
- Transaction Count
- Average Basket Value

SCD Type 2 will be implemented where the source data supports meaningful historical changes.

---

# 4. M4 — PostgreSQL → Parquet

M4 is complete.

The extractor is located at:

```text
04_extraction/export_postgres_to_parquet.py
```

Default output:

```text
data/parquet_export/
```

Example:

```text
data/parquet_export/
├── customers_2026_09_29_part-00001.parquet
├── customers_2026_09_29_part-00002.parquet
├── invoice_items_2026_09_29_part-00001.parquet
├── invoices_2026_09_29_part-00001.parquet
├── products_2026_09_29_part-00001.parquet
├── stores_2026_09_29_part-00001.parquet
├── customers_2026_09_29_extraction_manifest.json
└── _extraction_run_manifest_2026_09_29.json
```

Run all source tables:

```bash
python 04_extraction/export_postgres_to_parquet.py --all
```

The extraction date is generated automatically by the extractor.

---

# 5. M5 — Parquet → Databricks RAW

M5 is the local-to-Databricks delivery boundary.

The uploader is located at:

```text
05_databricks/raw/upload_parquet_to_databricks.py
```

Default local source:

```text
data/parquet_export/
```

Default Databricks target:

```text
/Volumes/workspace/retail/raw
```

The target Volume name is configurable and must match the Volume actually created in the Databricks workspace.

The uploader:

- Preserves original Parquet filenames
- Uploads Parquet without business transformation
- Does not overwrite existing files by default
- Supports explicit `--overwrite`
- Supports date-scoped uploads
- Supports a dry-run mode
- Can upload M4 JSON manifests separately under `_metadata`

Example:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py
```

Dry run:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --dry-run
```

Specific extraction date:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --extraction-date 2026_09_29
```

Include technical manifests:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --include-manifests
```

The intended scheduled chain is:

```text
PostgreSQL
    ↓
M4 — PostgreSQL → Parquet
    ↓
data/parquet_export/
    ↓
M5 — Parquet → Databricks Volume
    ↓
RAW
    ↓
Incremental ingestion
    ↓
Bronze
```

---

# 6. Incremental File Ingestion Strategy

The project is designed so that each extraction produces uniquely identifiable files.

For example:

```text
customers_2026_09_29_part-00001.parquet
customers_2026_09_30_part-00001.parquet
customers_2026_10_01_part-00001.parquet
```

The uploader preserves these filenames.

This means the Databricks landing area accumulates distinct extraction artifacts instead of repeatedly overwriting the same filename.

The future RAW → Bronze ingestion can therefore discover new files incrementally.

The M5 uploader does not implement the downstream incremental ingestion engine itself.

That responsibility belongs to the next Databricks implementation step.

---

# 7. Data Quality / Error Analytics

One of the goals of the project is to make data quality measurable rather than simply hiding invalid records.

Quarantined data will eventually feed a dedicated analytical model.

Potential metrics include:

- Total records processed
- Valid records
- Invalid records
- Acceptance percentage
- Error percentage
- Errors by type
- Errors by source table
- Errors by store
- Errors by state
- Error trends over time
- Most frequent data-quality rules violated

---

# 8. Power BI

Power BI will consume the Gold layer.

The final project is expected to contain:

### Executive Dashboard

- Revenue
- Gross Profit
- Transactions
- Average Basket Value
- Sales Growth
- Gross Margin

### Store / Regional Dashboard

- Sales by region
- Sales by state
- Store performance
- Transaction volume
- Average basket
- Margin

### Product Dashboard

- Sales by category
- Sales by subcategory
- Brand performance
- Product profitability
- Quantity sold
- Discount impact

### Data Quality Dashboard

- Error rate
- Quarantined records
- Errors by category
- Errors by rule
- Errors by source table
- Errors by store
- Error trends

---

# 9. Technology Stack

### Data Generation

- Python
- Faker
- Pandas / Python standard libraries

### Source Database

- PostgreSQL
- Docker
- Ubuntu / WSL2

### Data Lake / Processing

- Databricks Free Edition
- Unity Catalog
- Unity Catalog Volumes
- Apache Spark / PySpark
- Delta Lake
- Parquet
- Databricks SDK for Python

### Analytics

- Power BI

### Development / Version Control

- Git
- GitHub

---

# 10. Repository Structure

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
├── 04_extraction/
│   └── export_postgres_to_parquet.py
│
├── 05_databricks/
│   └── raw/
│       └── upload_parquet_to_databricks.py
│
├── data/
│   └── parquet_export/
│
├── docs/
│   ├── ...
│   ├── 05_export_to_parquet.md
│   └── 06_upload_to_databricks.md
│
├── docker-compose.yml
├── .gitignore
├── requirements.txt
└── README.md
```

---

# 11. M5 Validation

The M5 local-to-Databricks upload path has been validated.

Confirmed Databricks Unity Catalog hierarchy:

```text
workspace
└── retail
    └── Volumes
        └── raw
```

Validated Volume path:

```text
/Volumes/workspace/retail/raw
```

Authentication is configured through the Databricks CLI/SDK profile:

```text
retail-dev
```

The authenticated user was verified with:

```bash
databricks current-user me --profile retail-dev
```

The uploader supports controlled testing with:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --file customers_2026_09_29_part-00001.parquet
```

The full upload is:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py
```

The local M4 extraction contained 500 Parquet files during validation.

The resulting architecture is now:

```text
PostgreSQL
    ↓
M4 — dated Parquet extraction
    ↓
data/parquet_export/
    ↓
M5 — Databricks SDK upload
    ↓
/Volumes/workspace/retail/raw
    ↓
M6 — incremental RAW → Bronze ingestion
```

M5 does not transform business data and does not rename the extraction files.

# 12. Development Roadmap

## Phase 1 — Infrastructure

- [x] Configure WSL / Ubuntu
- [x] Configure Docker
- [x] Deploy PostgreSQL
- [x] Configure database initialization

## Phase 2 — Synthetic Data Generation

- [x] Generate stores
- [x] Generate products
- [x] Generate customers
- [x] Generate invoices
- [x] Generate invoice items
- [x] Introduce intentional data-quality issues
- [x] Generate large-scale dataset

## Phase 3 — PostgreSQL Source

- [x] Create source tables
- [x] Load generated data
- [x] Validate source data
- [x] Verify data using database tools

## Phase 4 — PostgreSQL → Parquet

- [x] Design extraction contract
- [x] Implement extraction script
- [x] Implement batch extraction
- [x] Generate Parquet files
- [x] Reconcile PostgreSQL vs Parquet
- [x] Generate extraction metadata
- [x] Document extraction process
- [x] Implement dated extraction filenames

## Phase 5 — Databricks RAW / Bronze

### M5 — File Delivery

- [x] Define local-to-Databricks upload contract
- [x] Implement Parquet upload script
- [x] Preserve M4 filenames
- [x] Target Unity Catalog Volume
- [x] Protect existing files from accidental overwrite
- [x] Support date-scoped uploads
- [x] Support technical manifest upload

### M6 — Incremental RAW / Bronze ingestion

- [ ] Create Databricks RAW ingestion process
- [ ] Implement incremental file discovery
- [ ] Create Bronze Delta tables
- [ ] Add ingestion metadata
- [ ] Implement technical reconciliation
- [ ] Validate RAW vs Bronze

## Phase 6 — Silver / Data Quality

- [ ] Define data-quality rules
- [ ] Clean and standardize data
- [ ] Implement quarantine tables
- [ ] Classify errors
- [ ] Implement referential-integrity checks
- [ ] Implement business-rule validation
- [ ] Document data-quality framework

## Phase 7 — Gold

- [ ] Design dimensional model
- [ ] Create dimensions
- [ ] Create fact tables
- [ ] Implement SCD Type 2 where appropriate
- [ ] Create analytical datasets
- [ ] Create data-quality Gold model

## Phase 8 — Power BI

- [ ] Build semantic model
- [ ] Create DAX measures
- [ ] Build Executive dashboard
- [ ] Build Store / Regional dashboard
- [ ] Build Product dashboard
- [ ] Build Data Quality dashboard

## Phase 9 — Finalization

- [ ] Architecture diagram
- [ ] Data lineage documentation
- [ ] Data-quality documentation
- [ ] Performance documentation
- [ ] End-to-end testing
- [ ] Final GitHub cleanup
- [ ] Portfolio presentation

---

# 13. Design Principles

### Preserve source data

The source system contains intentionally imperfect data.

The extraction and RAW layers preserve that data rather than silently correcting it.

### Separate ingestion from transformation

Extraction and landing are not responsible for business-data cleansing.

### Make data quality explicit

Invalid data should be identified, classified, and quarantined rather than silently discarded.

### Maintain lineage

It should be possible to understand where a Gold record originated.

### Reconcile every major boundary

Important ingestion stages should provide evidence that data has not been lost or duplicated.

### Build for scale

The project intentionally operates on tens of millions of records to demonstrate techniques appropriate for larger datasets.

### Prefer explainable architecture

Technologies and patterns should be included because they solve a real problem in the architecture.

### Treat extraction files as immutable artifacts

A new extraction should produce a new file identity rather than silently replacing a previously processed file.

---

# 14. Current Milestone

M4 — PostgreSQL → Parquet extraction is complete.

The current implementation is:

```text
Python
  ↓
CSV
  ↓
PostgreSQL
  ↓
Dated Parquet Extraction
  ↓
M5 — Databricks Upload
  ↓
RAW Volume
```

The Parquet filename contract is:

```text
<table>_<YYYY_MM_DD>_part-<NNNNN>.parquet
```

Example:

```text
invoice_items_2026_09_29_part-00180.parquet
```

M5 preserves this identity when uploading the files to Databricks.

The next implementation boundary is:

```text
Databricks RAW
     ↓
Incremental ingestion
     ↓
Bronze Delta
```

---

# 15. Portfolio Objective

This project is intended as a practical demonstration of data engineering and solution architecture skills.

The objective is to demonstrate the ability to design and implement a complete data platform rather than isolated technologies.

The final solution will cover:

```text
Source System
     ↓
Data Extraction
     ↓
Databricks RAW
     ↓
Bronze
     ↓
Silver + Data Quality
     ↓
Gold
     ↓
Business Intelligence
```

The completed project will be suitable as a portfolio project demonstrating data engineering, analytics engineering, and data platform architecture experience.
