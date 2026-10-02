# US Retail End-to-End Data Engineering Platform

An end-to-end data engineering portfolio project simulating a large US retail company, from an operational PostgreSQL source system through a Medallion architecture in Databricks and ultimately to analytical dashboards.

The project demonstrates:

- synthetic data generation at scale
- PostgreSQL source-system design
- batch extraction
- Parquet source extraction
- Databricks Unity Catalog and Volumes
- RAW and Bronze ingestion
- Delta Lake
- incremental upsert processing
- data-quality management
- Silver transformations and quarantine
- dimensional modeling
- SCD Type 2
- Gold analytical modeling
- Power BI
- data lineage and reconciliation

---

## 1. Project Status

| Milestone | Description | Status |
|---|---|---|
| M1 | Infrastructure / PostgreSQL | ✅ Completed |
| M2 | Synthetic data generation | ✅ Completed |
| M3 | PostgreSQL source loading | ✅ Completed |
| M4 | PostgreSQL → Parquet extraction | ✅ Completed |
| M5 | Parquet → Databricks RAW | ✅ Completed |
| M6 | Databricks Bronze creation + incremental loading | 🚧 In progress |
| M7 | Silver / Data Quality / Quarantine | ⬜ Planned |
| M8 | Gold analytical model | ⬜ Planned |
| M9 | Power BI dashboards | ⬜ Planned |
| M10 | Final documentation / portfolio packaging | ⬜ Planned |

Current implementation checkpoint:

```text
Python
  ↓
CSV
  ↓
PostgreSQL / retail_raw
  ↓
M4 — Parquet extraction
  ↓
M5 — Databricks RAW Volume
  ↓
M6 — Bronze Delta + incremental MERGE
  ↓
M7 — Silver / Data Quality
  ↓
Gold
  ↓
Power BI
```

---

## 2. Business Scenario

The project simulates a US retail company operating:

- 50 stores
- 2,500 products
- 100,000 customers
- a large transaction dataset
- invoice headers and invoice line items

The source system intentionally contains data-quality problems.

Examples include:

- NULL customer references
- NULL monetary values
- invalid quantities
- invalid discounts
- referential-integrity problems
- incomplete or inconsistent records

These errors are intentional.

The project does **not** remove them during ingestion.

Instead:

```text
Source → RAW → Bronze
                   │
                   ▼
             Silver / DQ
                   │
          ┌────────┴────────┐
          ▼                 ▼
        Valid            Invalid
          │                 │
          ▼                 ▼
       Silver          Quarantine
```

---

## 3. Architecture

```text
                    ┌───────────────────┐
                    │ Python Generator  │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ CSV Source Data   │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ PostgreSQL        │
                    │ System of Record  │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ M4 — Parquet      │
                    │ Extraction        │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ M5 — Databricks   │
                    │ RAW Volume        │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ M6 — Bronze       │
                    │ Delta / MERGE     │
                    └─────────┬─────────┘
                              │
                       Data Quality
                       & Transformation
                              │
                   ┌──────────┴──────────┐
                   ▼                     ▼
            ┌──────────────┐      ┌──────────────┐
            │ SILVER       │      │ QUARANTINE   │
            │ Clean/Valid  │      │ Invalid/DQ   │
            └──────┬───────┘      └──────┬───────┘
                   │                     │
                   └──────────┬──────────┘
                              ▼
                    ┌───────────────────┐
                    │ GOLD              │
                    │ Analytics         │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Power BI          │
                    └───────────────────┘
```

---

## 4. Layer Responsibilities

### PostgreSQL

PostgreSQL is the simulated operational source / System of Record.

The source schema is:

```text
retail_raw.stores
retail_raw.products
retail_raw.customers
retail_raw.invoices
retail_raw.invoice_items
```

The current source loader defines all business fields as `TEXT`, deliberately allowing dirty values to reach downstream layers.

### Parquet

Parquet is an extraction and transport format.

M4 preserves:

- source column names
- NULL values
- invalid values
- referential problems
- records
- source semantics

M4 also provides extraction controls including batch processing, row-count reconciliation, manifests, and extraction-date-based filenames.

### RAW

RAW is the immutable Databricks landing area.

Current validated Volume:

```text
/Volumes/workspace/retail/raw
```

M5 uploads Parquet files without business transformations and preserves the original filenames.

### Bronze

Bronze is a structured Delta representation of the source data.

Bronze:

- preserves business column names
- preserves source values
- adds technical metadata
- tracks source-file lineage
- processes new files incrementally
- performs record-level upserts with Delta `MERGE`

Bronze does **not** perform business cleansing.

### Silver

Silver will perform:

- type casting
- data-quality validation
- null handling
- referential-integrity checks
- business-rule validation
- deduplication
- quarantine
- conformance

### Gold

Gold will provide analytical models such as:

```text
dim_date
dim_store
dim_product
dim_customer
fact_sales
```

and data-quality analytical structures.

---

## 5. PostgreSQL Source Tables

Current source structures:

### stores

```text
store_id
store_name
city
state
zip_code
region
manager_name
opened_date
square_footage
```

### products

```text
product_id
sku
product_name
category
subcategory
brand
unit_price
cost
is_active
```

### customers

```text
customer_id
first_name
last_name
email
phone
street_address
city
state
zip_code
loyalty_tier
join_date
```

### invoices

```text
invoice_id
store_id
customer_id
invoice_date
payment_method
```

### invoice_items

```text
invoice_id
line_item
product_id
quantity
unit_price
discount
```

---

## 6. M4 — PostgreSQL → Parquet

M4 extracts PostgreSQL tables to:

```text
data/parquet_export/
```

Example:

```text
customers_2026_09_29_part-00001.parquet
customers_2026_09_29_part-00002.parquet
invoices_2026_09_29_part-00001.parquet
invoice_items_2026_09_29_part-00001.parquet
```

The source extractor does not apply business transformations.

Technical controls include:

- repeatable-read snapshot
- server-side cursor
- batch extraction
- multiple Parquet parts
- row-count reconciliation
- atomic file publication
- extraction manifests
- extraction-date-based filenames

---

## 7. M5 — Databricks RAW

M5 uploads the M4 Parquet files to:

```text
/Volumes/workspace/retail/raw
```

The RAW Volume is under:

```text
workspace
└── retail
    └── raw
```

Original filenames are preserved.

Example:

```text
customers_2026_09_29_part-00001.parquet
```

The RAW area is treated as an immutable landing zone.

M5 does not:

- clean data
- change data types
- deduplicate
- merge files
- apply business rules
- overwrite existing files by default

Technical JSON extraction manifests are kept separately under `_metadata` when uploaded.

---

## 8. M6 — Databricks Bronze

M6 is the current implementation stage.

The Bronze schema is:

```text
workspace.bronze
```

The business tables are:

```text
workspace.bronze.stores
workspace.bronze.products
workspace.bronze.customers
workspace.bronze.invoices
workspace.bronze.invoice_items
```

A technical control table is also created:

```text
workspace.bronze._bronze_file_ingestion_log
```

### Bronze metadata

Every business Bronze table adds:

```text
_insert_datetime_utc
_update_datetime_utc
_source_file_name
```

The metadata is technical only.

`_insert_datetime_utc` records the first Bronze insertion.

`_update_datetime_utc` records the latest successful Bronze upsert.

`_source_file_name` identifies the Parquet file that supplied the current row version.

---

## 9. M6 Incremental Upsert

The loader is:

```text
05_databricks/bronze/02_load_bronze_tables.py
```

It is designed to run as a Databricks Job.

The processing pattern is:

```text
RAW Volume
    │
    ▼
discover Parquet files
    │
    ▼
check ingestion log
    │
    ├── already SUCCESS → skip
    │
    └── new file
          │
          ▼
       read Parquet
          │
          ▼
    add technical metadata
          │
          ▼
       Delta MERGE
          │
          ├── _source_row_id exists → UPDATE technical metadata
          │
          └── _source_row_id absent → INSERT complete source row
          │
          ▼
       mark SUCCESS
```

The MERGE identity is:

```text
_source_row_id
```

It is deliberately **not** a business key.

This distinction is essential because the source may contain duplicate or invalid business keys. Bronze must preserve every physical source row.

The Bronze metadata is:

| Column | Purpose |
|---|---|
| `_insert_datetime_utc` | First Bronze insertion timestamp for the physical source row |
| `_update_datetime_utc` | Latest technical upsert timestamp |
| `_source_file_name` | Exact Parquet filename |
| `_source_row_id` | Technical identity of the physical source row; not a business key |

No delete operation is performed.

A later extraction does not delete a Bronze row simply because the row is absent from that extraction.

---

## 10. M6 Source-Preservation Rule

Example:

```text
PostgreSQL:
quantity = -3

        ↓

Parquet:
quantity = -3

        ↓

RAW:
quantity = -3

        ↓

Bronze:
quantity = -3

        ↓

Silver:
validate / correct / quarantine
```

Bronze must not silently turn:

```text
-3 → 3
NULL → 0
invalid → NULL
```

Those are Silver responsibilities.

---

## 11. M6 Duplicate-Key Handling

Duplicate business keys are **not an error for Bronze ingestion**.

Examples that must all be loaded:

```text
customer_id = C001
customer_id = C001
```

or:

```text
invoice_id = INV001
line_item  = 1

invoice_id = INV001
line_item  = 1
```

Bronze does not select a winner, deduplicate, or reject these rows.

Each physical source row receives its own `_source_row_id`, allowing Delta MERGE to operate without using the business key as the merge condition.

The intended flow is:

```text
source duplicate / invalid value
          ↓
       Parquet
          ↓
         RAW
          ↓
        Bronze
          ↓
Silver data-quality / correction / quarantine
```

This means **errors are preserved and cached in Bronze rather than lost before Silver**.

Only technical failures may stop ingestion, such as an unreadable Parquet file, incompatible schema, or failed Delta transaction. A bad business value is never a reason to discard its row.

---

## 12. Repository Structure

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
│   └── bronze/
│       ├── 01_create_bronze_tables.sql
│       └── 02_load_bronze_tables.py
│
├── data/
│   └── samples/
│
├── docs/
│   ├── ...
│   ├── 06_upload_to_databricks.md
│   └── 07_bronze_creation_and_loading.md
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 13. M6 Job

The recommended Databricks Job is:

```text
Job:
retail-m6-bronze-incremental-load

Task:
load-bronze-tables

Type:
Python script

Script:
05_databricks/bronze/02_load_bronze_tables.py
```

The Job reads:

```text
/Volumes/workspace/retail/raw
```

and writes:

```text
workspace.bronze
```

The Job is intended to be executed once for the current project milestone, but the implementation is deliberately safe for repeated executions.

A repeated execution:

- skips already-successful files
- processes newly arrived files
- updates existing records with MERGE
- inserts new records
- does not delete Bronze records
- does not modify RAW files

Detailed Job instructions are in:

```text
docs/07_bronze_creation_and_loading.md
```

---

## 14. Validation

After M6, verify:

```sql
SHOW TABLES IN workspace.bronze;
```

Then:

```sql
SELECT COUNT(*) FROM workspace.bronze.stores;
SELECT COUNT(*) FROM workspace.bronze.products;
SELECT COUNT(*) FROM workspace.bronze.customers;
SELECT COUNT(*) FROM workspace.bronze.invoices;
SELECT COUNT(*) FROM workspace.bronze.invoice_items;
```

Check the file ledger:

```sql
SELECT
    source_table,
    status,
    COUNT(*) AS files
FROM workspace.bronze._bronze_file_ingestion_log
GROUP BY source_table, status
ORDER BY source_table, status;
```

Check lineage:

```sql
SELECT
    _source_file_name,
    COUNT(*) AS rows
FROM workspace.bronze.customers
GROUP BY _source_file_name
ORDER BY _source_file_name;
```

---

## 15. Technology Stack

### Data Generation

- Python
- Faker
- Pandas / standard library

### Source Database

- PostgreSQL
- Docker
- Ubuntu / WSL2

### Data Lake / Processing

- Databricks
- Apache Spark / PySpark
- Delta Lake
- Parquet
- Unity Catalog Volumes

### Analytics

- Power BI

### Development

- Git
- GitHub
- VS Code
- DBeaver

---

## 16. Design Principles

### Preserve source data

Ingestion layers do not silently correct source data.

### Separate ingestion from transformation

Bronze is responsible for technical ingestion and lineage. Silver is responsible for business-data-quality processing.

### Make data quality explicit

Invalid data should be identified and classified rather than hidden.

### Maintain lineage

A Bronze row should be traceable to its source Parquet file.

### Reconcile boundaries

Important pipeline boundaries should provide technical evidence that records and files were not silently lost.

### Build for scale

The project deliberately uses a large synthetic retail dataset to demonstrate techniques applicable to larger data platforms.

### Prefer explainable architecture

Each technology and design pattern should have a clear architectural purpose.

---

## 17. Roadmap

### Completed

- [x] WSL2 / Ubuntu infrastructure
- [x] Docker / PostgreSQL
- [x] Synthetic data generation
- [x] Source data-quality error injection
- [x] PostgreSQL loading
- [x] PostgreSQL → Parquet extraction
- [x] Parquet reconciliation and manifests
- [x] Databricks RAW Volume
- [x] Parquet upload to RAW
- [x] M6 Bronze DDL
- [x] M6 incremental Bronze loader design
- [x] M6 Databricks Job design
- [x] M6 duplicate/error preservation design

### Next

- [ ] Execute and validate M6 Job
- [ ] Validate duplicate and invalid source rows are retained in Bronze
- [ ] Validate Bronze row counts against M4 manifests
- [ ] Validate incremental re-execution
- [ ] Implement Silver data-quality rules
- [ ] Implement quarantine structures
- [ ] Implement referential-integrity validation
- [ ] Implement business-rule validation
- [ ] Build Gold dimensional model
- [ ] Build Power BI dashboards
- [ ] Final architecture and portfolio documentation

---

## 18. Portfolio Objective

The project demonstrates the design and implementation of a complete data platform:

```text
Source System
     ↓
Data Extraction
     ↓
Parquet
     ↓
Databricks RAW
     ↓
Bronze / Incremental MERGE
     ↓
Silver + Data Quality
     ↓
Quarantine
     ↓
Gold
     ↓
Power BI
```

The completed solution is intended to demonstrate practical data-engineering capabilities across ingestion, data quality, transformation, analytical modeling, orchestration, lineage, and BI.
