# US Retail End-to-End Data Engineering Platform

End-to-end data engineering portfolio project simulating a large US retail company, from an operational PostgreSQL source system through a Databricks Medallion architecture and ultimately to analytical and data-quality models for Power BI.

The project is intentionally built with imperfect synthetic source data so that data quality, quarantine, lineage, reconciliation and downstream correction can be demonstrated rather than hidden.

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
| M7 | Bronze → Silver / Data Quality / Quarantine | 🚧 Implementation ready |
| M8 | Gold analytical model | ⬜ Planned |
| M9 | Power BI dashboards | ⬜ Planned |
| M10 | Final documentation | ⬜ Planned |

The current implementation boundary is:

```text
PostgreSQL
    ↓
M4 — dated Parquet extraction
    ↓
M5 — Databricks RAW Volume
    ↓
M6 — Bronze Delta
    ↓
M7 — Silver Data Quality / Quarantine
    ↓
M8 — Gold
    ↓
Power BI
```

M7 is complete from an implementation/documentation perspective when the Databricks Job has been executed and its acceptance criteria validated.

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

A NULL `Customer_ID` on an invoice is intentionally treated as a legitimate **anonymous / unidentified customer transaction**, not as a Silver data-quality error. The sale remains available for downstream sales analytics. A supplied `Customer_ID` that does not exist as exactly one valid customer remains a referential-integrity error and is quarantined.

These problems are deliberately preserved through extraction, RAW and Bronze.

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
                 Data Quality / DQ
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
       M7 — Silver              Quarantine
       Clean / Typed             Invalid Data
       / Conformed              / DQ Reasons
              │                       │
              └───────────┬───────────┘
                          ▼
                    M8 — Gold
                          │
                          ▼
                      Power BI
```

---

## 4. Layer Responsibilities

### PostgreSQL

PostgreSQL is the simulated operational System of Record.

Source tables:

```text
stores
products
customers
invoices
invoice_items
```

The source schema is intentionally permissive so that downstream layers can demonstrate data-quality handling.

### Parquet / M4

Parquet is an extraction and transport format, not a transformation layer.

M4 preserves:

- NULL values
- invalid values
- duplicate business identifiers
- referential-integrity problems
- source field names
- source row content

No business correction or deduplication is performed during extraction.

### RAW / M5

RAW is the immutable Databricks landing area.

Validated Volume:

```text
/Volumes/workspace/retail/raw
```

M5 preserves the extracted Parquet filenames and does not apply business transformations.

### Bronze / M6

Bronze is a structured Delta representation of the source.

Bronze preserves business-data problems and adds technical metadata:

```text
_insert_datetime_utc
_update_datetime_utc
_source_file_name
_source_row_id
```

The technical source-row identity is not a business key. This is required because Bronze must preserve duplicate and invalid business identifiers.

### Silver / M7

Silver is the first business-data-quality layer.

Responsibilities:

- required-field validation
- date validation and typing
- numeric validation and typing
- business-key duplicate detection
- referential-integrity validation
- invoice-level transaction validation
- quarantine of invalid records
- lineage preservation
- reconciliation

Bronze is never modified by M7.

---

## 5. M6 Bronze Objects

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

M6 is executed as a Databricks **Python script task**, not as a notebook.

---

## 6. M7 Silver Objects

M7 creates the Silver schema:

```text
workspace.silver
```

### Clean tables

```text
workspace.silver._silver_file_ingestion_log
workspace.silver.customers
workspace.silver.invoice_items
workspace.silver.invoices
workspace.silver.products
workspace.silver.stores
```

### Quarantine tables

```text
workspace.silver.quarantined_customers
workspace.silver.quarantined_invoice_items
workspace.silver.quarantined_invoices
workspace.silver.quarantined_products
workspace.silver.quarantined_stores
```

Every Silver and quarantine business table retains:

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

## 7. M7 Data Quality Rules

### Customers

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

- `customer_id` must be unique;
- `join_date`, when populated, must be a valid date;
- blank strings are treated as missing values.

### Products

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

- `product_id` must be unique;
- `unit_price` and `cost` must be numeric;
- `unit_price >= 0`;
- `cost >= 0`.

### Stores

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

- `store_id` must be unique;
- `opened_date` must be valid;
- `square_footage`, when populated, must be numeric and non-negative.

### Invoices

Required:

```text
invoice_id
store_id
invoice_date
payment_method
```

Customer identification is optional at transaction time:

- `customer_id = NULL` is accepted and represents an anonymous / unidentified customer;
- a supplied, non-NULL `customer_id` must reference exactly one valid customer;
- a supplied `customer_id` that does not exist in the valid customer dimension is quarantined;
- anonymous transactions remain available for sales, product, store and payment analytics;
- customer-specific analytics must distinguish identified customers from anonymous transactions.

Additional rules:

- `invoice_id` must be unique;
- `invoice_date` must be valid;
- `store_id` must reference exactly one valid store;
- an invoice must have at least one item.

### Invoice Items

Required:

```text
invoice_id
line_item
product_id
quantity
unit_price
```

Additional rules:

- `line_item` must be a positive integer;
- `quantity` must be a positive integer;
- `product_id` must reference exactly one valid product;
- `unit_price` must be numeric and non-negative;
- `discount`, when populated, must be numeric and non-negative;
- `(invoice_id, line_item)` must be unique.

---

## 8. Invoice Atomicity

Invoice integrity is enforced at transaction level.

An invoice enters clean Silver only when:

```text
Header is valid
AND
Store reference is valid
AND
Customer_ID is NULL OR the supplied Customer_ID references one valid customer
AND
Invoice has at least one item
AND
EVERY item is valid
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

This prevents a partially valid transaction from entering the trusted Silver dataset.

---

## 9. Referential Integrity

M7 validates:

```text
invoices.store_id
        → workspace.silver.stores.store_id

invoices.customer_id
        → workspace.silver.customers.customer_id
        (only when Customer_ID is supplied; NULL is accepted as anonymous)

invoice_items.product_id
        → workspace.silver.products.product_id
```

A reference is considered valid only when the target identifier corresponds to exactly one valid entity.

A duplicated target identifier is therefore treated as ambiguous and invalid.

---

## 10. Silver Type Standardization

Silver converts Bronze source strings into business-oriented types:

```text
customers.join_date          DATE
products.unit_price          DECIMAL(18,2)
products.cost                DECIMAL(18,2)
stores.opened_date            DATE
stores.square_footage         BIGINT
invoices.invoice_date         DATE
invoice_items.line_item       BIGINT
invoice_items.quantity        BIGINT
invoice_items.unit_price      DECIMAL(18,2)
invoice_items.discount        DECIMAL(18,2)
```

Malformed values are quarantined rather than causing silent conversion or data loss.

---

## 11. M7 DQ Diagnostic Report

The PySpark loader supports `--dry-run` and prints a diagnostic report before any production write. The report deliberately separates **root causes** from **cascade quarantine** so that valid information quarantined by invoice atomicity is not mistaken for additional source errors.

The report includes:

- Bronze, Silver-valid and quarantine counts;
- quarantine rates;
- root DQ reasons for invoice headers;
- root DQ reasons for invoice items;
- bad item rows and distinct invoices affected by each item error;
- number of invoices with NULL `Customer_ID` that are accepted as anonymous transactions;
- number of invoices with a supplied but invalid `Customer_ID`;
- invoices affected by one or more bad items;
- final invoice and item quarantine counts;
- direct-error rates versus final quarantine rates;
- a 1% DQ sanity reference;
- invoice/item quarantine-path reconciliation;
- representative samples of directly bad headers, accepted anonymous invoices, bad-item invoices and directly bad item rows.

The distinction is important: an invoice with a NULL `Customer_ID` is **not** a DQ error. An invoice with a supplied customer ID that does not exist in the valid customer dimension **is** a DQ error.

The dry-run output is diagnostic only and does not write Silver or quarantine rows.

## 11. M7 Repository Structure

```text
us-retail-end-to-end-medallion/
│
├── 01_infra/
│   └── postgres/
│
├── 02_data_gen/
│   └── ...
│
├── 03_source/
│   └── ...
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
│   └── silver/
│       ├── 01_silver_tables_creation.sql
│       └── 02_silver_databricks_load.py
│
├── data/
│   └── samples/
│
├── docs/
│   ├── ...
│   ├── 07_bronze_creation_and_loading.md
│   └── 08_silver_creation_and_loading.md
│
├── docker-compose.yml
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 12. Databricks Workspace Structure

```text
Workspace
└── retail
    ├── bronze
    │   ├── 01_create_bronze_tables.sql
    │   └── 02_load_bronze_tables.py
    │
    └── silver
        ├── 01_silver_tables_creation.sql
        └── 02_silver_databricks_load.py
```

Databricks data hierarchy:

```text
workspace
├── bronze
│   ├── stores
│   ├── products
│   ├── customers
│   ├── invoices
│   ├── invoice_items
│   └── _bronze_file_ingestion_log
│
└── silver
    ├── stores
    ├── products
    ├── customers
    ├── invoices
    ├── invoice_items
    ├── quarantined_stores
    ├── quarantined_products
    ├── quarantined_customers
    ├── quarantined_invoices
    ├── quarantined_invoice_items
    └── _silver_file_ingestion_log
```

---

## 13. M7 Job

Recommended Databricks Job:

```text
M7 - Silver Data Quality Load
    │
    └── load_silver
          │
          ├── Type: Python script
          ├── Source: Workspace
          └── File:
              /Workspace/retail/silver/02_silver_databricks_load.py
```

The script must run as a **Python script task**, not as a notebook.

No parameters are required for the standard execution.

Optional controlled execution:

```text
--table customers
--table products
--table stores
--table invoices
--table invoice_items
```

Dry run:

```text
--dry-run
```

Full M7 operating instructions are documented in:

```text
docs/08_silver_creation_and_loading.md
```

---

## 14. M7 Reconciliation

M7 must preserve traceability from Bronze to one of two outcomes:

```text
Bronze record
     │
     ├── Valid → Silver
     │
     └── Invalid → Quarantine
```

For ordinary entity tables:

```text
Bronze rows = Silver rows + Quarantine rows
```

Invoice-level reconciliation also respects transaction atomicity: if an invoice is invalid, all related invoice detail records follow the invoice into quarantine.

The process prints Bronze/Silver/Quarantine counts for operational reconciliation.

---

## 15. Idempotency and Lineage

Bronze provides the physical source-row identity:

```text
_source_row_id
```

M7 uses this technical identity to MERGE valid Silver records and avoid creating duplicate Silver rows during Job retries.

Quarantine records are also protected from repeated insertion using `_source_row_id`.

Technical lineage remains available through:

```text
_source_file_name
_source_row_id
_insert_datetime_utc
_update_datetime_utc
```

---

## 16. Data Quality Analytics

Quarantine data is intentionally not deleted.

It is designed to support future Gold data-quality models and a Power BI Error Correction dashboard.

Potential future metrics include:

- total records processed;
- valid records;
- quarantined records;
- acceptance rate;
- quarantine rate;
- errors by rule;
- errors by source table;
- errors by store;
- errors by state;
- error trends over time;
- most frequent data-quality violations.

---

## 17. M7 Acceptance Criteria

M7 is considered validated when:

- `workspace.silver` exists;
- all requested Silver tables exist;
- all requested quarantine tables exist;
- all required technical metadata columns exist;
- required fields are enforced;
- dates are validated and typed;
- numeric values are validated and typed;
- duplicate business identifiers are quarantined;
- invalid references are quarantined;
- invoice header quality is enforced;
- invoice item quality is enforced;
- an invalid invoice item quarantines the entire invoice;
- an invalid invoice header quarantines the entire invoice;
- valid invoices contain all and only valid invoice items;
- clean Silver tables contain no invalid foreign-key references;
- quarantine rows retain source lineage;
- a Job rerun does not create duplicate Silver rows;
- Bronze remains unchanged.

---

## 18. Development Roadmap

### Phase 1 — Infrastructure

- [x] WSL2 / Ubuntu
- [x] Docker
- [x] PostgreSQL
- [x] Database initialization

### Phase 2 — Synthetic Data Generation

- [x] Generate stores
- [x] Generate products
- [x] Generate customers
- [x] Generate invoices
- [x] Generate invoice items
- [x] Introduce intentional data-quality issues
- [x] Generate large-scale dataset

### Phase 3 — PostgreSQL Source

- [x] Create source tables
- [x] Load generated data
- [x] Preserve source-data-quality issues
- [x] Validate source data

### Phase 4 — PostgreSQL → Parquet

- [x] Define extraction contract
- [x] Implement extraction script
- [x] Batch extraction
- [x] Generate Parquet
- [x] Reconcile PostgreSQL vs Parquet
- [x] Extraction metadata
- [x] Dated extraction filenames

### Phase 5 — Databricks RAW / Bronze

#### M5 — RAW Upload

- [x] Upload Parquet files
- [x] Preserve filenames
- [x] Use Unity Catalog Volume
- [x] Protect existing files from accidental overwrite
- [x] Support controlled/date-scoped upload

#### M6 — Bronze

- [x] Create Bronze Delta tables
- [x] Preserve source business values
- [x] Add ingestion metadata
- [x] Preserve duplicate business keys
- [x] Implement technical source-row identity
- [x] Implement incremental file processing
- [x] Document and validate Job execution

### Phase 6 — Silver / Data Quality

#### M7

- [x] Define required-field rules
- [x] Define type standardization
- [x] Define duplicate rules
- [x] Define referential-integrity rules
- [x] Define invoice atomicity rules
- [x] Create Silver tables
- [x] Create quarantine tables
- [x] Implement PySpark quality load
- [x] Implement lineage preservation
- [x] Implement reconciliation output
- [ ] Execute Databricks Job and validate acceptance criteria

### Phase 7 — Gold

- [ ] Design dimensional model
- [ ] Create dimensions
- [ ] Create fact tables
- [ ] Implement SCD Type 2 where appropriate
- [ ] Create analytical aggregates
- [ ] Create Gold data-quality model
- [ ] Validate Gold against Silver

### Phase 8 — Power BI

- [ ] Connect Power BI to Gold
- [ ] Build executive dashboard
- [ ] Build store/regional dashboard
- [ ] Build product dashboard
- [ ] Build data-quality dashboard
- [ ] Validate KPIs

---

## 19. Architectural Principles

### Source Preservation

Source data is preserved before business transformation.

### Separation of Concerns

Each layer has a defined responsibility.

### Data Quality as a First-Class Concern

Invalid records are not silently discarded. They are quarantined and measurable.

### Transaction Integrity

Invoices are treated as atomic business transactions at Silver level.

### Traceability

Every Silver/quarantine record remains traceable to its Bronze source row.

### Reconciliation

Layer transitions must be measurable and explainable.

### Reproducibility

Infrastructure and processing are implemented as code and documented execution procedures.

### Business-Oriented Analytics

Gold will expose trusted, business-oriented structures rather than raw source structures.

---

## 20. Technology Stack

### Data Generation

- Python
- Faker
- Python standard libraries

### Source System

- PostgreSQL
- Docker
- Ubuntu / WSL2

### Data Platform

- Databricks
- Apache Spark / PySpark
- Delta Lake
- Unity Catalog
- Unity Catalog Volumes
- Parquet

### Analytics

- Power BI

### Development

- Git
- GitHub
- VS Code
- DBeaver

---

## 21. Current Milestone

```text
M1  Infrastructure             ✅
M2  Data Generation            ✅
M3  PostgreSQL Source          ✅
M4  PostgreSQL → Parquet       ✅
M5  Databricks RAW             ✅
M6  Bronze                     ✅
M7  Silver / DQ / Quarantine   🚧
M8  Gold                       ⬜
M9  Power BI                   ⬜
M10 Final Documentation        ⬜
```

The immediate implementation step is to upload the M7 SQL and PySpark files, create `workspace.silver`, create the `M7 - Silver Data Quality Load` Job, execute it, and validate the acceptance criteria documented in `docs/08_silver_creation_and_loading.md`.
