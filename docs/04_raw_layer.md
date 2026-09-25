# M3 - Load Source Data into PostgreSQL

**Status:** Done
**Environment:** DEV VM (Windows) → Host (Windows) → WSL2 Ubuntu → Docker PostgreSQL

---

## 1. Objective

Load the synthetic CSV data generated in **M2** into PostgreSQL, which represents the **simulated operational source system** for the retail platform.

The purpose of this milestone is to establish a stable source system that will later be extracted into Parquet and ingested into the Databricks Medallion architecture.

### Important design principle

The PostgreSQL source contains intentionally introduced data-quality issues.

These issues are **not corrected during M3**.

The source data must remain representative of an imperfect operational system so that later Medallion layers can demonstrate:

* Data-quality detection
* Data cleansing
* Data validation
* Error classification
* Quarantine
* Data lineage
* Error-correction reporting

The pipeline therefore follows this principle:

```text
Synthetic Data
      ↓
PostgreSQL
(Source / Operational System)
      ↓
Parquet Extraction
      ↓
Databricks RAW
      ↓
Bronze
      ↓
Silver + Quarantine
      ↓
Gold + Data Quality Model
      ↓
Power BI
```

PostgreSQL is therefore **not considered the Databricks RAW layer**.

---

# 2. Infrastructure

PostgreSQL runs inside Docker on WSL2 Ubuntu.

The development environment is a Windows VM that connects to PostgreSQL through the Windows host.

```text
[DEV VM]
    |
    | PostgreSQL :5432
    v
[Windows Host]
    |
    | portproxy
    v
[WSL2 Ubuntu]
    |
    v
[Docker Container]
retail_postgres :5432
```

The WSL2 IP address may change after a reboot, so the Windows port proxy must be refreshed when required.

---

## 2.1 Host setup

Run the following commands from an **Administrator PowerShell** on the Windows host:

```powershell
$wslIp = (wsl hostname -I).Split()[0]

Write-Host "WSL IP: $wslIp"

netsh interface portproxy delete v4tov4 `
    listenaddress=0.0.0.0 `
    listenport=5432

netsh interface portproxy add v4tov4 `
    listenaddress=0.0.0.0 `
    listenport=5432 `
    connectaddress=$wslIp `
    connectport=5432

netsh advfirewall firewall add rule `
    name="Postgres 5432" `
    dir=in `
    action=allow `
    protocol=TCP `
    localport=5432

netsh interface portproxy show v4tov4
```

This configuration may need to be repeated after a WSL2 restart because the WSL2 IP is not guaranteed to remain constant.

---

# 3. PostgreSQL Verification

From WSL2:

```bash
docker ps | grep postgres
```

Expected result:

```text
0.0.0.0:5432->5432/tcp
retail_postgres
```

Verify the PostgreSQL database:

```bash
psql -U retail_admin -d retail_db -h localhost
```

The source schema contains the following tables:

```text
retail_raw.stores
retail_raw.products
retail_raw.customers
retail_raw.invoices
retail_raw.invoice_items
```

The `retail_raw` schema name is retained from the current PostgreSQL implementation.

In the overall architecture, however, PostgreSQL represents the **source system**, while the Databricks RAW layer will be introduced in a later milestone.

---

# 4. Source Files

The synthetic data generated in M2 is stored under:

```text
data/raw/
```

The main source files are:

| File                  | Description                                     |
| --------------------- | ----------------------------------------------- |
| `stores.csv`          | Store master data                               |
| `products.csv`        | Product master data                             |
| `customers.csv`       | Customer master data                            |
| `retail_raw.csv`      | Denormalized invoice and invoice-item data      |
| `invoices_sample.csv` | Sample invoice data used for inspection/testing |

The main transactional dataset is `retail_raw.csv`.

It contains both invoice-level and line-item-level information.

---

# 5. Destination Tables

The PostgreSQL source schema contains five tables:

```text
retail_raw.stores
retail_raw.products
retail_raw.customers
retail_raw.invoices
retail_raw.invoice_items
```

The tables contain technical ingestion metadata such as:

```text
raw_id
ingestion_ts
```

where applicable.

The PostgreSQL schema is designed to accept the generated source data without applying business-level data-quality corrections.

---

## 5.1 Stores

```text
raw_id
store_id
store_name
region
state
city
zip_code
manager_name
opened_date
square_footage
ingestion_ts
```

---

## 5.2 Products

```text
raw_id
product_id
product_name
category
subcategory
unit_price
...
ingestion_ts
```

---

## 5.3 Customers

```text
raw_id
customer_id
customer_email
...
ingestion_ts
```

---

## 5.4 Invoices

Invoice headers are stored in:

```text
retail_raw.invoices
```

with the logical structure:

```text
raw_id
invoice_id
store_id
customer_id
invoice_date
payment_method
ingestion_ts
```

---

## 5.5 Invoice Items

Invoice line items are stored in:

```text
retail_raw.invoice_items
```

with the logical structure:

```text
raw_id
invoice_id
line_item
store_id
product_id
quantity
unit_price
discount
line_total
ingestion_ts
```

---

# 6. Source-to-Table Mapping

The source files are mapped into the PostgreSQL source tables as follows:

```text
stores.csv
      ↓
retail_raw.stores

products.csv
      ↓
retail_raw.products

customers.csv
      ↓
retail_raw.customers

retail_raw.csv
      ├── distinct invoice_id
      │       ↓
      │   retail_raw.invoices
      │
      └── all rows
              ↓
          retail_raw.invoice_items
```

The denormalized `retail_raw.csv` therefore provides the source information required to populate both the invoice header and invoice-item tables.

No additional PostgreSQL tables are created during this loading process.

---

# 7. Loader

The PostgreSQL loading process is implemented in:

```text
01_infra/load_to_postgres.py
```

The loader is responsible for moving the generated CSV data into the existing PostgreSQL source schema.

---

## 7.1 Design Principles

### 1. Map by column name

The loader does not assume that CSV column order matches PostgreSQL column order.

It reads the destination table metadata from PostgreSQL and maps columns by normalized names.

The normalization process converts names to a common representation:

```text
lowercase
+
non-alphanumeric characters → _
```

For example:

```text
Store Name → store_name
StoreID    → store_id
```

This prevents failures caused by differences between source column names and destination column names.

---

### 2. Preserve source data

The loader should not apply business-level data-quality corrections.

The purpose of M3 is to establish the source system, not to clean it.

Examples of issues that may intentionally remain in the source include:

* NULL values
* Missing customer references
* Invalid or incomplete values
* Other intentionally generated data-quality problems

These issues will be addressed in later Medallion layers.

---

### 3. Technical compatibility only

The loader may perform the minimum technical processing required to insert data into PostgreSQL.

For example:

* Empty strings may be represented as `NULL` where required.
* Values may be converted to PostgreSQL-compatible types when necessary for ingestion.
* Database-generated technical fields such as `raw_id` and `ingestion_ts` may be populated by PostgreSQL.

However, the loader must not silently apply business rules such as:

```text
quantity > 0
line_total = quantity × unit_price × (1 - discount)
customer_id must exist
invoice must have at least one item
```

Those are data-quality rules for later stages.

---

### 4. Technical metadata

The PostgreSQL source tables may contain technical metadata such as:

```text
raw_id
ingestion_ts
```

These fields are infrastructure metadata and are not part of the original business data.

---

### 5. Repeatable execution

The loader is designed to support repeatable development runs.

The current implementation truncates the destination tables before loading the generated dataset.

This provides a deterministic development environment and avoids duplicate records when the complete synthetic dataset is loaded again.

---

# 8. Loading Invoice Data

The `retail_raw.csv` file contains both invoice-level and line-item-level information.

The loader separates the information into two PostgreSQL tables:

```text
retail_raw.invoices
retail_raw.invoice_items
```

Invoice headers are generated from the distinct invoice identifiers present in the source dataset.

Invoice items contain all source rows.

This allows the operational source model to represent the transactional relationship:

```text
Invoice
   |
   +-- Line Item
   +-- Line Item
   +-- Line Item
   ...
```

---

# 9. How to Run

From the development VM:

```powershell
.\.venv\Scripts\Activate.ps1

python 01_infra\load_to_postgres.py
```

The loader should report the tables being processed and the number of records loaded.

A successful execution ends with a committed transaction.

Example:

```text
=== stores.csv -> retail_raw.stores ===
Loaded 50 rows

=== products.csv -> retail_raw.products ===
Loaded ...

=== customers.csv -> retail_raw.customers ===
Loaded ...

=== retail_raw.csv -> retail_raw.invoices + retail_raw.invoice_items ===
Loaded ... invoices
Loaded ... invoice_items

COMMIT OK
```

The exact record counts depend on the generated M2 dataset.

---

# 10. Verification

After loading, verify the row counts from PostgreSQL:

```sql
SELECT 'stores' AS table_name, COUNT(*)
FROM retail_raw.stores

UNION ALL

SELECT 'products', COUNT(*)
FROM retail_raw.products

UNION ALL

SELECT 'customers', COUNT(*)
FROM retail_raw.customers

UNION ALL

SELECT 'invoices', COUNT(*)
FROM retail_raw.invoices

UNION ALL

SELECT 'invoice_items', COUNT(*)
FROM retail_raw.invoice_items;
```

Inspect sample records:

```sql
SELECT *
FROM retail_raw.invoices
LIMIT 5;
```

```sql
SELECT *
FROM retail_raw.invoice_items
LIMIT 5;
```

The purpose of this verification is to confirm that the source dataset was loaded successfully.

It is **not** intended to validate or correct business data quality at this stage.

---

# 11. Data Quality Philosophy

Data-quality problems intentionally present in the generated dataset must survive the M3 loading process.

This is an important architectural requirement.

For example:

```text
PostgreSQL SOURCE

invoice_id | customer_id | ...
-----------+-------------+------
100001     | 12345       | ...
100002     | NULL        | ...
100003     | 98765       | ...
```

The `NULL` value should not be silently replaced or removed during M3.

Later, Silver can evaluate the record:

```text
invoice_id = 100002
        ↓
customer_id IS NULL
        ↓
DQ RULE FAILED
        ↓
QUARANTINE
        ↓
error_code
error_description
source_lineage
```

This allows the project to demonstrate the complete data-quality lifecycle instead of hiding errors during ingestion.

---

# 12. Troubleshooting Log

| Error                          | Root Cause                                                | Resolution                                              |
| ------------------------------ | --------------------------------------------------------- | ------------------------------------------------------- |
| `InvalidTextRepresentation`    | CSV column order did not match the PostgreSQL table order | Map columns by normalized name                          |
| `NotNullViolation` on `raw_id` | Technical metadata column was handled incorrectly         | Allow PostgreSQL to generate `raw_id` when not provided |
| `sales.csv not found`          | Generator produces `retail_raw.csv` instead               | Map `retail_raw.csv` to invoices and invoice items      |
| WSL IP changes after reboot    | WSL2 IP is dynamic                                        | Re-run the Windows `portproxy` configuration            |
| Business data-quality issue    | Intentional source-data condition                         | Preserve it for downstream Silver/Quarantine processing |

---

# 13. Milestone Outcome

At the end of M3, the project has a populated PostgreSQL source system containing:

```text
50 stores
products
~100,000 customers
invoices
invoice items
```

The generated dataset is available in PostgreSQL and can be used as the source for the next stage of the pipeline.

The important outcome is that PostgreSQL represents the **source of truth for the simulated operational system**, including its intentional data-quality imperfections.

No business-level data cleansing is performed during M3.

---

# 14. Next Steps

## M4 - PostgreSQL → Parquet Extraction

The next milestone is to extract the PostgreSQL source data into Parquet files.

The extraction process will:

* Read data from PostgreSQL
* Preserve source values
* Preserve intentional NULLs and data-quality issues
* Generate Parquet files with deterministic schemas
* Handle large transactional tables efficiently
* Provide reliable extraction for the invoice and invoice-item datasets
* Prepare the data for Databricks ingestion

The extraction layer must not correct business data.

---

## M5 - Databricks RAW and Bronze

After Parquet extraction:

```text
PostgreSQL
     ↓
Parquet
     ↓
Databricks RAW
     ↓
Bronze
```

RAW will provide an immutable landing representation of the extracted source data.

Bronze will provide a structured representation suitable for downstream processing while maintaining source traceability.

No business-level correction should occur simply because data moves from RAW to Bronze.

---

## M6 - Silver, Data Quality and Quarantine

Silver will introduce the project's data-quality processing.

The pipeline will:

* Validate business rules
* Identify invalid records
* Clean and standardize valid records
* Separate invalid records into quarantine
* Assign error codes
* Store error descriptions
* Preserve source lineage
* Track rejected records

Example:

```text
Bronze
   |
   +--------------------+
   |                    |
VALID                 INVALID
   |                    |
   ↓                    ↓
Silver              Quarantine
   |                    |
   +---------+----------+
             |
             ↓
       Gold Data Quality
```

---

## M7 - Gold

Gold will contain the analytical dimensional model used by downstream consumers.

Expected components include:

* Fact tables
* Dimension tables
* Business metrics
* Aggregations
* Data-quality/error model

---

## M8 - Power BI

Power BI will consume the Gold layer and provide:

### Business analytics

Examples:

* Sales
* Revenue
* Products
* Stores
* Customers
* Regional performance

### Data Quality / Error Correction

Examples:

* Number of rejected records
* Errors by type
* Errors by source
* Errors by table
* Error trends
* Corrected vs. rejected records
* Data-quality KPIs

---

# 15. Architecture Principle

The project intentionally separates **data ingestion** from **data quality**.

```text
SOURCE
PostgreSQL
   |
   | Preserve source data
   ↓
PARQUET
   |
   | Preserve source data
   ↓
RAW
   |
   | Preserve source data + lineage
   ↓
BRONZE
   |
   | Apply data-quality rules
   ↓
SILVER
   ├───────────────┐
   ↓               ↓
CLEAN DATA     QUARANTINE
   |               |
   └───────┬───────┘
           ↓
         GOLD
           |
     +-----+------+
     |            |
Analytics    Data Quality
     |            |
     +-----+------+
           ↓
       POWER BI
```

This separation makes it possible to demonstrate not only how data is moved through a modern data platform, but also how data-quality problems are detected, classified, isolated and ultimately exposed to business users.
