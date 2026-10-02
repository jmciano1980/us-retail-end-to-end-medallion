# M7 — Silver Creation and Loading in Databricks

## 1. Purpose

This document describes how to implement and execute **M7 — Silver Creation and Loading** in Databricks.

M7 is the first business-data-quality layer of the project. Bronze preserves the source payload, including NULLs, invalid values, duplicate business identifiers and referential-integrity problems. Silver evaluates that Bronze data against explicit business and technical quality rules.

The M7 implementation creates:

- clean/conformed Silver Delta tables;
- quarantine Delta tables for rejected records;
- a Silver control table;
- a PySpark Job that performs validation, referential-integrity checks, invoice-level atomicity, type conversion and reconciliation.

---

## 2. M7 Architecture

```text
PostgreSQL
    |
    | M4 — extraction
    v
Parquet
    |
    | M5 — upload
    v
RAW Volume
/Volumes/workspace/retail/raw
    |
    | M6 — ingestion
    v
workspace.bronze
    |
    | M7 — quality / typing / quarantine
    +-----------------------------+
    |                             |
    v                             v
workspace.silver            workspace.silver
clean tables                quarantined_* tables
```

The M7 code belongs in the Databricks Workspace:

```text
Workspace
└── retail
    └── silver
        ├── 01_silver_tables_creation.sql
        └── 02_silver_databricks_load.py
```

The Bronze and Silver data are Delta tables under the `workspace` catalog.

---

## 3. Silver Tables

The SQL script creates:

```text
workspace.silver._silver_file_ingestion_log
workspace.silver.customers
workspace.silver.invoice_items
workspace.silver.invoices
workspace.silver.products
workspace.silver.stores
```

and the quarantine tables:

```text
workspace.silver.quarantined_customers
workspace.silver.quarantined_invoice_items
workspace.silver.quarantined_invoices
workspace.silver.quarantined_products
workspace.silver.quarantined_stores
```

All business and quarantine tables contain:

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

For fields that require type conversion, quarantine tables also retain the raw source value in a `*_raw` column when useful for diagnosis.

---

## 4. Silver Quality Principles

Silver follows these principles:

1. Bronze remains unchanged.
2. No invalid source row is silently discarded.
3. Invalid records are explicitly quarantined.
4. Business identifiers must be present and unique where they are used as entity identifiers.
5. Foreign-key references must point to one valid Silver entity.
6. Duplicate identifiers are treated as ambiguous and therefore invalid references.
7. Date fields must contain valid dates.
8. Numeric fields must be convertible to the expected numeric type.
9. Negative prices/costs are rejected.
10. Quantity and line-item numbers must be positive.
11. Invoice headers and invoice lines are treated as one transaction-level quality unit.
12. A NULL `Customer_ID` is an accepted anonymous retail transaction; it is not quarantined.
13. A supplied `Customer_ID` must resolve to exactly one valid customer.

---

## 5. Customer Rules

A customer is valid only if it has:

- `customer_id`
- `first_name`
- `last_name`
- `street_address`
- `city`
- `state`
- `zip_code`

In addition:

- `customer_id` must not be duplicated;
- `join_date`, when populated, must be a valid `DATE`;
- blank strings are treated as missing values.

Invalid customers are written to:

```text
workspace.silver.quarantined_customers
```

---

## 6. Product Rules

A product is valid only if it has:

- `product_id`
- `sku`
- `product_name`
- `category`
- `subcategory`
- `brand`
- `unit_price`
- `cost`

In addition:

- `product_id` must be unique;
- `unit_price` and `cost` must be numeric;
- `unit_price >= 0`;
- `cost >= 0`.

Invalid products are written to:

```text
workspace.silver.quarantined_products
```

---

## 7. Store Rules

A store is valid only if it has:

- `store_id`
- `store_name`
- `city`
- `state`
- `zip_code`
- `region`
- `manager_name`
- `opened_date`

In addition:

- `store_id` must be unique;
- `opened_date` must be a valid `DATE`;
- `square_footage`, when populated, must be numeric and non-negative.

Invalid stores are written to:

```text
workspace.silver.quarantined_stores
```

---

## 8. Invoice Header Rules

An invoice header is valid when it has:

- `invoice_id`
- `store_id`
- `invoice_date`
- `payment_method`

Customer identification is optional:

- `customer_id = NULL` is accepted as an anonymous / unidentified customer transaction;
- when `customer_id` is supplied, it must reference exactly one valid Silver customer;
- a supplied customer ID that does not exist in the valid customer dimension is quarantined;
- a NULL customer ID does not create a DQ error and does not, by itself, quarantine the invoice.

In addition:

- `invoice_id` must be unique;
- `invoice_date` must be a valid `DATE`;
- `store_id` must reference exactly one valid Silver store.

Invalid invoice headers are written to:

```text
workspace.silver.quarantined_invoices
```

---

## 9. Invoice Item Rules

An invoice item is valid only if it has:

- `invoice_id`
- `line_item`
- `product_id`
- `quantity`
- `unit_price`

In addition:

- `line_item` must be a positive integer;
- `quantity` must be a positive integer;
- `product_id` must reference exactly one valid Silver product;
- `unit_price` must be numeric and non-negative;
- `discount`, when populated, must be numeric and non-negative;
- `(invoice_id, line_item)` must be unique.

Invalid invoice items are written to:

```text
workspace.silver.quarantined_invoice_items
```

---

## 10. Invoice Atomicity Rule

This is one of the most important M7 rules.

An invoice is considered a valid Silver transaction only when:

```text
Header is valid
AND
Store reference is valid
AND
Customer_ID is NULL OR the supplied Customer_ID references one valid customer
AND
Invoice has at least one item
AND
ALL invoice items are valid
```

Therefore:

```text
If one invoice item is invalid
        |
        +--> quarantine that item
        +--> quarantine the invoice header
        +--> quarantine ALL other items belonging to the invoice
```

Likewise, if the invoice header is invalid:

```text
Invalid header
    |
    +--> quarantine header
    +--> quarantine every invoice item belonging to that invoice
```

This prevents Silver from containing a transaction whose header and details are only partially valid.

---

## 11. Referential Integrity

The following references are checked:

```text
invoices.store_id       -> valid workspace.silver.stores.store_id
invoices.customer_id    -> valid workspace.silver.customers.customer_id
                         (only when Customer_ID is supplied; NULL is accepted)
invoice_items.product_id -> valid workspace.silver.products.product_id
invoice_items.invoice_id  -> valid invoice header
```

A reference is valid only when the target identifier exists exactly once among valid dimension records.

If an identifier is duplicated in the source dimension table, it is considered ambiguous and therefore invalid for downstream references.

---

## 12. DQ Diagnostic / Dry-Run Analysis

Run the PySpark loader with:

```text
--dry-run
```

Dry-run mode performs the complete M7 validation but does not write Silver or quarantine tables. It prints a detailed report designed to distinguish actual source errors from records quarantined only because of invoice atomicity.

The report includes:

- Bronze, Silver-valid and quarantine counts;
- direct root DQ reasons for invoice headers and invoice items;
- root item error counts plus distinct invoices affected;
- accepted anonymous invoices (`Customer_ID IS NULL`);
- supplied-but-invalid customer IDs;
- invoices with at least one directly invalid item;
- final invoice and item quarantine rates;
- comparison against the 1% synthetic-DQ sanity reference;
- reconciliation of the header and bad-item quarantine paths;
- samples of directly bad invoice headers;
- samples of accepted anonymous invoices;
- samples of invoices quarantined because of bad items;
- samples of directly bad invoice items.

The report uses separate root-cause and cascade concepts. In particular, `INVOICE_QUARANTINED_BY_BAD_HEADER_OR_ITEM` is a cascade marker, not a new independent source error.

The customer-specific interpretation is:

```text
Customer_ID = NULL
    -> accepted anonymous transaction
    -> NOT a DQ error

Customer_ID supplied + ID exists exactly once
    -> valid customer reference

Customer_ID supplied + ID does not exist / is not valid
    -> INVALID_CUSTOMER_REFERENCE
    -> invoice header quarantined
```

This distinction is important for downstream Power BI: anonymous sales remain in the sales facts, while customer-identification metrics can separately measure the proportion of transactions/revenue associated with known customers.

## 12. Duplicate Handling

Duplicates are not deleted silently.

They are quarantined and remain traceable through `_source_row_id`.

The following uniqueness rules are applied:

```text
customers:       customer_id
products:        product_id
stores:          store_id
invoices:        invoice_id
invoice_items:   invoice_id + line_item
```

This is deliberately different from Bronze, where duplicate business keys are preserved as source information.

---

## 14. Date Validation

The following fields are converted to Spark `DATE`:

```text
customers.join_date
stores.opened_date
invoices.invoice_date
```

A populated date that cannot be parsed as `yyyy-MM-dd` is invalid and is quarantined.

The original Bronze value is retained in the corresponding `*_raw` quarantine column.

---

## 15. Numeric Type Standardization

Silver establishes business-oriented numeric types:

```text
products.unit_price       DECIMAL(18,2)
products.cost             DECIMAL(18,2)
stores.square_footage     BIGINT
invoice_items.line_item   BIGINT
invoice_items.quantity    BIGINT
invoice_items.unit_price  DECIMAL(18,2)
invoice_items.discount    DECIMAL(18,2)
```

The Bronze layer remains string-oriented and source-preserving. Type conversion therefore belongs here, in Silver.

---

## 16. Step 1 — Upload the SQL and PySpark Files

Create the following local repository structure:

```text
05_databricks/
└── silver/
    ├── 01_silver_tables_creation.sql
    └── 02_silver_databricks_load.py
```

Upload both files to:

```text
Workspace/retail/silver/
```

The final Workspace structure should be:

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

Do not upload these scripts to the RAW Volume.

---

## 17. Step 2 — Create the Silver Tables

Open:

```text
Workspace/retail/silver/01_silver_tables_creation.sql
```

Execute it in a Databricks SQL-capable environment.

The script creates:

```text
workspace.silver
```

and all Silver and quarantine tables.

Validate the result with:

```sql
SHOW TABLES IN workspace.silver;
```

Then verify the schema:

```sql
DESCRIBE workspace.silver.customers;
DESCRIBE workspace.silver.products;
DESCRIBE workspace.silver.stores;
DESCRIBE workspace.silver.invoices;
DESCRIBE workspace.silver.invoice_items;
```

---

## 18. Step 3 — Verify Bronze Before Running M7

M7 reads from:

```text
workspace.bronze
```

Verify that the M6 Job has completed successfully before starting M7.

At minimum:

```sql
SELECT COUNT(*) FROM workspace.bronze.customers;
SELECT COUNT(*) FROM workspace.bronze.products;
SELECT COUNT(*) FROM workspace.bronze.stores;
SELECT COUNT(*) FROM workspace.bronze.invoices;
SELECT COUNT(*) FROM workspace.bronze.invoice_items;
```

The M7 process does not read the RAW Volume directly. Bronze is the input to Silver.

---

## 19. Step 4 — Do Not Run the PySpark File as a Notebook

The file:

```text
02_silver_databricks_load.py
```

is designed to run as a **Python script task in a Databricks Job**.

Do not create a notebook just to execute this script.

The correct execution model is:

```text
Databricks Job
    |
    └── Python script task
            |
            └── 02_silver_databricks_load.py
```

This follows the same execution model used for M6.

---

## 19. Step 5 — Create the M7 Job

In Databricks:

1. Open **Jobs & Pipelines**.
2. Select **Create job**.
3. Set the Job name to:

```text
M7 - Silver Data Quality Load
```

Add one task.

Recommended task name:

```text
load_silver
```

Set task type to:

```text
Python script
```

Do not select Notebook.

---

## 20. Step 6 — Select the PySpark Script

Configure the task source as:

```text
Source: Workspace
```

Select:

```text
Workspace/retail/silver/02_silver_databricks_load.py
```

The task should look approximately like:

```text
Task name:
    load_silver

Task type:
    Python script

Source:
    Workspace

File:
    /Workspace/retail/silver/02_silver_databricks_load.py
```

---

## 21. Step 7 — Configure Compute

Use a Databricks compute option that supports:

- Apache Spark;
- Delta Lake;
- PySpark;
- Delta Lake MERGE operations.

If Serverless compute is available for Python Jobs in the workspace, it can be used. Otherwise use an available Databricks compute resource appropriate for the workload.

Because the project contains a large invoice-item dataset, do not treat M7 as a small local-data notebook workload.

---

## 22. Step 8 — Parameters

For the standard execution, no parameters are required.

The script reads:

```text
workspace.bronze.customers
workspace.bronze.products
workspace.bronze.stores
workspace.bronze.invoices
workspace.bronze.invoice_items
```

Optional parameters are available for controlled execution:

```text
--table customers
--table products
--table stores
--table invoices
--table invoice_items
```

Multiple table parameters can be supplied.

Example:

```text
--table customers --table products
```

A dry run can be performed with:

```text
--dry-run
```

Do not add notebook parameters such as `-f`.

---

## 23. Step 9 — Run M7

Save the Job and select:

```text
Run now
```

The PySpark script will:

1. ensure the Silver schema exists;
2. read Bronze tables;
3. standardize the required data types;
4. validate required fields;
5. detect duplicate business identifiers;
6. validate dimension references;
7. validate invoice headers;
8. validate invoice items;
9. enforce invoice atomicity;
10. write valid records to Silver;
11. write rejected records to quarantine;
12. report Bronze/Silver/quarantine counts.

---

## 24. Step 10 — Validate Silver Results

Check row counts:

```sql
SELECT COUNT(*) FROM workspace.silver.customers;
SELECT COUNT(*) FROM workspace.silver.products;
SELECT COUNT(*) FROM workspace.silver.stores;
SELECT COUNT(*) FROM workspace.silver.invoices;
SELECT COUNT(*) FROM workspace.silver.invoice_items;
```

Check quarantine counts:

```sql
SELECT COUNT(*) FROM workspace.silver.quarantined_customers;
SELECT COUNT(*) FROM workspace.silver.quarantined_products;
SELECT COUNT(*) FROM workspace.silver.quarantined_stores;
SELECT COUNT(*) FROM workspace.silver.quarantined_invoices;
SELECT COUNT(*) FROM workspace.silver.quarantined_invoice_items;
```

---

## 25. Step 11 — Validate Data Quality

### Customers

```sql
SELECT customer_id, COUNT(*) AS cnt
FROM workspace.silver.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

Expected result: no rows.

### Products

```sql
SELECT product_id, COUNT(*) AS cnt
FROM workspace.silver.products
GROUP BY product_id
HAVING COUNT(*) > 1;
```

Expected result: no rows.

### Stores

```sql
SELECT store_id, COUNT(*) AS cnt
FROM workspace.silver.stores
GROUP BY store_id
HAVING COUNT(*) > 1;
```

Expected result: no rows.

### Invoices

```sql
SELECT invoice_id, COUNT(*) AS cnt
FROM workspace.silver.invoices
GROUP BY invoice_id
HAVING COUNT(*) > 1;
```

Expected result: no rows.

### Invoice items

```sql
SELECT invoice_id, line_item, COUNT(*) AS cnt
FROM workspace.silver.invoice_items
GROUP BY invoice_id, line_item
HAVING COUNT(*) > 1;
```

Expected result: no rows.

---

## 26. Step 12 — Validate Referential Integrity

### Invoice → Store

```sql
SELECT COUNT(*) AS invalid_store_refs
FROM workspace.silver.invoices i
LEFT JOIN workspace.silver.stores s
    ON i.store_id = s.store_id
WHERE s.store_id IS NULL;
```

Expected result: `0`.

### Invoice → Customer

```sql
SELECT COUNT(*) AS invalid_customer_refs
FROM workspace.silver.invoices i
LEFT JOIN workspace.silver.customers c
    ON i.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

Expected result: `0`.

### Invoice item → Product

```sql
SELECT COUNT(*) AS invalid_product_refs
FROM workspace.silver.invoice_items ii
LEFT JOIN workspace.silver.products p
    ON ii.product_id = p.product_id
WHERE p.product_id IS NULL;
```

Expected result: `0`.

---

## 27. Step 13 — Validate Invoice Atomicity

The Silver model must never contain a header without all of its valid details.

Check that every Silver invoice has at least one line:

```sql
SELECT i.invoice_id
FROM workspace.silver.invoices i
LEFT JOIN workspace.silver.invoice_items ii
    ON i.invoice_id = ii.invoice_id
GROUP BY i.invoice_id
HAVING COUNT(ii.line_item) = 0;
```

Expected result: no rows.

Also verify that an invoice cannot exist simultaneously in clean and quarantine paths:

```sql
SELECT invoice_id
FROM workspace.silver.invoices
INTERSECT
SELECT invoice_id
FROM workspace.silver.quarantined_invoices;
```

Expected result: no rows.

---

## 28. Step 14 — Validate Quarantine Reasons

The quarantine tables are designed to make data quality measurable.

Example:

```sql
SELECT
    _dq_error_code,
    COUNT(*) AS record_count
FROM workspace.silver.quarantined_customers
GROUP BY _dq_error_code
ORDER BY record_count DESC;
```

And for invoice items:

```sql
SELECT
    _dq_error_code,
    COUNT(*) AS record_count
FROM workspace.silver.quarantined_invoice_items
GROUP BY _dq_error_code
ORDER BY record_count DESC;
```

This information will later feed the Gold data-quality model and Power BI Error Correction dashboard.

---

## 29. Step 15 — Validate Technical Lineage

Every Silver and quarantine record must retain:

```text
_source_file_name
_source_row_id
_insert_datetime_utc
_update_datetime_utc
```

Example:

```sql
SELECT
    _source_file_name,
    _source_row_id,
    _insert_datetime_utc,
    _update_datetime_utc
FROM workspace.silver.quarantined_invoice_items
LIMIT 20;
```

This makes it possible to trace a rejected Silver record back to the physical Bronze source row.

---

## 30. Step 16 — Reconciliation

M7 should be reconciled against Bronze.

For ordinary entity tables:

```text
Bronze rows = Silver valid rows + Quarantine rows
```

For invoices and invoice items, the same principle applies, with the important transaction-level rule that an invalid invoice causes its complete transaction to be quarantined.

The PySpark Job prints reconciliation counts at the end of the run.

---

## 31. Incremental / Rerun Behavior

M7 uses `_source_row_id` as the technical row identity for valid Silver records.

A rerun therefore MERGEs an already-known Silver row instead of inserting another copy.

Quarantine records are appended only when their `_source_row_id` has not already been quarantined.

This makes the Job safe to rerun after a failure or operational retry.

The `_silver_file_ingestion_log` records execution status for operational traceability.

---

## 32. M7 Acceptance Criteria

M7 is considered complete when all of the following are true:

- `workspace.silver` exists;
- all five clean Silver tables exist;
- all five quarantine tables exist;
- `_silver_file_ingestion_log` exists;
- all required technical metadata columns exist;
- required fields are enforced;
- date fields are validated and typed;
- numeric fields are validated and typed;
- duplicate business identifiers are quarantined;
- invalid foreign-key references are quarantined;
- invalid invoice lines quarantine the complete invoice;
- invalid invoice headers quarantine the complete invoice;
- Silver invoices have at least one valid line;
- Silver contains no invalid business references;
- quarantine records retain source lineage;
- rerunning the Job does not duplicate Silver rows;
- Bronze remains unchanged.

---

## 33. M7 Processing Boundary

The resulting architecture is:

```text
PostgreSQL
    |
    v
M4 — Parquet extraction
    |
    v
M5 — RAW upload
    |
    v
RAW
    |
    v
M6 — Bronze
    |
    v
M7 — Silver Data Quality
    |
    +-------------------------+
    |                         |
    v                         v
Clean Silver              Quarantine
    |                         |
    +------------+------------+
                 |
                 v
             M8 — Gold
```

M7 is the boundary where source-quality problems stop being propagated as trusted business data.
