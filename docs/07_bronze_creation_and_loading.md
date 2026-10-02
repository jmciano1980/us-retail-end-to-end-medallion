# M6 — Bronze Creation and Loading in Databricks

## 1. Purpose

This document describes how to implement and execute **M6 — Bronze Creation and Loading** in Databricks.

The objective of M6 is to load every record extracted from PostgreSQL into the Bronze layer without applying business-data corrections or filtering.

The Bronze layer is an ingestion and lineage layer. Data-quality validation and correction belong to the Silver layer.

## 2. M6 Architecture

```text
PostgreSQL
    |
    | M4 — extraction
    v
Parquet files
    |
    | M5 — upload without transformation
    v
Databricks Unity Catalog Volume
/Volumes/workspace/retail/raw
    |
    | M6 — Bronze ingestion
    v
workspace.bronze
```

The Databricks Workspace contains the code used by the Job:

```text
Workspace
└── retail
    └── bronze
        ├── 01_create_bronze_tables.sql
        └── 02_load_bronze_tables.py
```

The Parquet data is stored separately in the Unity Catalog Volume:

```text
/Volumes/workspace/retail/raw/
```

**Workspace = code. Volume = data. Bronze tables = ingested Delta data.**

---

## 3. Bronze Data Preservation Rules

Bronze must preserve the source data.

It must not:

- standardize values;
- convert invalid values into valid values;
- apply business rules;
- remove records;
- filter records because of data-quality problems;
- deduplicate business keys;
- repair referential-integrity problems;
- replace NULL values;
- reject rows because they contain invalid business data.

### Duplicate business keys

Duplicate business keys are source-data conditions.

For example, if the source contains:

```text
customer_id = 1001
customer_id = 1001
```

both records must be loaded into Bronze.

The Bronze layer must never silently deduplicate them. Data-quality handling belongs to Silver.

### Technical identity

Because duplicate business keys must be preserved, the ingestion process must not use the business key as the sole physical identity of a Bronze row.

A technical source-row identity is used so that:

- duplicate business keys remain separate rows;
- the same source file can be processed idempotently;
- rerunning the Job does not create duplicate copies of the same source rows.

### Technical failures

Business-data-quality problems must not cause source rows to be rejected.

Technical failures can still cause the Job to fail, for example:

- corrupted Parquet;
- inaccessible source files;
- incompatible technical schema;
- Delta transaction failure;
- infrastructure failure.

---

# 4. Step 1 — Open the Databricks Workspace

Open the Databricks workspace used by the project.

From the left navigation menu select:

**Workspace**

The M6 code should be stored under:

```text
Workspace/
└── retail/
    └── bronze/
```

---

# 5. Step 2 — Create the Workspace Folder

If it does not already exist, create:

```text
retail
```

Inside `retail`, create:

```text
bronze
```

The final structure should be:

```text
Workspace
└── retail
    └── bronze
```

---

# 6. Step 3 — Upload the M6 Scripts

Upload both scripts into:

```text
Workspace/retail/bronze/
```

The final structure must be:

```text
Workspace
└── retail
    └── bronze
        ├── 01_create_bronze_tables.sql
        └── 02_load_bronze_tables.py
```

Do **not** upload these scripts to the RAW Volume.

The scripts belong to the Workspace.

The Parquet files belong to:

```text
/Volumes/workspace/retail/raw/
```

---

# 7. Step 4 — Verify the RAW Volume

Before creating the Bronze Job, verify that the Parquet files are available in:

```text
/Volumes/workspace/retail/raw/
```

For example:

```text
/Volumes/workspace/retail/raw/
├── customers_....parquet
├── products_....parquet
├── stores_....parquet
├── invoices_....parquet
└── invoice_items_....parquet
```

The exact filenames depend on the extraction batch.

M6 reads these files without modifying their business data.

---

# 8. Step 5 — Create the Bronze Tables

Open:

```text
Workspace/retail/bronze/01_create_bronze_tables.sql
```

Run the SQL script using a Databricks SQL-capable environment.

The script creates the Bronze objects under:

```text
workspace.bronze
```

The expected business tables are:

```text
workspace.bronze.stores
workspace.bronze.products
workspace.bronze.customers
workspace.bronze.invoices
workspace.bronze.invoice_items
```

The implementation also creates the technical ingestion/control structures required by the Bronze process.

---

# 9. Step 6 — Verify the Bronze Tables

After executing the SQL script:

```sql
SHOW TABLES IN workspace.bronze;
```

The expected result includes the Bronze business tables.

At this point the tables exist, but the M6 ingestion Job has not necessarily loaded the Parquet data.

---

# 10. Step 7 — Do NOT Run the Python File as a Notebook

The file:

```text
02_load_bronze_tables.py
```

is designed to run as a **Python script task in a Databricks Job**.

It is not intended to be executed by attaching it to a notebook kernel.

Do not create a notebook and attach the script just to execute it.

The correct execution model is:

```text
Databricks Job
    |
    └── Python script task
            |
            └── 02_load_bronze_tables.py
```

Running it through a notebook can result in Python receiving notebook-specific arguments such as:

```text
-f /.../connection.json
```

Those arguments are not application parameters for the Bronze loader.

---

# 11. Step 8 — Open Jobs & Pipelines

From the Databricks left navigation menu select:

**Jobs & Pipelines**

Then select:

**Create job**

Recommended Job name:

```text
M6 - Bronze Ingestion
```

---

# 12. Step 9 — Create the Bronze Loading Task

Under **Tasks**, add a task.

Recommended task name:

```text
load_bronze
```

For the task type select:

**Python script**

Do **not** select:

**Notebook**

This distinction is important.

---

# 13. Step 10 — Select the Python Script

Configure the Python script source as:

```text
Workspace
```

Browse to:

```text
Workspace/
└── retail/
    └── bronze/
        └── 02_load_bronze_tables.py
```

Select:

```text
02_load_bronze_tables.py
```

The task should conceptually look like:

```text
Task name:
    load_bronze

Task type:
    Python script

Source:
    Workspace

File:
    /Workspace/retail/bronze/02_load_bronze_tables.py
```

---

# 14. Step 11 — Configure Compute

Select an appropriate Databricks compute option.

If Serverless compute is available for Python Jobs in the workspace, it can be used.

Otherwise select an available Databricks compute resource appropriate for the workload.

The Job must provide the Spark/Delta capabilities required by the Python script.

---

# 15. Step 12 — Configure Parameters

For the initial execution, leave parameters empty if the script defaults are being used.

The default RAW location is:

```text
/Volumes/workspace/retail/raw
```

Therefore no RAW path parameter is required for the standard project configuration.

Do **not** manually add notebook kernel parameters such as:

```text
-f
```

or:

```text
/local_disk0/.../connection.json
```

Those are not M6 application parameters.

---

# 16. Step 13 — Save the Job

Save the Job.

The resulting configuration should be approximately:

```text
M6 - Bronze Ingestion
│
└── load_bronze
      │
      ├── Type: Python script
      ├── Source: Workspace
      ├── File: 02_load_bronze_tables.py
      └── Compute: Databricks-supported compute
```

---

# 17. Step 14 — Run the Job

Select:

**Run now**

The Job executes:

```text
02_load_bronze_tables.py
```

The script reads the Parquet files from:

```text
/Volumes/workspace/retail/raw/
```

and loads the corresponding Bronze Delta tables.

---

# 18. Step 15 — Monitor the Job

Open the active Job run and inspect the task output.

The first run should discover the available source files and process them.

The Job must not reject a file merely because its records contain:

- NULL values;
- invalid values;
- duplicate business keys;
- negative quantities;
- invalid references;
- other business-data-quality issues.

Those conditions must survive into Bronze.

---

# 19. Step 16 — Validate the Bronze Load

After the Job completes successfully, query the Bronze tables.

For example:

```sql
SELECT COUNT(*)
FROM workspace.bronze.customers;
```

Repeat for the other tables:

```sql
SELECT COUNT(*)
FROM workspace.bronze.products;

SELECT COUNT(*)
FROM workspace.bronze.stores;

SELECT COUNT(*)
FROM workspace.bronze.invoices;

SELECT COUNT(*)
FROM workspace.bronze.invoice_items;
```

The Bronze row counts should reconcile with the source Parquet extraction.

---

# 20. Step 17 — Validate Duplicate Preservation

It is particularly important to verify that duplicate source business keys were not removed.

For example:

```sql
SELECT
    customer_id,
    COUNT(*) AS row_count
FROM workspace.bronze.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

If duplicate `customer_id` values exist in the source, they must remain visible in Bronze.

The same principle applies to:

```text
store_id
product_id
invoice_id
(invoice_id, line_item)
```

depending on the source table.

Duplicate business keys are data-quality information that will be handled in Silver.

---

# 21. Step 18 — Validate Technical Metadata

Bronze rows contain technical metadata required for ingestion and lineage.

Typical metadata includes:

```text
_insert_datetime_utc
_update_datetime_utc
_source_file_name
_source_row_id
```

The metadata is separate from the original business columns.

The business columns must remain unchanged.

The technical metadata supports:

- lineage;
- incremental ingestion;
- idempotency;
- source-file tracking;
- operational troubleshooting.

---

# 22. Step 19 — Test Incremental Behavior

After the initial successful run, execute the same Job again:

**Run now**

The purpose of this second execution is to demonstrate that already processed source files are not loaded repeatedly.

The ingestion process should recognize previously processed source data and avoid creating another copy of the same physical source rows.

This is an important M6 acceptance criterion.

---

# 23. Step 20 — Adding a New Extraction Batch

When a new M4 extraction produces additional Parquet files, upload those files into:

```text
/Volumes/workspace/retail/raw/
```

Execute the same Job again.

Conceptually:

```text
Existing RAW files
        |
        | already processed
        v
   no duplicate load

New RAW files
        |
        | not previously processed
        v
    Bronze load
```

No new Bronze Job needs to be created for every extraction batch.

---

# 24. Bronze vs Silver Responsibilities

The project deliberately separates ingestion from data quality.

### Bronze

Bronze is responsible for:

- ingesting the source;
- preserving source values;
- preserving source errors;
- preserving duplicates;
- maintaining technical lineage;
- maintaining incremental ingestion;
- avoiding accidental data loss.

### Silver

Silver will be responsible for:

- identifying data-quality problems;
- applying data-quality rules;
- correcting values where appropriate;
- handling duplicate business keys;
- handling referential-integrity problems;
- quarantining records when required;
- producing clean/conformed datasets.

Therefore:

```text
RAW
 |
 | exact extraction
 v
BRONZE
 |
 | identify/correct/quarantine data quality
 v
SILVER
 |
 | business transformations
 v
GOLD
```

---

# 25. Important Design Principle

A bad source record is still a source record.

For example, if PostgreSQL contains:

```text
invoice_id = 10001
quantity = -3
```

Bronze must not change:

```text
-3
```

into:

```text
3
```

Likewise, if PostgreSQL contains:

```text
invoice_id = 10001
invoice_id = 10001
```

Bronze must not arbitrarily remove one of them.

The Bronze layer preserves the evidence of what arrived from the source.

The Silver layer determines how those records should be handled.

---

# 26. M6 Acceptance Criteria

M6 is considered complete when all of the following are true:

- [ ] `01_create_bronze_tables.sql` is stored in the Databricks Workspace.
- [ ] `02_load_bronze_tables.py` is stored in the Databricks Workspace.
- [ ] RAW Parquet files are available under `/Volumes/workspace/retail/raw/`.
- [ ] Bronze tables exist under `workspace.bronze`.
- [ ] The Python loader runs as a Databricks **Job / Python script task**.
- [ ] The loader is not executed as a notebook.
- [ ] All source records are loaded.
- [ ] Duplicate business keys are preserved.
- [ ] NULL values are preserved.
- [ ] Invalid business values are preserved.
- [ ] Referential-integrity problems are preserved.
- [ ] No business-data correction occurs in Bronze.
- [ ] Source values and field names are preserved.
- [ ] Technical metadata is populated.
- [ ] Incremental/idempotent processing prevents reloading the same source data.
- [ ] Row counts can be reconciled with the source extraction.
- [ ] The same Job can be executed again for subsequent extraction batches.

---

# 27. Final M6 Flow

```text
                 M4
        PostgreSQL extraction
                 |
                 v
          Parquet files
                 |
                 | M5
                 v
    /Volumes/workspace/retail/raw
                 |
                 | M6
                 v
       Databricks Python Job
                 |
                 v
        Bronze Delta tables
                 |
                 | M7
                 v
        Silver data quality
                 |
                 v
          Gold analytics
```

The critical architectural rule is:

> **Bronze loads what the source contains. Silver decides what is wrong and how it should be handled.**
