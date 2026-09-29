# M4 — PostgreSQL to Parquet Extraction

## Purpose

This step extracts the source data from PostgreSQL into Apache Parquet files for subsequent ingestion into the Databricks Medallion architecture.

The Parquet layer is an **extraction and transport layer**.

It is **not** a transformation layer.

The extractor must preserve the source data as faithfully as possible:

- `NULL` remains `NULL`.
- Invalid source values are not corrected.
- Referential-integrity problems are not fixed.
- Business rules are not applied.
- Records are not intentionally removed.
- No deduplication is performed.
- No business transformations are performed.

The objective is to move the source data from PostgreSQL to files while providing technical controls that make the extraction auditable and safe for downstream incremental ingestion.

---

## Source

The source database is PostgreSQL.

Default schema:

```text
retail_raw
```

Current source tables:

```text
customers
invoice_items
invoices
products
stores
```

The PostgreSQL connection is read from the project's `.env` file.

Example:

```text
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=retail_db
POSTGRES_USER=retail_admin
POSTGRES_PASSWORD=<secret>
```

The extractor automatically loads the `.env` file located at the project root. Shell environment variables take precedence when both are present.

Secrets must not be committed to Git.

---

## Output

The default destination is:

```text
data/parquet_export/
```

All tables are written directly into this directory.

There are intentionally **no folders per table**.

Example:

```text
data/parquet_export/
├── customers_2026_09_29_part-00001.parquet
├── customers_2026_09_29_part-00002.parquet
├── invoice_items_2026_09_29_part-00001.parquet
├── invoice_items_2026_09_29_part-00002.parquet
├── invoices_2026_09_29_part-00001.parquet
├── products_2026_09_29_part-00001.parquet
├── stores_2026_09_29_part-00001.parquet
├── customers_2026_09_29_extraction_manifest.json
├── invoice_items_2026_09_29_extraction_manifest.json
├── invoices_2026_09_29_extraction_manifest.json
├── products_2026_09_29_extraction_manifest.json
├── stores_2026_09_29_extraction_manifest.json
└── _extraction_run_manifest_2026_09_29.json
```

---

## Why the extraction date is part of the filename

The extraction date is deliberately part of every Parquet filename.

The naming convention is:

```text
<table>_<YYYY_MM_DD>_part-<NNNNN>.parquet
```

For example:

```text
invoice_items_2026_09_29_part-00180.parquet
```

This is an important part of the incremental ingestion design.

The extraction process is expected to be executed periodically, potentially through cron in WSL.

If the same table is extracted again on another day, the generated files have different names:

```text
invoice_items_2026_09_29_part-00001.parquet
invoice_items_2026_09_30_part-00001.parquet
```

This allows downstream file-based incremental processing to distinguish a new extraction from a previously processed file.

The filename therefore contains three pieces of technical identity:

1. Source table.
2. Extraction date.
3. Part number.

The date in the filename represents the **extraction date**, not a business date contained in the source data.

The extractor does not modify source business dates to implement this convention.

---

## Important incremental-processing principle

The Parquet files are intended to be consumed downstream by Databricks.

Therefore, the extraction process must not rely on overwriting an existing filename to represent a new extraction.

For example, this is **not** the intended pattern:

```text
invoice_items_part-00180.parquet
```

being generated every day and overwritten.

Instead:

```text
invoice_items_2026_09_29_part-00180.parquet
invoice_items_2026_09_30_part-00180.parquet
```

represent distinct extraction files.

This is particularly important for file-based incremental ingestion, where the downstream process may track files that have already been processed.

The extraction layer therefore treats the generated Parquet files as immutable extraction artifacts for that execution.

---

## Part files

Large tables are split into multiple Parquet files.

The default maximum number of rows per file is:

```text
500000
```

The part number is zero-padded to five digits:

```text
part-00001
part-00002
...
part-00180
```

The part number is local to the table and extraction date.

For example:

```text
customers_2026_09_29_part-00001.parquet
customers_2026_09_29_part-00002.parquet
```

and:

```text
invoice_items_2026_09_29_part-00001.parquet
invoice_items_2026_09_29_part-00180.parquet
```

---

## Extraction consistency

Each table extraction is performed using a PostgreSQL `REPEATABLE READ` transaction.

The extractor begins the transaction before reading the source table and uses a server-side cursor to process the result in batches.

This provides a consistent database snapshot during the extraction while avoiding the need to load the entire table into Python memory.

The extractor therefore does not require a primary key to perform the extraction.

This is important because the source model may contain tables without primary keys.

---

## Batch processing

The default batch size is:

```text
50000
```

The extractor reads rows in batches rather than loading an entire table into memory.

This provides a controlled memory footprint for large source tables.

The batch size can be changed from the command line.

Example:

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --schema retail_raw \
  --all \
  --batch-size 100000
```

---

## Source-to-file reconciliation

For every extracted table, the extractor obtains the source row count:

```sql
SELECT COUNT(*)
FROM retail_raw.<table>;
```

It then counts the rows actually written to Parquet.

The extraction is considered successful only when:

```text
source_row_count == extracted_row_count
```

The reconciliation result is recorded in the table extraction manifest and the overall extraction-run manifest.

This provides a technical control against skipped or unexpectedly duplicated records during extraction.

---

## Extraction manifests

Each table receives an extraction manifest.

Example:

```text
customers_2026_09_29_extraction_manifest.json
```

The manifest records technical information about the extraction, including:

- source schema
- source table
- extraction date
- source row count
- extracted row count
- number of Parquet files
- batch size
- rows per file
- compression
- generated files
- reconciliation status

An overall run manifest is also generated:

```text
_extraction_run_manifest_2026_09_29.json
```

This provides a single technical record for the execution.

---

## No transformation policy

The extractor must not become an ETL transformation layer.

Examples of operations that do **not** belong here:

```text
DELETE invalid records
FILTER orphan records
DEDUPLICATE customers
STANDARDIZE customer names
NORMALIZE addresses
REPAIR foreign keys
APPLY business rules
CALCULATE business metrics
```

Those concerns belong to downstream processing in the Medallion architecture.

The extraction layer should preserve the source condition so that Bronze remains an auditable representation of the source system.

---

## Running the extractor

From the project root:

```bash
python 04_extraction/export_postgres_to_parquet.py --all
```

The default output is:

```text
data/parquet_export/
```

### Extract one table

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --schema retail_raw \
  --table customers
```

### Extract multiple tables

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --schema retail_raw \
  --table customers \
  --table invoices \
  --table invoice_items
```

### Extract all tables

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --schema retail_raw \
  --all
```

### Specify a different destination

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --all \
  --destination /some/other/path
```

### Change batch size

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --all \
  --batch-size 100000
```

### Change maximum rows per Parquet file

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --all \
  --rows-per-file 1000000
```

### Change compression

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --all \
  --compression snappy
```

### Overwrite an extraction for the same extraction date

The extractor normally protects existing files.

If an extraction for the same table/date already exists and it must intentionally be regenerated, use:

```bash
python 04_extraction/export_postgres_to_parquet.py \
  --table customers \
  --overwrite
```

`--overwrite` is an explicit operator action. It is not part of the normal incremental daily execution.

---

## Cron / scheduled execution

The intended operational model is that the extractor can eventually be scheduled from WSL using cron.

For example, a daily execution could run:

```bash
python /home/martin/projects/us-retail-end-to-end-medallion/04_extraction/export_postgres_to_parquet.py --all
```

The extractor itself determines the extraction date when it starts.

Therefore the scheduled process does not need to construct the filename manually.

The resulting files will automatically contain the execution date:

```text
customers_2026_09_29_part-00001.parquet
customers_2026_09_30_part-00001.parquet
customers_2026_10_01_part-00001.parquet
```

This makes the extraction process suitable for repeated scheduled execution.

---

## Operational sequence

The intended pipeline is:

```text
PostgreSQL
    │
    │  M4 extraction
    ▼
Parquet files
    │
    │  Databricks incremental ingestion
    ▼
Bronze
    │
    ▼
Silver
    │
    ▼
Gold
```

The extraction process ends at the Parquet layer.

It does not perform the Medallion transformations.

---

## M4 acceptance criteria

M4 is considered technically complete when:

- [ ] PostgreSQL source schema is configurable.
- [ ] Individual tables can be extracted.
- [ ] Multiple tables can be extracted.
- [ ] All configured tables can be extracted.
- [ ] `.env` is loaded automatically.
- [ ] Output defaults to `data/parquet_export`.
- [ ] All Parquet files are written directly into the output directory.
- [ ] Table name is included in every Parquet filename.
- [ ] Extraction date is included in every Parquet filename.
- [ ] Filename follows `<table>_<YYYY_MM_DD>_part-<NNNNN>.parquet`.
- [ ] Large tables are split into multiple parts.
- [ ] Batch processing is used.
- [ ] A consistent PostgreSQL snapshot is used per table.
- [ ] Primary keys are not required.
- [ ] Source rows are not intentionally filtered or transformed.
- [ ] Source and extracted row counts are reconciled.
- [ ] Table-level extraction manifests are generated.
- [ ] A run-level extraction manifest is generated.
- [ ] Existing extraction files are protected unless `--overwrite` is explicitly requested.
- [ ] The output naming convention supports downstream incremental file ingestion.
- [ ] The process can be scheduled for repeated execution.

---

## Design principle

The central design principle for M4 is:

> **Extract faithfully, identify each extraction uniquely, reconcile what was extracted, and leave transformation to Databricks.**

The Parquet files are the controlled boundary between the PostgreSQL source system and the Databricks Medallion architecture.
