# M5 — Upload PostgreSQL Parquet Files to Databricks RAW

**Status: ✅ Completed / validated**

## Purpose

This step moves the Parquet extraction produced by M4 from the local WSL environment into the Databricks RAW landing zone.

The architecture is:

```text
PostgreSQL
    │
    ▼
M4 — Parquet Extraction
    │
    ▼
data/parquet_export/
    │
    │  M5 — Upload
    ▼
Databricks Unity Catalog Volume
    │
    ▼
RAW
    │
    ▼
Bronze
```

The purpose of this step is **file delivery only**.

It does not:

- transform business data
- clean records
- deduplicate records
- change data types
- apply business rules
- merge files
- overwrite previous extraction files by default

The Parquet files produced by M4 are treated as immutable source-extraction artifacts.

---

## Validated Databricks Environment

The M5 implementation has been validated against the project's Databricks
Free Edition workspace.

Confirmed Unity Catalog hierarchy:

```text
workspace
└── retail
    └── Volumes
        └── raw
```

Therefore the validated RAW Volume path is:

```text
/Volumes/workspace/retail/raw
```

The uploader successfully authenticates through the Databricks CLI/SDK
profile configured for the workspace and uploads the M4 Parquet files while
preserving their filenames.

---

## Databricks Free Edition

The project uses Databricks Free Edition.

Free Edition provides serverless compute and Unity Catalog capabilities subject to the edition's limitations and fair-use policy.

For this project, the important storage concept is a **Unity Catalog Volume**.

Databricks documents Volumes as the governed storage mechanism for non-tabular files such as Parquet, and they can be used as ingestion landing areas for Auto Loader or other file-based ingestion mechanisms.

The standard Volume path is:

```text
/Volumes/<catalog>/<schema>/<volume>/
```

For the current project, the intended context is:

```text
catalog = workspace
schema  = retail
```

The Volume name has been verified in Databricks Catalog Explorer as `raw`.

The uploader still makes the complete destination configurable, but the project default is now a validated path.

---

## Expected RAW Volume

The default destination configured by the script is:

```text
/Volumes/workspace/retail/raw
```

This assumes that a Unity Catalog Volume named:

```text
raw
```

exists under:

```text
workspace.retail
```

If the Volume has a different name, do **not** modify the Python source.

Use:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --destination /Volumes/workspace/retail/<volume_name>
```

The destination may also contain an optional subdirectory.

Example:

```text
/Volumes/workspace/retail/raw
/Volumes/workspace/retail/raw/postgres
```

---

## RAW layout

The Parquet source files remain flat in the RAW landing directory.

Example:

```text
/Volumes/workspace/retail/raw/
├── customers_2026_09_29_part-00001.parquet
├── customers_2026_09_29_part-00002.parquet
├── invoice_items_2026_09_29_part-00001.parquet
├── invoice_items_2026_09_29_part-00002.parquet
├── invoices_2026_09_29_part-00001.parquet
├── products_2026_09_29_part-00001.parquet
├── stores_2026_09_29_part-00001.parquet
└── _metadata/
    ├── customers_2026_09_29_extraction_manifest.json
    ├── invoice_items_2026_09_29_extraction_manifest.json
    ├── invoices_2026_09_29_extraction_manifest.json
    ├── products_2026_09_29_extraction_manifest.json
    ├── stores_2026_09_29_extraction_manifest.json
    └── _extraction_run_manifest_2026_09_29.json
```

The `_metadata` directory is deliberately separated from the Parquet landing area.

This prevents downstream Parquet ingestion from treating technical JSON manifests as source data.

---

## Why the original Parquet filename is preserved

M4 creates filenames containing:

```text
<table>_<YYYY_MM_DD>_part-<NNNNN>.parquet
```

For example:

```text
invoice_items_2026_09_29_part-00180.parquet
```

M5 does not rename these files.

The same filename is uploaded to Databricks.

This preserves the source-file identity across the boundary:

```text
PostgreSQL
    ↓
invoice_items_2026_09_29_part-00180.parquet
    ↓
Databricks RAW
    ↓
invoice_items_2026_09_29_part-00180.parquet
```

This is important for downstream incremental file ingestion and lineage.

A subsequent extraction creates a different filename:

```text
invoice_items_2026_09_30_part-00180.parquet
```

Therefore the arrival of a new extraction is represented by a new file rather than by overwriting the previous extraction.

---

## Incremental ingestion design

The intended long-term architecture is file-based incremental ingestion.

The landing area receives new extraction files:

```text
2026-09-29
    customers_2026_09_29_part-00001.parquet
    invoices_2026_09_29_part-00001.parquet
    ...

2026-09-30
    customers_2026_09_30_part-00001.parquet
    invoices_2026_09_30_part-00001.parquet
    ...
```

The files remain available in RAW.

Downstream Databricks ingestion can then discover newly arrived files and process them incrementally.

Databricks documents Unity Catalog Volumes as suitable landing locations for ingestion technologies including Auto Loader and `COPY INTO`.

The M5 uploader itself does not implement Auto Loader.

That responsibility belongs to the next ingestion layer.

---

## Authentication

The uploader uses the Databricks SDK for Python:

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()
```

The script does not contain a Databricks token or password.

Authentication is delegated to the Databricks SDK authentication chain.

This allows the same source code to work with a configured local Databricks profile or environment-based authentication.

For local development, configure Databricks authentication according to the Databricks CLI/SDK documentation.

Do not commit credentials to Git.

If the project uses `.env` for local credentials, `.env` must remain ignored by Git.

---

## Install dependency

Inside the project's Python virtual environment:

```bash
pip install databricks-sdk
```

If `python-dotenv` is already used by the project, the uploader will automatically load the project root `.env`.

The uploader does not require PySpark.

It runs locally in WSL and communicates with Databricks through the SDK.

---

## First verification

Before uploading data, verify the target Volume in Databricks Catalog Explorer.

The expected object hierarchy is:

```text
workspace
└── retail
    └── raw
```

If `raw` does not exist, create the Volume in Databricks first.

The user running the uploader needs the appropriate permissions on the Volume, including write access.

---

## Dry run

Before the first real upload:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --dry-run
```

This lists the files that would be uploaded without sending anything to Databricks.

Example:

```text
Databricks RAW upload
========================================================================
Source:             .../data/parquet_export
Destination:        /Volumes/workspace/retail/raw
Extraction date:    all available dates
Include manifests:  False
Overwrite:          False

Parquet files:      7
Manifest files:     0

DRY RUN — no files will be uploaded.
```

---

## Upload the available Parquet files

Normal execution:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py
```

The default source is:

```text
data/parquet_export/
```

The default destination is:

```text
/Volumes/workspace/retail/raw
```

---

## Upload one extraction date

To upload only the extraction generated on September 29, 2026:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --extraction-date 2026_09_29
```

This matches files containing:

```text
_2026_09_29_
```

in their filename.

This is useful when the local extraction directory contains several days of files.

---

## Upload Parquet plus extraction manifests

To also upload the M4 technical manifests:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --include-manifests
```

Parquet files go to:

```text
/Volumes/workspace/retail/raw/
```

JSON manifests go to:

```text
/Volumes/workspace/retail/raw/_metadata/
```

The manifests are therefore available in Databricks without becoming part of the Parquet source-data landing set.

---

## Validated upload

The M5 uploader was tested in WSL using the configured Databricks profile.

The authentication was verified with:

```bash
databricks current-user me --profile retail-dev
```

The uploader was then used to upload a controlled single file before
executing the full upload.

Example:

```bash
export DATABRICKS_CONFIG_PROFILE=retail-dev

python 05_databricks/raw/upload_parquet_to_databricks.py \
  --file customers_2026_09_29_part-00001.parquet
```

The local extraction directory contained 500 Parquet files at the time of
the M5 validation.

The single-file test was used deliberately before the complete upload to
validate the full path:

```text
WSL
  ↓
Databricks SDK
  ↓
workspace.retail.raw
  ↓
customers_2026_09_29_part-00001.parquet
```

After successful validation, the uploader can be executed without `--file`
to upload the complete set of matching Parquet files.

---

## Existing files

The uploader does **not** overwrite existing files by default.

If:

```text
invoice_items_2026_09_29_part-00180.parquet
```

already exists in RAW, a normal execution stops rather than silently replacing it.

This is intentional.

RAW is an immutable landing area and the extraction filenames represent file identity.

An explicit replacement can be requested with:

```bash
python 05_databricks/raw/upload_parquet_to_databricks.py \
  --overwrite
```

`--overwrite` should be considered an exceptional operational action, not the normal daily mode.

---

## Scheduled execution

The intended future execution chain is:

```text
cron / scheduler
      │
      ▼
04_extraction/export_postgres_to_parquet.py
      │
      ▼
data/parquet_export/
      │
      ▼
05_databricks/raw/upload_parquet_to_databricks.py
      │
      ▼
Databricks RAW Volume
      │
      ▼
incremental Databricks ingestion
```

The uploader can therefore be called after a successful M4 extraction.

For example:

```bash
python 04_extraction/export_postgres_to_parquet.py --all && \
python 05_databricks/raw/upload_parquet_to_databricks.py
```

The `&&` is intentional: the upload should not start when the extraction process fails.

---

## Technical controls

M5 provides the following controls:

### Source identity

The original M4 filename is preserved.

### Incremental file identity

The extraction date remains part of the filename.

### No silent overwrite

Existing files cause an error unless `--overwrite` is explicitly specified.

### Manifest separation

Technical JSON manifests are placed under `_metadata`.

### Destination configurability

The Volume path is configurable without changing Python source code.

### Dry-run mode

Files can be reviewed before transmission.

### Date-scoped upload

A specific extraction date can be selected.

---

## Relationship with RAW

RAW is the first Databricks landing layer.

The intended responsibility boundary is:

```text
M4
PostgreSQL → Parquet
    │
    │ source extraction
    ▼
M5
Local Parquet → Databricks Volume
    │
    │ file landing
    ▼
RAW
    │
    │ ingestion
    ▼
Bronze
```

M5 must not perform transformations that belong to Bronze or Silver.

For example, if PostgreSQL contains:

```text
quantity = -3
```

M4 extracts:

```text
quantity = -3
```

M5 uploads the Parquet containing:

```text
quantity = -3
```

The value remains unchanged.

---

## M5 acceptance criteria

M5 is considered complete when:

- [x] Databricks Free Edition workspace is accessible.
- [x] Unity Catalog is available.
- [x] `workspace.retail` is confirmed as the target catalog/schema context.
- [x] The `raw` Unity Catalog Volume is created and verified.
- [x] The uploader authenticates without credentials embedded in source code.
- [x] `upload_parquet_to_databricks.py` is located under `05_databricks/raw/`.
- [x] Local Parquet files are discovered from `data/parquet_export/`.
- [x] Original M4 filenames are preserved.
- [x] Extraction dates remain part of the filenames.
- [x] Files are uploaded to the RAW Volume.
- [x] Existing files are not overwritten by default.
- [x] `--overwrite` is explicit.
- [x] Technical manifests can be uploaded separately under `_metadata`.
- [x] A specific extraction date can be selected.
- [x] Dry-run mode works.
- [x] The process can be chained after the M4 extraction.
- [x] No business transformation occurs during upload.

---

## Next step

M5 is complete.

The next implementation boundary is **incremental Databricks RAW → Bronze
ingestion**.

The next step should consume newly arrived Parquet files from the Unity
Catalog Volume without reprocessing files that have already been ingested.

The expected flow is:

```text
Unity Catalog Volume
        │
        │ new Parquet files
        ▼
Databricks RAW
        │
        │ technical ingestion metadata
        ▼
Bronze Delta
```

The next implementation should therefore focus on **incremental discovery and ingestion of newly arrived Parquet files**, rather than re-uploading or reprocessing the complete historical landing area.
