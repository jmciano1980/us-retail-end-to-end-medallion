# 12 — Gold Creation and Loading

## 1. Purpose

This document describes the Databricks implementation sequence for the Gold base model and the Gold aggregate serving layer. It follows:

- `09_gold_model_design.md`
- `10_bi_requirements_and_dashboard_design.md`
- `11_gold_aggregation_design.md`

The target is to serve the Executive, Store/Regional and Product dashboards from Gold aggregate tables plus dimensions, rather than routinely querying the 55M-row `fact_invoice` / `fact_invoice_item` tables. The Data Quality dashboard uses aggregate tables for its KPIs/charts and record-level DQ/quarantine datasets for its remediation grid.

These scripts implement the Gold layer and create/load tables in `workspace.gold`.

## 2. Files and execution order

| Order | File | Purpose |
|---:|---|---|
| 1 | `05_databricks/gold/01_create_gold_schema_tables.sql` | Creates Gold dimensions, facts, DQ tables and quarantine-detail tables |
| 2 | `05_databricks/gold/02_create_gold_agg_tables.sql` | Creates the five Gold aggregate tables |
| 3 | `05_databricks/gold/03_load_gold_schema_tables.py` | Loads Silver data into Gold dimensions/facts, SCD2 dimensions, DQ rules/issues and quarantine details |
| 4 | `05_databricks/gold/04_load_gold_agg_tables.py` | Builds the daily sales and DQ aggregates |
| 5 | `05_databricks/gold/05_reconciliation.py` | Read-only validation of row-count conservation across layers and correctness of all five Gold aggregates |

The SQL setup scripts use `CREATE ... IF NOT EXISTS`; they do not drop existing tables. Run the Python loaders as **Databricks Python script tasks**, not by pasting them into notebooks.

## 3. Aggregate tables and report mapping

| Aggregate table | Grain | Executive | Store/Regional | Product | Data Quality |
|---|---|:---:|:---:|:---:|:---:|
| `agg_sales_daily` | Day × Store × Loyalty Tier × Payment Method | Yes | Yes | — | — |
| `agg_sales_product_daily` | Day × Product | — | — | Yes, default product analysis | — |
| `agg_sales_product_store_daily` | Day × Product × Store × Loyalty Tier × Payment Method | Yes, filtered category/subcategory analysis | — | Yes, when location/segment filters apply | — |
| `agg_data_quality_daily` | Issue date × source table/file × rule × severity × status × geography key | — | — | — | Issue trends/charts |
| `agg_data_quality_record_daily` | Evaluation date × source table/file × geography key | — | — | — | Quality percentages/counts |

### Executive

- `agg_sales_daily`: Total Revenue, Total Invoices, Average Invoice Value, Revenue Growth %, revenue/invoice trends, store rollups, Top 10 Cities and geography hierarchy.
- `agg_sales_product_store_daily`: revenue by Category/Subcategory where product/category visuals must respond to the dashboard's applicable product, location, loyalty and payment filters.

### Store/Regional

- `agg_sales_daily`: KPIs, regional trend, Revenue by State map, Region → State → City → Store drill-down, Top/Bottom 10 Stores and Revenue vs Invoice Volume.

### Product

- `agg_sales_product_daily`: Product Revenue, Units Sold, Average Selling Price, Gross Margin %, product trend, Top 10 Products and margin rankings when no location/loyalty segmentation is selected.
- `agg_sales_product_store_daily`: the same product measures with Store, Region, State, Loyalty Tier and Payment Method context. Product Category/Subcategory/Brand are resolved through the Gold dimensions.

### Data Quality

- `agg_data_quality_daily`: issue counts/trends by source table, file, rule, severity and status, plus Pareto.
- `agg_data_quality_record_daily`: total, valid and incorrect record counts used by Data Quality % and Incorrect Records %.
- `fact_data_quality_issue` and `quarantined_*`: individual remediation records, including the source identity and business attributes available in the quarantine payload. This is the only planned record-level grid and does not require sales-fact scans.

## 4. Databricks prerequisites

Before starting:

1. The M7 Silver process must have completed successfully.
2. The workspace must contain `workspace.silver` tables and quarantine tables as defined by `05_databricks/silver/01_silver_tables_creation.sql`.
3. The Job identity must be able to read `workspace.silver` and create/read/write/modify `workspace.gold`.
4. A supported Databricks Runtime or serverless Python environment must provide PySpark and Delta Lake.
5. The Databricks CLI profile `retail-dev` must authenticate successfully if you use the CLI to upload workspace files.
6. Do not run concurrent Gold base-load or aggregate-load jobs. Both are single-writer workflows.

## 5. Upload the files to the workspace

From the repository root, create a stable workspace folder:

```bash
databricks workspace mkdirs /workspace/retail/gold --profile retail-dev
```

Upload the SQL files and Python scripts. CLI flags vary by Databricks CLI version; inspect `databricks workspace import --help` if necessary. For CLI versions supporting these options:

```bash
databricks workspace import /workspace/retail/gold/01_create_gold_schema_tables.sql \
  --file 05_databricks/gold/01_create_gold_schema_tables.sql \
  --format AUTO --language SQL --profile retail-dev

databricks workspace import /workspace/retail/gold/02_create_gold_agg_tables.sql \
  --file 05_databricks/gold/02_create_gold_agg_tables.sql \
  --format AUTO --language SQL --profile retail-dev

databricks workspace import /workspace/retail/gold/03_load_gold_schema_tables.py \
  --file 05_databricks/gold/03_load_gold_schema_tables.py \
  --format SOURCE --language PYTHON --profile retail-dev

databricks workspace import /workspace/retail/gold/04_load_gold_agg_tables.py \
  --file 05_databricks/gold/04_load_gold_agg_tables.py \
  --format SOURCE --language PYTHON --profile retail-dev

databricks workspace import /workspace/retail/gold/05_reconciliation.py \
  --file 05_databricks/gold/05_reconciliation.py \
  --format SOURCE --language PYTHON --profile retail-dev
```

The Markdown document belongs in GitHub; it is not a Job task.

## 6. Create Gold tables

Create a setup task or use a Databricks SQL editor connected to the intended catalog/warehouse:

1. Execute `01_create_gold_schema_tables.sql`.
2. Execute `02_create_gold_agg_tables.sql`.
3. Confirm that the tables exist in `workspace.gold`.

Example checks:

```sql
SHOW TABLES IN workspace.gold;
DESCRIBE TABLE workspace.gold.fact_invoice;
DESCRIBE TABLE workspace.gold.agg_sales_daily;
```

If your workspace does not support a SQL-file task in Jobs, execute the SQL files through the SQL editor. This is a setup step; do not run the DDL from a notebook.

## 7. Configure Job 1 — Gold base model

Create a Databricks Job named:

`M8 - Gold - Load Base Model`

Configure one task:

| Setting | Value |
|---|---|
| Task type | Python script |
| Source | Workspace |
| Path | `/workspace/retail/gold/03_load_gold_schema_tables.py` |
| Compute | Supported job compute/serverless Python option |
| Maximum concurrent runs | `1` |
| Retries | Start with `0` or `1`; increase only after idempotency has been verified |

Permissions required:
- Read `workspace.silver`.
- Read/write/modify `workspace.gold`.
- Use the selected compute.

Run it manually first. Check that:
- `dim_date` covers every Silver invoice date.
- Geography and category hierarchies are populated.
- Customer, store and product SCD2 dimensions have no more than one current row per natural key.
- `fact_invoice` has one row per `invoice_id`.
- `fact_invoice_item` has one row per `(invoice_id, line_item)`.
- Anonymous invoices use `customer_key = -1`; this is valid business behavior.
- DQ issue and quarantine tables are populated if Silver quarantine data exists.

Do not run the aggregation Job until the base Job succeeds and these checks pass.

## 8. Configure Job 2 — Gold aggregates

Create a Databricks Job named:

`M8 - Gold - Build Aggregates`

Configure one task:

| Setting | Value |
|---|---|
| Task type | Python script |
| Source | Workspace |
| Path | `/workspace/retail/gold/04_load_gold_agg_tables.py` |
| Compute | Supported job compute sized for the largest aggregate |
| Maximum concurrent runs | `1` |
| Dependency | Base-model Job must complete successfully |

### Refresh behavior

The current aggregate loader performs a **full rebuild** of the five aggregate tables. This is intentionally simple for initial correctness testing, but it is not yet an incremental refresh implementation. Measure the runtime and output size—especially `agg_sales_product_store_daily`—before scheduling it regularly. If a full rebuild is too expensive, implement partition-scoped recomputation with reliable change detection and tests for updates/deletes.

Run the Job manually and check the row counts:

```sql
SELECT COUNT(*) FROM workspace.gold.agg_sales_daily;
SELECT COUNT(*) FROM workspace.gold.agg_sales_product_daily;
SELECT COUNT(*) FROM workspace.gold.agg_sales_product_store_daily;
SELECT COUNT(*) FROM workspace.gold.agg_data_quality_daily;
SELECT COUNT(*) FROM workspace.gold.agg_data_quality_record_daily;
```

## 9. Recommended orchestration

```text
Gold DDL setup (run once or when schema changes)
                    |
                    v
M8 - Gold - Load Base Model
                    |
                    v
M8 - Gold - Build Aggregates
                    |
                    v
Manual reconciliation: 05_reconciliation.py
                    |
                    v
Power BI query validation
```

Run the base model after a successful Silver load. Run aggregates only after the base model completes successfully. Keep maximum concurrent runs at `1` for both Jobs.

## 10. Reconciliation tests before Power BI

Use small date ranges and selected stores/products first; then run broader checks.

1. `agg_sales_daily.revenue` equals invoice-line revenue aggregated to the same date/store/loyalty/payment grain.
2. `agg_sales_daily.invoice_count` equals distinct invoice headers for that grain. It must not be computed from line counts.
3. `agg_sales_product_daily` and `agg_sales_product_store_daily` reconcile to invoice-line revenue, quantity and gross margin.
4. `gross_margin = revenue - product cost` using the approved cost basis.
5. DQ issue counts reconcile to `fact_data_quality_issue`.
6. DQ valid/incorrect denominators reconcile to the defined Silver clean/quarantine population.
7. No duplicate aggregate rows exist at the declared grain.
8. Unknown dimension members and no-customer invoices are handled according to `09_gold_model_design.md`.

## 11. Reconciliation script — `05_reconciliation.py`

### Purpose and operating mode

`05_databricks/gold/05_reconciliation.py` is a **manual, read-only PySpark validation script**. Run it after the Silver load, Gold base-model load, and Gold aggregate build have completed successfully. It is not a data-loading step and must not be used to repair discrepancies automatically. It reads the configured RAW Volume and tables in `workspace.bronze`, `workspace.silver`, and `workspace.gold`; it does not insert, update, delete, merge, or overwrite records.

Run it inside the Databricks runtime because it requires both an active Spark session and `dbutils.fs.ls`. Upload it to `/workspace/retail/gold/05_reconciliation.py`, then run it as a Python script in a supported Databricks compute environment (for example, a manually triggered Python script task). Do not paste the whole script into a notebook cell. Ensure the execution identity can list/read `/Volumes/workspace/retail/raw` and read the source and Gold tables.

### Layer-level row-count checks

The script checks five source entities: `customers`, `products`, `stores`, `invoices`, and `invoice_items`. For each entity, it reports counts for RAW, Bronze, Silver-valid, Silver-quarantine, Gold-valid, and Gold-quarantine, then evaluates these equations:

| Check | Expected relationship |
|---|---|
| RAW = Bronze | RAW extracted rows equal Bronze rows |
| Bronze = Silver valid + Silver quarantine | Every Bronze row is accounted for in Silver |
| Silver valid = Gold valid | Valid Silver rows are represented in Gold |
| Silver quarantine = Gold quarantine | Quarantined Silver rows are preserved in Gold quarantine detail |
| Gold valid + Gold quarantine = Bronze | End-to-end row conservation |

For Gold SCD2 dimensions, the script counts only current rows (`is_current = true`) and excludes the generic unknown member with key `-1`. Historical SCD2 versions and the unknown member are not additional source entities. Invoice and invoice-item valid counts use `fact_invoice` and `fact_invoice_item`, respectively.

### Aggregate correctness checks

The script independently reconstructs expected aggregates from Gold facts/dimensions and compares them to the persisted aggregate tables. It uses a full outer comparison at each declared grain, with null-safe key equality, to identify missing rows, unexpected rows, measure differences, and duplicate grain groups.

| Aggregate | Grain checked | Measures checked |
|---|---|---|
| `agg_sales_daily` | Date × Store × Loyalty Tier × Payment Method | Invoice count, revenue, units sold, gross margin |
| `agg_sales_product_daily` | Date × Product | Revenue, units sold, gross margin |
| `agg_sales_product_store_daily` | Date × Product × Store × Loyalty Tier × Payment Method | Revenue, units sold, gross margin |
| `agg_data_quality_daily` | Issue date × source table/file × rule × severity × status × geography key | DQ issue measures defined by the aggregate loader |
| `agg_data_quality_record_daily` | Evaluation date × source table/file × geography key | Total, valid, incorrect record counts |

Sales expectations are rebuilt from `fact_invoice` and `fact_invoice_item` using the approved calculation logic: line revenue is quantity × unit price minus the nullable discount (treated as zero when absent), and gross margin is line revenue minus quantity × product cost. Invoice count is based on distinct invoice headers, not invoice-item rows. The product aggregates are compared at their full declared grain, not only by total row count.

The DQ issue aggregate is reconstructed from `fact_data_quality_issue` and `dim_data_quality_rule`. The DQ record aggregate is checked against the current per-file population in Silver valid and quarantine tables. The script also verifies the arithmetic invariant `total_records = valid_records + incorrect_records` for every row in `agg_data_quality_record_daily`.

### How to interpret the result

- `PASS` means the particular count or aggregate comparison matched.
- `FAIL` indicates a count, measure, grain, duplicate, or arithmetic discrepancy. Review the printed comparison details and the relevant loader logic before changing data.
- The script exits with status `1` if any source-layer check or aggregate validation fails; a successful run exits normally.
- A missing RAW file set is reported as a warning and currently contributes a count of zero. Treat that warning as a reason to verify the input path and filenames before interpreting the reconciliation result.

### Important RAW snapshot caveat

The script counts **all matching Parquet files** in `/Volumes/workspace/retail/raw`, across all extraction dates. If that folder contains multiple full snapshots while Bronze represents a current/upserted state, RAW counts can exceed Bronze even when the load is behaving as designed. The script reports the number of extraction batches and warns when multiple dates are present. Do not treat `RAW = Bronze` as a meaningful acceptance criterion until you have confirmed that RAW contains only the intended batch, or otherwise defined how multiple snapshots should be reconciled. Files whose names do not follow `<table>_YYYY_MM_DD_part-NNNNN.parquet` are ignored and reported.

The script validates counts and aggregate outputs; it does not prove row-by-row source identity across every medallion layer. A passing result is a necessary validation gate, not a substitute for key-level, SCD2 temporal, business-rule, or Power BI query-plan testing.

### Recommended execution sequence

1. Complete the M7 Silver load and confirm quarantine processing succeeded.
2. Run `M8 - Gold - Load Base Model`.
3. Run `M8 - Gold - Build Aggregates`.
4. Run `05_reconciliation.py` manually.
5. Investigate and resolve any failures, then rerun the reconciliation script.
6. Proceed to Power BI query/performance validation only after the relevant checks pass or any expected snapshot-related differences are explicitly explained and documented.

## 12. Power BI setup and no-fact-scan acceptance

Use aggregate-backed measures and the shared Gold dimensions:

- Executive: `agg_sales_daily`; `agg_sales_product_store_daily` for filtered category/product-hierarchy visuals.
- Store/Regional: `agg_sales_daily`.
- Product: `agg_sales_product_daily` and `agg_sales_product_store_daily`.
- Data Quality: `agg_data_quality_daily` and `agg_data_quality_record_daily`; DQ issue/quarantine details for the remediation grid.

Use `dim_date`, `dim_geography`, `dim_store`, `dim_product`, `dim_subcategory`, `dim_category`, `dim_customer`, and `dim_data_quality_rule` for slicers, labels and hierarchies as appropriate. Follow the relationships in `09_gold_model_design.md`; do not create a direct geography relationship to the sales facts.

**Creating aggregate tables does not automatically force Power BI to query them.** Configure the semantic model, measures, relationships, and aggregation mappings as required by the chosen Power BI storage mode. Then:

1. Test every documented slicer and drill-down.
2. Use Power BI Performance Analyzer to capture visual queries.
3. Inspect Databricks query history/query plans.
4. Confirm routine Executive, Store/Regional and Product interactions read aggregate tables/dimensions and do not scan `fact_invoice` or `fact_invoice_item`.
5. Confirm the DQ remediation grid queries only DQ issue/quarantine datasets.

Do not declare the performance requirement achieved until those query tests pass.

## 13. Known validation points

Treat the first execution as a controlled validation run, not a blind production launch.

- Validate SCD2 effective-date joins for customer, store and product against historical examples.
- Profile cardinality and runtime for `agg_sales_product_store_daily`.
- The initial DQ record aggregate is a current per-file snapshot; historical quality trends require preserved snapshots or event history with a stable reporting-population definition.
- The initial DQ aggregate uses the unknown geography member unless a reliable source-to-business-dimension mapping is implemented. If the report must filter DQ by geography, store, product or customer, implement and validate those mappings before marking the DQ dashboard complete.
- Check the exact quarantine schema and DQ status semantics in the deployed Silver tables.
- Check the installed CLI's import flags with `--help`.

These checks are part of finishing M8. Do not mark Gold loading complete until the base facts, aggregates, DQ counts, and Power BI query paths have been validated.
