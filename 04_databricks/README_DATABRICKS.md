# M4 - Databricks Bronze/Silver/Gold

## Milestone Goal
Build Medallion Architecture from Postgres OLTP (`retail_raw.*`) to Databricks Delta Lake.

- **Raw**: Postgres M3 DONE
- **Bronze**: Exact copy via Parquet -> Volume
- **Silver**: Cleaning + SCD2
- **Gold**: Star schema

### Why Postgres -> Parquet -> Bronze?
Free Edition cannot reach WSL Docker Postgres at 192.168.182.1. Pattern:
```
[WSL Postgres: retail_raw.*] -> [data/parquet_export/*.parquet] -> [Volume: /Volumes/workspace/retail/raw_postgres_export] -> [bronze.*]
```

## Folder Structure - CORRECT ORDER

```
04_databricks/
├── 01_postgres_to_parquet/
│   ├── 01_export_postgres_to_parquet.py  # M4.1 Export
│   └── requirements.txt
├── 02_data_export/                     # M4.1b Upload - MUST BE BEFORE BRONZE
│   ├── 02_upload_parquet_to_databricks_volume.py
│   ├── upload_via_cli.sh
│   └── DATABRICKS_UPLOAD_GUIDE.md
└── 03_bronze_ingestion/
    └── 03_bronze_ingest_from_parquet.py  # M4.2 Bronze
    └── 04_bronze_ddl.sql  # M4.2 Bronze
    └── 05_bronze_loading_process.py  # M4.2 Bronze
    └── 06_bronze_control_querys.py  # M4.2 Bronze
    └── notebook_bronze_ingest_databricks.py  # M4.2 Bronze

```

## M4.1 - Export
Your run: 53 files / 1.93 GB

## M4.1b - Upload to Volume [BEFORE M4.2]
Volume: /Volumes/workspace/retail/raw_postgres_export

```bash
pip uninstall -y databricks-cli databricks
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
hash -r
unset DATABRICKS_TOKEN
databricks auth login --host https://dbc-a6423fd6-0d52.cloud.databricks.com
python 04_databricks/02_data_export/02_upload_parquet_to_databricks_volume.py
```


# M4.2 - Databricks Bronze/Silver/Gold

## Milestone Goal
Build Medallion Architecture from Postgres OLTP (`retail_raw.*`) to Databricks Delta Lake.

- **Raw**: Postgres M3 DONE
- **Bronze**: Exact copy via Parquet -> Volume, as-is, incremental with file tracking
- **Silver**: Cleaning + SCD2
- **Gold**: Star schema

### Why Postgres -> Parquet -> Bronze?
Free Edition cannot reach WSL Docker Postgres at 192.168.182.1. Pattern:

