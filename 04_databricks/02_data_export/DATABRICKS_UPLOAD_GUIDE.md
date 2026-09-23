# 03_data_export - How to Load Parquet Files into Databricks (Free Edition)

Order: BEFORE 03_bronze_ingestion
Data: 53 files / 1.93 GB / Volume /Volumes/workspace/retail/raw_postgres_export

### 1. The 3 Errors You Hit
1. Error: No such command 'auth' - Old CLI 0.17, need v0.2xx
2. Unauthenticated: auth_type=pat, host=dbc-a6423fd6-0d52.cloud.databricks.com - Free Edition blocks PAT, need OAuth
3. SSLError: EOF occurred - WSL bug

### 2. Install New CLI
pip uninstall -y databricks-cli databricks
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
hash -r
which databricks
databricks --version

Fix bash: .../.venv/bin/databricks: No such file with hash -r
Fix Target directory not writable with | sudo sh

### 3. OAuth Login
unset DATABRICKS_TOKEN
unset DATABRICKS_HOST
databricks auth login --host https://dbc-a6423fd6-0d52.cloud.databricks.com
databricks fs ls /Volumes/workspace/retail/raw_postgres_export/

### 4. Upload
Method A - SDK:
python 04_databricks/02_data_export/02_upload_parquet_to_databricks_volume.py

Method B - CLI:
databricks fs cp ./data/parquet_export/* /Volumes/workspace/retail/raw_postgres_export/ --overwrite --recursive

Time: 1.93GB @20Mbps = 12-15min

### 5. Troubleshooting
- Volume check skipped: 'str' has no attribute 'value' -> Use VolumeType.MANAGED enum
- bytes has no seekable -> Use BytesIO
- SSL EOF -> sudo apt install ca-certificates && sudo update-ca-certificates + unset http_proxy https_proxy or run from PowerShell

### 6. Validate
LIST '/Volumes/workspace/retail/raw_postgres_export/';
-- 53 files

Next: 03_bronze_ingestion
