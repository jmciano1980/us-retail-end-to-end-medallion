#!/bin/bash
# Alternative using Databricks CLI v0.18+ - also pipeline-friendly
# pip install databricks-cli
# databricks auth profiles
set -e
LOCAL_PATH=${1:-data/parquet_export}
VOLUME_PATH=${2:-/Volumes/workspace/retail/raw_postgres_export}

echo "Uploading $LOCAL_PATH -> $VOLUME_PATH via CLI"
for f in $LOCAL_PATH/*.parquet; do
  echo "Uploading $(basename $f)..."
  databricks files upload --overwrite "$f" "$VOLUME_PATH/$(basename $f)"
done
echo "Done. Listing:"
databricks fs ls "$VOLUME_PATH"
