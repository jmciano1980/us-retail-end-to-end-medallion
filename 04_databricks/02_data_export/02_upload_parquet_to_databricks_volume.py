"""
M4.1 -> M4.2 - Upload local Parquet export to Databricks Volume (pipeline version)
No manual Drag&Drop - uses Databricks SDK (same as CI/CD)

Prereqs:
  pip install databricks-sdk

Auth:
  Env vars: DATABRICKS_HOST=https://dbc-xxxx.cloud.databricks.com + DATABRICKS_TOKEN=dapi...

Usage (from project root):
  python 04_databricks/02_data_export/02_upload_parquet_to_databricks_volume.py

Or with custom paths:
  python 04_databricks/02_data_export/02_upload_parquet_to_databricks_volume.py --local-path data/parquet_export --volume-path /Volumes/workspace/retail/raw_postgres_export
"""
"""
M4.1 -> M4.2 - Fixed version: volume enum + SSL retry
"""
import argparse
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
import requests.exceptions

def find_project_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "data" / "parquet_export").exists() or (p / ".git").exists():
            return p
    return start

def upload_with_retry(w, local_file: Path, remote_file: str, max_retries=5):
    for attempt in range(1, max_retries+1):
        try:
            with open(local_file, "rb") as fp:
                w.files.upload(file_path=remote_file, contents=fp, overwrite=True)
            return True
        except (requests.exceptions.SSLError, TimeoutError) as e:
            print(f"!! SSL/Timeout attempt {attempt}/{max_retries}: {e}")
            if attempt == max_retries:
                raise
            sleep = 5 * attempt # 5s, 10s, 15s, 20s...
            print(f" -> retry in {sleep}s...")
            time.sleep(sleep)
        except Exception as e:
            print(f"!! Error: {e}")
            raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-path", default="data/parquet_export")
    parser.add_argument("--volume-path", default="/Volumes/workspace/retail/raw_postgres_export")
    args = parser.parse_args()

    this_file = Path(__file__).resolve()
    project_root = find_project_root(this_file.parent)

    local_root = (project_root / args.local_path).resolve() if not Path(args.local_path).is_absolute() else Path(args.local_path).resolve()
    volume_root = args.volume_path.rstrip("/")

    if not local_root.exists():
        raise FileNotFoundError(f"Not found: {local_root}")

    w = WorkspaceClient()
    print(f"Workspace: {w.config.host}")
    print(f"Local: {local_root} -> {volume_root}")

    # Fix volume creation - use enum
    try:
        parts = volume_root.replace("/Volumes/", "").split("/")
        catalog, schema, volume = parts[0], parts[1], parts[2]
        try:
            w.volumes.read(f"{catalog}.{schema}.{volume}")
            print(f"Volume exists: {catalog}.{schema}.{volume}")
        except Exception:
            print(f"Creating volume...")
            w.volumes.create(catalog_name=catalog, schema_name=schema, name=volume, volume_type=VolumeType.MANAGED)
    except Exception as e:
        print(f"Volume check skipped (will still upload if volume exists): {e}")

    files = sorted(local_root.glob("*.parquet"))
    print(f"Found {len(files)} files, {sum(f.stat().st_size for f in files)/1024**3:.2f} GB")

    for i, f in enumerate(files, 1):
        remote = f"{volume_root}/{f.name}"
        print(f"[{i}/{len(files)}] {f.name} ({f.stat().st_size/1024**2:.1f} MB)")
        upload_with_retry(w, f, remote)
        print(f" -> OK")

    print(f"\n✅ DONE")

if __name__ == "__main__":
    main()