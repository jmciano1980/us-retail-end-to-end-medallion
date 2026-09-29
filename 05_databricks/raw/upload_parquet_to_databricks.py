#!/usr/bin/env python3
"""
Upload PostgreSQL Parquet extraction files to Databricks Unity Catalog RAW.

Local source:
    data/parquet_export/

Default Databricks destination:
    /Volumes/workspace/retail/raw

The destination can be overridden with:
    --destination /Volumes/<catalog>/<schema>/<volume>/<optional-subdir>

Authentication is intentionally delegated to the Databricks SDK default
authentication chain. This keeps credentials out of source code.

Typical usage:
    python 05_databricks/raw/upload_parquet_to_databricks.py

Upload a specific extraction date:
    python 05_databricks/raw/upload_parquet_to_databricks.py \
        --extraction-date 2026_09_29

Upload exactly one file:
    python 05_databricks/raw/upload_parquet_to_databricks.py \
        --file customers_2026_09_29_part-00001.parquet

Dry run:
    python 05_databricks/raw/upload_parquet_to_databricks.py --dry-run

Include technical JSON manifests under RAW/_metadata:
    python 05_databricks/raw/upload_parquet_to_databricks.py \
        --include-manifests

The script is designed for append-only incremental file ingestion:
Parquet files are identified by their source filename and are not
silently overwritten unless --overwrite is explicitly supplied.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    print(
        "ERROR: databricks-sdk is not installed. "
        "Install it with: pip install databricks-sdk",
        file=sys.stderr,
    )
    raise SystemExit(1)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "parquet_export"
DEFAULT_DESTINATION = "/Volumes/workspace/retail/raw"
DEFAULT_PATTERN = "*.parquet"


def load_dotenv_if_available() -> None:
    """Load project .env when python-dotenv is available.

    Existing environment variables are never overwritten.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload extracted Parquet files to Databricks RAW."
    )

    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help=f"Local Parquet directory (default: {DEFAULT_SOURCE})",
    )

    parser.add_argument(
        "--destination",
        default=os.getenv("DATABRICKS_RAW_PATH", DEFAULT_DESTINATION),
        help=(
            "Unity Catalog Volume path, e.g. "
            "/Volumes/workspace/retail/raw "
            "(default: /Volumes/workspace/retail/raw)"
        ),
    )

    parser.add_argument(
        "--extraction-date",
        help=(
            "Upload only files for one extraction date, using YYYY_MM_DD. "
            "Example: 2026_09_29"
        ),
    )

    parser.add_argument(
        "--file",
        help=(
            "Upload exactly one local file from --source. "
            "The value may be a filename or a path relative to --source. "
            "This option is mutually exclusive with --extraction-date."
        ),
    )

    parser.add_argument(
        "--include-manifests",
        action="store_true",
        help=(
            "Also upload JSON extraction manifests to "
            "<destination>/_metadata/."
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly overwrite a destination file if it already exists.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show files that would be uploaded without uploading them.",
    )

    return parser.parse_args()


def validate_extraction_date(value: str | None) -> str | None:
    if value is None:
        return None

    try:
        datetime.strptime(value, "%Y_%m_%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid --extraction-date '{value}'. "
            "Expected YYYY_MM_DD, for example 2026_09_29."
        ) from exc

    return value


def iter_files(
    source: Path,
    extraction_date: str | None,
    include_manifests: bool,
    file_name: str | None,
) -> tuple[list[Path], list[Path]]:
    if not source.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source}")

    if not source.is_dir():
        raise NotADirectoryError(f"Source path is not a directory: {source}")

    if file_name:
        candidate = Path(file_name)
        parquet_path = candidate if candidate.is_absolute() else source / candidate

        if not parquet_path.exists():
            raise FileNotFoundError(
                f"Requested file does not exist: {parquet_path}"
            )

        if parquet_path.suffix.lower() != ".parquet":
            raise ValueError(
                f"--file must point to a .parquet file: {parquet_path}"
            )

        if extraction_date and f"_{extraction_date}_" not in parquet_path.name:
            raise ValueError(
                f"--file '{parquet_path.name}' does not match "
                f"--extraction-date {extraction_date}."
            )

        parquet_files = [parquet_path]
    else:
        parquet_files = sorted(source.glob(DEFAULT_PATTERN))

        if extraction_date:
            parquet_files = [
                path
                for path in parquet_files
                if f"_{extraction_date}_" in path.name
            ]

    manifests: list[Path] = []

    if include_manifests:
        manifests = sorted(source.glob("*.json"))

        if extraction_date:
            manifests = [
                path
                for path in manifests
                if extraction_date in path.name
            ]

    return parquet_files, manifests


def destination_file_exists(
    client: WorkspaceClient,
    path: str,
) -> bool:
    """Return True when a file already exists in the destination."""
    try:
        client.files.get_metadata(path)
        return True
    except Exception as exc:
        message = str(exc).lower()

        # The SDK currently exposes API errors with status information in
        # different shapes depending on version. Treat explicit not-found
        # responses as absence; re-raise other failures.
        if "404" in message or "not found" in message:
            return False

        raise


def ensure_directory(client: WorkspaceClient, path: str) -> None:
    client.files.create_directory(path)


def upload_file(
    client: WorkspaceClient,
    local_file: Path,
    destination_file: str,
    overwrite: bool,
) -> None:
    parent = destination_file.rsplit("/", 1)[0]

    try:
        ensure_directory(client, parent)
    except Exception as exc:
        message = str(exc).lower()
        if "already exists" not in message and "409" not in message:
            raise

    if not overwrite and destination_file_exists(client, destination_file):
        raise FileExistsError(
            f"Destination file already exists: {destination_file}. "
            "Use --overwrite only when an intentional replacement is required."
        )

    client.files.upload_from(
        destination_file,
        str(local_file),
        overwrite=overwrite,
    )


def print_configuration(
    source: Path,
    destination: str,
    extraction_date: str | None,
    args_file: str | None,
    include_manifests: bool,
    overwrite: bool,
) -> None:
    print("Databricks RAW upload")
    print("=" * 72)
    print(f"Source:             {source}")
    print(f"Destination:        {destination}")
    print(f"Extraction date:    {extraction_date or 'all available dates'}")
    print(f"Selected file:      {args_file or 'all matching files'}")
    print(f"Include manifests:  {include_manifests}")
    print(f"Overwrite:          {overwrite}")
    print()


def main() -> int:
    load_dotenv_if_available()
    args = parse_args()

    if args.file and args.include_manifests:
        print(
            "ERROR: --file cannot be combined with --include-manifests.",
            file=sys.stderr,
        )
        return 2

    try:
        extraction_date = validate_extraction_date(args.extraction_date)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        parquet_files, manifests = iter_files(
            args.source,
            extraction_date,
            args.include_manifests,
            args.file,
        )
    except (FileNotFoundError, NotADirectoryError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if not parquet_files and not manifests:
        print("No files matched the requested extraction.")
        return 0

    print_configuration(
        args.source,
        args.destination,
        extraction_date,
        args.file,
        args.include_manifests,
        args.overwrite,
    )

    print(f"Parquet files:      {len(parquet_files)}")
    print(f"Manifest files:     {len(manifests)}")
    print()

    if args.dry_run:
        print("DRY RUN — no files will be uploaded.")
        for path in parquet_files:
            print(f"  PARQUET  {path.name}")
        for path in manifests:
            print(f"  METADATA {path.name} -> _metadata/{path.name}")
        return 0

    try:
        client = WorkspaceClient()
    except Exception as exc:
        print(
            "ERROR: Could not initialize Databricks authentication.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 1

    uploaded = 0

    try:
        # Business/source files remain flat in RAW. This preserves the same
        # filename identity created by the PostgreSQL extraction step.
        for local_file in parquet_files:
            destination_file = (
                f"{args.destination.rstrip('/')}/{local_file.name}"
            )

            print(f"Uploading: {local_file.name}")
            upload_file(
                client=client,
                local_file=local_file,
                destination_file=destination_file,
                overwrite=args.overwrite,
            )
            uploaded += 1

        # Technical manifests are kept separate from the Parquet landing
        # area so downstream Parquet ingestion does not mistake JSON metadata
        # for source data.
        metadata_destination = (
            f"{args.destination.rstrip('/')}/_metadata"
        )

        for local_file in manifests:
            destination_file = (
                f"{metadata_destination}/{local_file.name}"
            )

            print(f"Uploading metadata: {local_file.name}")
            upload_file(
                client=client,
                local_file=local_file,
                destination_file=destination_file,
                overwrite=args.overwrite,
            )
            uploaded += 1

    except FileExistsError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        print(
            "ERROR: Databricks upload failed.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 1

    finished_at = datetime.now(timezone.utc).isoformat()

    print()
    print("Upload completed successfully.")
    print(f"Files uploaded: {uploaded}")
    print(f"Finished UTC:   {finished_at}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
