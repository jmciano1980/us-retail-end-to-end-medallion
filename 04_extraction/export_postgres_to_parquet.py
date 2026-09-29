#!/usr/bin/env python3
"""
M4 - PostgreSQL -> Parquet source extractor.

Purpose
-------
Extract PostgreSQL source tables to Parquet without business transformations.

Source preservation rules
-------------------------
- Same source column names.
- NULL remains NULL.
- No filtering.
- No deduplication.
- No business rules.
- No referential correction.
- No source database changes.

Technical controls
------------------
- PostgreSQL REPEATABLE READ snapshot per table.
- Server-side cursor for bounded-memory extraction.
- Batch processing.
- Multiple Parquet parts for large tables.
- Source row-count vs extracted row-count reconciliation.
- Atomic Parquet part publication.
- Per-table and per-run manifests.
- Extraction-date-based file names for incremental ingestion.

The extractor intentionally does NOT require a primary key.
A primary key is useful for keyset pagination, but extraction itself
must not fail merely because a source table has no PK.

Requirements
------------
pip install pyarrow "psycopg[binary]"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from psycopg import sql


# ---------------------------------------------------------------------------
# Project / configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DESTINATION = PROJECT_ROOT / "data" / "parquet_export"
DEFAULT_SCHEMA = "retail_raw"
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_ROWS_PER_FILE = 500_000
DEFAULT_COMPRESSION = "snappy"


def load_dotenv() -> Path | None:
    """
    Load the repository .env without requiring python-dotenv.

    Shell environment variables have priority over .env values.
    """
    env_path = PROJECT_ROOT / ".env"

    if not env_path.is_file():
        return None

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key or key in os.environ:
            continue

        if len(value) >= 2 and value[0] == value[-1]:
            if value[0] in ("'", '"'):
                value = value[1:-1]

        os.environ[key] = value

    return env_path


load_dotenv()


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract PostgreSQL tables to Parquet without transformations."
    )

    parser.add_argument(
        "--schema",
        default=os.getenv("POSTGRES_SCHEMA", DEFAULT_SCHEMA),
        help=f"Source PostgreSQL schema. Default: {DEFAULT_SCHEMA}",
    )

    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--table",
        action="append",
        dest="tables",
        metavar="TABLE",
        help="Extract one table. Repeat this option for multiple tables.",
    )
    selection.add_argument(
        "--all",
        action="store_true",
        help="Extract all BASE TABLE objects in the selected schema.",
    )

    parser.add_argument(
        "--destination",
        default=os.getenv(
            "PARQUET_DESTINATION",
            str(DEFAULT_DESTINATION),
        ),
        help="Output directory. Default: <project>/data/parquet_export",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(
            os.getenv("PARQUET_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))
        ),
        help=f"PostgreSQL rows fetched per batch. Default: {DEFAULT_BATCH_SIZE:,}",
    )

    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=int(
            os.getenv(
                "PARQUET_ROWS_PER_FILE",
                str(DEFAULT_ROWS_PER_FILE),
            )
        ),
        help=f"Maximum rows per Parquet part. Default: {DEFAULT_ROWS_PER_FILE:,}",
    )

    parser.add_argument(
        "--compression",
        choices=("snappy", "zstd", "gzip", "brotli", "lz4", "none"),
        default=os.getenv(
            "PARQUET_COMPRESSION",
            DEFAULT_COMPRESSION,
        ),
        help="Parquet compression codec.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output for the selected tables.",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

def connect() -> psycopg.Connection:
    required = {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    }

    if not required["POSTGRES_DB"]:
        raise RuntimeError("POSTGRES_DB is not set.")
    if not required["POSTGRES_USER"]:
        raise RuntimeError("POSTGRES_USER is not set.")

    return psycopg.connect(
        host=required["POSTGRES_HOST"],
        port=required["POSTGRES_PORT"],
        dbname=required["POSTGRES_DB"],
        user=required["POSTGRES_USER"],
        password=required["POSTGRES_PASSWORD"],
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
        application_name="us-retail-m4-parquet-extractor",
        autocommit=True,
    )


def quote_identifier(name: str) -> sql.Identifier:
    return sql.Identifier(name)


def get_tables(
    conn: psycopg.Connection,
    schema: str,
    requested_tables: list[str] | None,
) -> list[str]:
    """
    Return BASE TABLE names from the requested schema.

    If explicit tables were requested, validate them against the database.
    """
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """

    with conn.cursor() as cur:
        cur.execute(query, (schema,))
        available = [row[0] for row in cur.fetchall()]

    if requested_tables:
        missing = [
            table
            for table in requested_tables
            if table not in available
        ]

        if missing:
            raise RuntimeError(
                f"Requested table(s) not found in schema "
                f"'{schema}': {missing}"
            )

        # Preserve the order supplied by the user.
        return requested_tables

    return available


def get_row_count(
    conn: psycopg.Connection,
    schema: str,
    table: str,
) -> int:
    query = sql.SQL(
        "SELECT COUNT(*) FROM {}.{}"
    ).format(
        quote_identifier(schema),
        quote_identifier(table),
    )

    with conn.cursor() as cur:
        cur.execute(query)
        return int(cur.fetchone()[0])


# ---------------------------------------------------------------------------
# PostgreSQL -> Arrow type handling
# ---------------------------------------------------------------------------

def arrow_type_for_value(
    value: Any,
    existing_type: pa.DataType | None,
) -> pa.DataType:
    """
    Infer a Parquet-compatible type from actual PostgreSQL Python values.

    This is transport representation only. No business conversion is done.
    """
    if existing_type is not None:
        return existing_type

    if value is None:
        return pa.string()

    if isinstance(value, bool):
        return pa.bool_()

    if isinstance(value, int):
        return pa.int64()

    if isinstance(value, float):
        return pa.float64()

    if isinstance(value, Decimal):
        # Use decimal256 for safe transport of PostgreSQL NUMERIC values.
        precision = max(1, len(value.as_tuple().digits))
        scale = max(0, -value.as_tuple().exponent)
        precision = max(precision, scale + 1)
        return pa.decimal256(min(max(precision, 1), 76), scale)

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return pa.timestamp("us", tz="UTC")
        return pa.timestamp("us")

    if isinstance(value, date):
        return pa.date32()

    if isinstance(value, time):
        return pa.time64("us")

    if isinstance(value, (bytes, bytearray, memoryview)):
        return pa.binary()

    # UUID, JSON, JSONB, arrays and other PostgreSQL-specific values are
    # transported as their textual representation when Arrow has no direct
    # native representation available.
    return pa.string()


def normalize_for_arrow(
    value: Any,
    target_type: pa.DataType,
) -> Any:
    if value is None:
        return None

    if pa.types.is_string(target_type):
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            )

        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).hex()

        return str(value)

    if pa.types.is_binary(target_type):
        return bytes(value)

    return value


def build_arrow_table(
    rows: list[tuple[Any, ...]],
    column_names: list[str],
    column_types: list[pa.DataType | None],
) -> tuple[pa.Table, list[pa.DataType]]:
    """
    Build an Arrow table preserving column order and NULLs.
    """
    if not rows:
        raise ValueError("Cannot build Arrow table from zero rows.")

    final_types: list[pa.DataType] = []

    for index, current_type in enumerate(column_types):
        resolved = current_type

        if resolved is None:
            for row in rows:
                if row[index] is not None:
                    resolved = arrow_type_for_value(
                        row[index],
                        None,
                    )
                    break

        if resolved is None:
            # A column containing only NULL in the entire first batch is
            # represented as nullable string. Values remain NULL.
            resolved = pa.string()

        final_types.append(resolved)

    fields = [
        pa.field(
            name,
            final_types[index],
            nullable=True,
        )
        for index, name in enumerate(column_names)
    ]

    arrays = []

    for index, target_type in enumerate(final_types):
        values = [
            normalize_for_arrow(row[index], target_type)
            for row in rows
        ]

        try:
            arrays.append(
                pa.array(
                    values,
                    type=target_type,
                )
            )
        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError) as exc:
            raise RuntimeError(
                f"Could not represent PostgreSQL column "
                f"'{column_names[index]}' as Parquet type "
                f"'{target_type}': {exc}"
            ) from exc

    return (
        pa.Table.from_arrays(
            arrays,
            schema=pa.schema(fields),
        ),
        final_types,
    )


# ---------------------------------------------------------------------------
# Extraction run identity
# ---------------------------------------------------------------------------

def extraction_date_token(value: date | None = None) -> str:
    """Return the extraction date using the repository file-naming convention."""
    current = value or datetime.now().date()
    # The repository naming convention uses underscores between date parts:
    # YYYY_MM_DD. This keeps the date human-readable and sortable.
    return current.strftime("%Y_%m_%d")


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_table(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    destination: Path,
    batch_size: int,
    rows_per_file: int,
    compression: str,
    overwrite: bool,
    extraction_date: str,
) -> dict[str, Any]:
    # M4 output is intentionally FLAT: every Parquet file for every source
    # table is written directly under <project>/data/parquet_export.
    # The source table name AND extraction date are part of every file name.
    # Example: customers_2026_09_29_part-00001.parquet
    table_destination = destination
    table_destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    existing_parts = sorted(
        table_destination.glob(f"{table}_{extraction_date}_part-*.parquet")
    )

    if existing_parts and not overwrite:
        raise RuntimeError(
            f"Output files already exist for table "
            f"'{schema}.{table}' in {table_destination}. "
            f"Use --overwrite to replace them."
        )

    if overwrite:
        for existing in existing_parts:
            existing.unlink()
        existing_manifest = (
            table_destination / f"{table}_{extraction_date}_extraction_manifest.json"
        )
        if existing_manifest.exists():
            existing_manifest.unlink()

    started_at = datetime.now().astimezone()
    extraction_id = uuid.uuid4().hex

    # autocommit=True allows us to explicitly start the exact transaction
    # we want, before ANY SELECT is executed.
    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

    writer: pq.ParquetWriter | None = None
    temporary_path: Path | None = None
    part_files: list[str] = []
    published_files: list[Path] = []

    try:
        source_count = get_row_count(
            conn,
            schema,
            table,
        )

        query = sql.SQL(
            "SELECT * FROM {}.{}"
        ).format(
            quote_identifier(schema),
            quote_identifier(table),
        )

        # Named/server-side cursor keeps the result set out of client RAM.
        cursor_name = (
            "m4_"
            + uuid.uuid4().hex
        )

        with conn.cursor(name=cursor_name) as cur:
            cur.execute(query)

            description = cur.description

            if description is None:
                raise RuntimeError(
                    f"{schema}.{table}: PostgreSQL returned no columns."
                )

            column_names = [
                column.name
                for column in description
            ]

            column_types: list[pa.DataType | None] = [
                None
                for _ in column_names
            ]

            total_rows = 0
            part_number = 1
            rows_in_current_part = 0

            while True:
                rows = cur.fetchmany(batch_size)

                if not rows:
                    break

                rows = [tuple(row) for row in rows]

                arrow_batch, resolved_types = build_arrow_table(
                    rows,
                    column_names,
                    column_types,
                )

                column_types = resolved_types

                offset = 0

                while offset < arrow_batch.num_rows:
                    capacity = (
                        rows_per_file
                        - rows_in_current_part
                    )

                    take = min(
                        capacity,
                        arrow_batch.num_rows - offset,
                    )

                    chunk = arrow_batch.slice(
                        offset,
                        take,
                    )

                    if writer is None:
                        final_path = (
                            table_destination
                            / f"{table}_{extraction_date}_part-{part_number:05d}.parquet"
                        )

                        temporary_path = final_path.with_suffix(
                            ".parquet.tmp"
                        )

                        writer = pq.ParquetWriter(
                            temporary_path,
                            arrow_batch.schema,
                            compression=(
                                None
                                if compression == "none"
                                else compression
                            ),
                            use_dictionary=True,
                            write_statistics=True,
                        )

                    writer.write_table(chunk)

                    rows_in_current_part += take
                    total_rows += take
                    offset += take

                    if rows_in_current_part >= rows_per_file:
                        assert writer is not None
                        assert temporary_path is not None

                        writer.close()

                        os.replace(
                            temporary_path,
                            final_path,
                        )

                        part_files.append(
                            final_path.name
                        )
                        published_files.append(final_path)

                        writer = None
                        temporary_path = None
                        part_number += 1
                        rows_in_current_part = 0

            if writer is not None:
                assert temporary_path is not None

                final_path = (
                    table_destination
                    / f"{table}_{extraction_date}_part-{part_number:05d}.parquet"
                )

                writer.close()

                os.replace(
                    temporary_path,
                    final_path,
                )

                part_files.append(
                    final_path.name
                )
                published_files.append(final_path)

                writer = None
                temporary_path = None

        if total_rows != source_count:
            raise RuntimeError(
                f"{schema}.{table}: row-count reconciliation failed. "
                f"PostgreSQL={source_count}, Parquet={total_rows}"
            )

        # The cursor and all SELECTs were executed inside the same snapshot.
        # COMMIT only after all output and reconciliation checks succeeded.
        conn.execute("COMMIT")

    except Exception:
        if writer is not None:
            writer.close()

        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()

        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass

        # Never delete the shared destination directory: other tables may
        # already have been extracted successfully in this run. Remove only
        # files published by this table/extraction date.
        for published_file in published_files:
            try:
                published_file.unlink()
            except FileNotFoundError:
                pass

        raise

    ended_at = datetime.now().astimezone()

    manifest = {
        "extractor": "04_extraction/export_postgres_to_parquet.py",
        "extraction_id": extraction_id,
        "extraction_date": extraction_date,
        "source": {
            "type": "postgresql",
            "host": conn.info.host,
            "port": conn.info.port,
            "database": conn.info.dbname,
            "schema": schema,
            "table": table,
        },
        "destination": str(table_destination),
        "output_layout": "flat",
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "source_row_count": source_count,
        "parquet_row_count": total_rows,
        "row_count_reconciled": (
            source_count == total_rows
        ),
        "pagination_method": "server_side_cursor",
        "primary_key_required": False,
        "batch_size": batch_size,
        "rows_per_file": rows_per_file,
        "compression": compression,
        "part_count": len(part_files),
        "part_files": part_files,
        "columns": column_names,
        "transformation_applied": False,
        "business_rules_applied": False,
        "records_filtered": False,
        "records_deduplicated": False,
        "null_values_preserved": True,
        "source_modified": False,
    }

    manifest_path = (
        table_destination
        / f"{table}_{extraction_date}_extraction_manifest.json"
    )

    temporary_manifest = manifest_path.with_suffix(
        ".json.tmp"
    )

    temporary_manifest.write_text(
        json.dumps(
            manifest,
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    os.replace(
        temporary_manifest,
        manifest_path,
    )

    return manifest


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than zero.")

    if args.rows_per_file <= 0:
        raise SystemExit(
            "--rows-per-file must be greater than zero."
        )

    destination = Path(
        args.destination
    ).expanduser().resolve()

    # One extraction date is assigned to the complete run so every table
    # produced by that run belongs to the same dated ingestion batch.
    extraction_date = extraction_date_token()

    # --all and no selection both mean all tables.
    requested_tables = (
        args.tables
        if args.tables
        else None
    )

    print("M4 - PostgreSQL -> Parquet source extraction")
    print(f"Destination:           {destination}")
    print(f"Schema:                {args.schema}")
    print(f"Batch size:            {args.batch_size:,}")
    print(f"Rows per Parquet part: {args.rows_per_file:,}")
    print(f"Compression:           {args.compression}")
    print(f"Extraction date:       {extraction_date}")
    print("Output layout:         flat (all tables in one directory)")
    print(
        "Selection:             "
        + (
            "all tables"
            if requested_tables is None
            else ", ".join(requested_tables)
        )
    )

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifests: list[dict[str, Any]] = []

    with connect() as conn:
        tables = get_tables(
            conn,
            args.schema,
            requested_tables,
        )

        if not tables:
            raise RuntimeError(
                f"No BASE TABLE objects found in "
                f"schema '{args.schema}'."
            )

        print(
            "Tables:                "
            + ", ".join(tables)
        )

        for table in tables:
            print(
                f"\nExtracting "
                f"{args.schema}.{table} ..."
            )

            manifest = extract_table(
                conn=conn,
                schema=args.schema,
                table=table,
                destination=destination,
                batch_size=args.batch_size,
                rows_per_file=args.rows_per_file,
                compression=args.compression,
                overwrite=args.overwrite,
                extraction_date=extraction_date,
            )

            manifests.append(manifest)

            print(
                "  OK"
                f"  rows={manifest['parquet_row_count']:,}"
                f"  parts={manifest['part_count']}"
                f"  reconciled={manifest['row_count_reconciled']}"
            )

    run_manifest = {
        "extractor": "04_extraction/export_postgres_to_parquet.py",
        "run_id": uuid.uuid4().hex,
        "extraction_date": extraction_date,
        "schema": args.schema,
        "destination": str(destination),
        "output_layout": "flat",
        "tables": [
            manifest["source"]["table"]
            for manifest in manifests
        ],
        "source_row_count_total": sum(
            manifest["source_row_count"]
            for manifest in manifests
        ),
        "parquet_row_count_total": sum(
            manifest["parquet_row_count"]
            for manifest in manifests
        ),
        "all_tables_reconciled": all(
            manifest["row_count_reconciled"]
            for manifest in manifests
        ),
        "completed_at": datetime.now().astimezone().isoformat(),
    }

    run_manifest_path = (
        destination
        / f"_extraction_run_manifest_{extraction_date}.json"
    )

    run_manifest_path.write_text(
        json.dumps(
            run_manifest,
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    print("\nExtraction completed.")
    print(
        f"Source rows total:    "
        f"{run_manifest['source_row_count_total']:,}"
    )
    print(
        f"Parquet rows total:   "
        f"{run_manifest['parquet_row_count_total']:,}"
    )
    print(
        f"Reconciled:           "
        f"{run_manifest['all_tables_reconciled']}"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print(
            "\nExtraction interrupted.",
            file=sys.stderr,
        )
        raise SystemExit(130)
    except Exception as exc:
        print(
            f"\nERROR: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1)
