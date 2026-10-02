from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


RAW_PATH = "/Volumes/workspace/retail/raw"
CATALOG = "workspace"
SCHEMA = "bronze"
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}._bronze_file_ingestion_log"

TABLE_CONFIG = {
    "stores": {"pattern": r"^stores_\d{4}_\d{2}_\d{2}_part-\d{5}\.parquet$"},
    "products": {"pattern": r"^products_\d{4}_\d{2}_\d{2}_part-\d{5}\.parquet$"},
    "customers": {"pattern": r"^customers_\d{4}_\d{2}_\d{2}_part-\d{5}\.parquet$"},
    "invoices": {"pattern": r"^invoices_\d{4}_\d{2}_\d{2}_part-\d{5}\.parquet$"},
    "invoice_items": {"pattern": r"^invoice_items_\d{4}_\d{2}_\d{2}_part-\d{5}\.parquet$"},
}

METADATA_COLUMNS = {
    "_insert_datetime_utc",
    "_update_datetime_utc",
    "_source_file_name",
    "_source_row_id",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Incrementally load every RAW Parquet row into Bronze Delta tables. "
            "Duplicate business keys are preserved."
        )
    )
    parser.add_argument("--raw-path", default=RAW_PATH)
    parser.add_argument(
        "--table",
        action="append",
        choices=sorted(TABLE_CONFIG),
        help="Load only selected table(s). Repeat for multiple tables. Default: all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover files without changing Bronze.",
    )
    return parser.parse_args()


def ensure_control_table(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
            source_file_path STRING NOT NULL,
            source_file_name STRING NOT NULL,
            source_table STRING NOT NULL,
            processed_at_utc TIMESTAMP NOT NULL,
            status STRING NOT NULL
        ) USING DELTA
        """
    )


def processed_file_paths(spark: SparkSession, table_name: str) -> set[str]:
    rows = (
        spark.table(CONTROL_TABLE)
        .where((F.col("source_table") == table_name) & (F.col("status") == "SUCCESS"))
        .select("source_file_path")
        .collect()
    )
    return {r["source_file_path"] for r in rows}


def list_new_files(
    spark: SparkSession,
    raw_path: str,
    table_name: str,
    processed_files: set[str],
) -> list[str]:
    pattern = re.compile(TABLE_CONFIG[table_name]["pattern"])
    rows = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(raw_path)
        .select("path")
        .collect()
    )

    candidates = []
    for row in rows:
        path = row["path"]
        file_name = path.rsplit("/", 1)[-1]
        if pattern.match(file_name) and path not in processed_files:
            candidates.append(path)

    return sorted(candidates)


def validate_source_schema(df: DataFrame, table_name: str) -> None:
    if not df.columns:
        raise RuntimeError(f"{table_name}: source Parquet has no columns.")

    # The source schema is authoritative. The exact source/target column comparison
    # is performed after technical metadata is added in merge_file().


def add_metadata(df: DataFrame, source_file_path: str) -> DataFrame:
    """Add technical lineage fields without changing any source business value.

    _source_row_id is a technical identity for one physical row in one source file.
    It is NOT derived from business keys, so duplicate/NULL/invalid business keys are retained.
    """
    source_file_name = source_file_path.rsplit("/", 1)[-1]
    row_id = F.sha2(
        F.concat_ws(
            "||",
            F.lit(source_file_path),
            F.monotonically_increasing_id().cast("string"),
        ),
        256,
    )
    now = F.current_timestamp()
    return (
        df.withColumn("_insert_datetime_utc", now)
        .withColumn("_update_datetime_utc", now)
        .withColumn("_source_file_name", F.lit(source_file_name))
        .withColumn("_source_row_id", row_id)
    )


def merge_file(spark: SparkSession, table_name: str, source_path: str) -> tuple[int, int]:
    target_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    source_df = spark.read.parquet(source_path)

    validate_source_schema(source_df, table_name)
    source_df = add_metadata(source_df, source_path)

    target_columns = spark.table(target_name).columns
    source_columns = source_df.columns
    if source_columns != target_columns:
        raise RuntimeError(
            f"{table_name}: source/target columns differ after adding technical metadata.\n"
            f"Source: {source_columns}\nTarget: {target_columns}"
        )

    # IMPORTANT: MERGE uses the technical physical-row identity, NOT the business key.
    # Therefore duplicate business keys are never treated as duplicates by Bronze.
    condition = "t.`_source_row_id` = s.`_source_row_id`"

    target = DeltaTable.forName(spark, target_name)
    matched_update = {
        c: f"s.`{c}`"
        for c in target_columns
        if c not in {"_insert_datetime_utc"}
    }
    matched_update["_update_datetime_utc"] = "current_timestamp()"

    inserted = {c: f"s.`{c}`" for c in target_columns}

    before = spark.table(target_name).count()

    (
        target.alias("t")
        .merge(source_df.alias("s"), condition)
        .whenMatchedUpdate(set=matched_update)
        .whenNotMatchedInsert(values=inserted)
        .execute()
    )

    after = spark.table(target_name).count()
    return before, after


def mark_success(spark: SparkSession, table_name: str, source_path: str) -> None:
    source_name = source_path.rsplit("/", 1)[-1]
    spark.createDataFrame(
        [
            (
                source_path,
                source_name,
                table_name,
                datetime.now(timezone.utc),
                "SUCCESS",
            )
        ],
        "source_file_path string, source_file_name string, source_table string, processed_at_utc timestamp, status string",
    ).write.mode("append").saveAsTable(CONTROL_TABLE)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("M6-Bronze-Incremental-Load").getOrCreate()

    try:
        ensure_control_table(spark)
        tables = args.table or list(TABLE_CONFIG)
        total_files = 0
        total_rows = 0

        for table_name in tables:
            processed = processed_file_paths(spark, table_name)
            files = list_new_files(spark, args.raw_path, table_name, processed)

            print(f"\n=== {table_name} ===")
            print(f"Previously successful files: {len(processed)}")
            print(f"New files discovered: {len(files)}")

            for source_path in files:
                print(f"  -> {source_path}")
                total_files += 1

                if args.dry_run:
                    continue

                source_row_count = spark.read.parquet(source_path).count()
                before, after = merge_file(spark, table_name, source_path)
                mark_success(spark, table_name, source_path)
                total_rows += source_row_count
                print(f"     Source rows loaded: {source_row_count:,}")
                print(f"     Bronze row count: {before:,} -> {after:,}")

        print(f"\nCompleted. Files processed: {total_files:,}; source rows loaded: {total_rows:,}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
