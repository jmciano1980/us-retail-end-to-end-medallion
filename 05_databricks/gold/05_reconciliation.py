#!/usr/bin/env python3
"""Manual, read-only row-count reconciliation across RAW, Bronze, Silver, Gold.

Run manually in the Databricks runtime after Gold base loading. This is not a
Databricks Job and it does not modify any data.

RAW counts include every matching Parquet file in the RAW Volume. Multiple
full-snapshot extraction dates can make RAW exceed Bronze's current/upserted
state. Gold dimension counts use current SCD2 rows and exclude the generic -1
member; historical SCD2 versions are not counted as current business entities.
"""
import re
import sys
from pyspark.sql import SparkSession, functions as F

BRONZE = "workspace.bronze"
SILVER = "workspace.silver"
GOLD = "workspace.gold"
RAW_PATH = "/Volumes/workspace/retail/raw"
TABLES = ("customers", "products", "stores", "invoices", "invoice_items")
RAW_FILENAME = re.compile(
    r"^(customers|products|stores|invoices|invoice_items)_"
    r"\d{4}_\d{2}_\d{2}_part-\d+\.parquet$"
)


def count_table(spark, name, condition=None):
    try:
        df = spark.table(name)
        if condition is not None:
            df = df.filter(condition)
        return df.count()
    except Exception as exc:
        raise RuntimeError(f"Cannot read required table {name}: {exc}") from exc


def list_raw_files():
    try:
        entries = dbutils.fs.ls(RAW_PATH)
    except NameError as exc:
        raise RuntimeError(
            "dbutils.fs.ls is unavailable. Run this script in the Databricks runtime."
        ) from exc

    found, ignored = [], []
    for entry in entries:
        if entry.isDir():
            continue
        if not entry.name.lower().endswith(".parquet"):
            continue
        match = RAW_FILENAME.match(entry.name)
        if match:
            found.append((match.group(1), entry.path))
        else:
            ignored.append(entry.name)
    if ignored:
        print("WARNING: Parquet files ignored because their names do not match "
              "the expected table_date_part convention:")
        for name in sorted(ignored):
            print(f"  {name}")
    return found


def count_raw(spark, raw_files, table):
    paths = [path for name, path in raw_files if name == table]
    if not paths:
        print(f"WARNING: no matching RAW files for {table}")
        return 0
    return spark.read.parquet(*paths).count()


def reconcile_entity(spark, raw_files, table):
    raw = count_raw(spark, raw_files, table)
    bronze = count_table(spark, f"{BRONZE}.{table}")
    silver_valid = count_table(spark, f"{SILVER}.{table}")
    silver_quarantine = count_table(spark, f"{SILVER}.quarantined_{table}")

    if table == "customers":
        gold_valid = count_table(
            spark, f"{GOLD}.dim_customer",
            (F.col("is_current") == True) & (F.col("customer_key") != -1))
        gold_quarantine = count_table(spark, f"{GOLD}.quarantined_customers")
    elif table == "stores":
        gold_valid = count_table(
            spark, f"{GOLD}.dim_store",
            (F.col("is_current") == True) & (F.col("store_key") != -1))
        gold_quarantine = count_table(spark, f"{GOLD}.quarantined_stores")
    elif table == "products":
        gold_valid = count_table(
            spark, f"{GOLD}.dim_product",
            (F.col("is_current") == True) & (F.col("product_key") != -1))
        gold_quarantine = count_table(spark, f"{GOLD}.quarantined_products")
    elif table == "invoices":
        gold_valid = count_table(spark, f"{GOLD}.fact_invoice")
        gold_quarantine = count_table(spark, f"{GOLD}.quarantined_invoices")
    else:
        gold_valid = count_table(spark, f"{GOLD}.fact_invoice_item")
        gold_quarantine = count_table(spark, f"{GOLD}.quarantined_invoice_items")

    return {
        "table": table, "raw": raw, "bronze": bronze,
        "silver_valid": silver_valid, "silver_quarantine": silver_quarantine,
        "gold_valid": gold_valid, "gold_quarantine": gold_quarantine,
    }



# ---------- Gold aggregate reconciliation ----------
def compare_grouped(expected, actual, keys, measures, label):
    """Compare an expected aggregation with its Gold table at the declared grain.

    Full outer join plus null-safe key equality detects missing, extra, and
    measure-mismatched aggregate rows. Returns (mismatch_count, duplicate_count).
    """
    expected = expected.select(*keys, *measures).withColumn("_expected_present", F.lit(1)).alias("e")
    actual = actual.select(*keys, *measures).withColumn("_actual_present", F.lit(1)).alias("a")
    join_condition = None
    for key in keys:
        part = F.col(f"e.{key}").eqNullSafe(F.col(f"a.{key}"))
        join_condition = part if join_condition is None else (join_condition & part)

    joined = expected.join(actual, join_condition, "full")
    # Presence markers distinguish a genuinely NULL key value (which may be
    # valid at this grain) from a row missing on one side of the full join.
    mismatch = (F.col("e._expected_present").isNull() |
                F.col("a._actual_present").isNull())
    for measure in measures:
        # Compare decimal and integer measures exactly; use a tiny tolerance only
        # for floating point types, if a future schema introduces them.
        mismatch = mismatch | ~F.col(f"e.{measure}").eqNullSafe(F.col(f"a.{measure}"))
    mismatch_count = joined.filter(mismatch).count()

    actual_df = actual.select(*keys)
    duplicate_count = (actual_df.groupBy(*keys).count()
                       .filter(F.col("count") > 1).count())
    print(f"{label}: {'PASS' if mismatch_count == 0 else 'FAIL'} — "
          f"{mismatch_count:,} missing/extra/mismatched grain rows; "
          f"{duplicate_count:,} duplicate grain groups")
    return mismatch_count, duplicate_count


def build_expected_sales(spark):
    """Recalculate the three sales aggregates from Gold facts, not from aggregates."""
    invoices = spark.table(f"{GOLD}.fact_invoice")
    items = spark.table(f"{GOLD}.fact_invoice_item")
    dates = spark.table(f"{GOLD}.dim_date").select("date_key", "full_date")
    customers = spark.table(f"{GOLD}.dim_customer").select("customer_key", "loyalty_tier")
    products = spark.table(f"{GOLD}.dim_product").select("product_key", "cost")

    invoice_context = (invoices.join(dates, "date_key", "inner")
        .join(customers, "customer_key", "left")
        .select("invoice_key", "invoice_id", "date_key", "store_key", "payment_method",
                F.coalesce(F.col("loyalty_tier"), F.lit("Unknown")).alias("loyalty_tier")))

    line = (items.join(invoice_context, "invoice_key", "inner")
        .join(products, "product_key", "left")
        .withColumn("line_revenue", F.col("quantity") * F.col("unit_price") -
                    F.coalesce(F.col("discount"), F.lit(0)))
        .withColumn("line_cost", F.col("quantity") * F.coalesce(F.col("cost"), F.lit(0)))
        .withColumn("line_margin", F.col("line_revenue") - F.col("line_cost")))

    daily_keys = ["date_key", "store_key", "loyalty_tier", "payment_method"]
    daily_revenue = line.groupBy(*daily_keys).agg(
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    daily_invoices = invoice_context.groupBy(*daily_keys).agg(
        F.countDistinct("invoice_id").cast("long").alias("invoice_count"))
    daily = (daily_revenue.join(daily_invoices, daily_keys, "full")
             .fillna({"invoice_count": 0, "units_sold": 0, "revenue": 0, "gross_margin": 0})
             .select(*daily_keys, "invoice_count", "revenue", "units_sold", "gross_margin"))

    product_daily = line.groupBy("date_key", "product_key").agg(
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))

    product_store_keys = ["date_key", "product_key", "store_key", "loyalty_tier", "payment_method"]
    product_store_daily = line.groupBy(*product_store_keys).agg(
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    return daily, product_daily, product_store_daily


def reconcile_sales_aggregates(spark):
    print("\\nGOLD SALES AGGREGATE RECONCILIATION")
    expected_daily, expected_product, expected_product_store = build_expected_sales(spark)
    results = []
    results.append(compare_grouped(
        expected_daily, spark.table(f"{GOLD}.agg_sales_daily"),
        ["date_key", "store_key", "loyalty_tier", "payment_method"],
        ["invoice_count", "revenue", "units_sold", "gross_margin"],
        "agg_sales_daily"))
    results.append(compare_grouped(
        expected_product, spark.table(f"{GOLD}.agg_sales_product_daily"),
        ["date_key", "product_key"], ["units_sold", "revenue", "gross_margin"],
        "agg_sales_product_daily"))
    results.append(compare_grouped(
        expected_product_store, spark.table(f"{GOLD}.agg_sales_product_store_daily"),
        ["date_key", "product_key", "store_key", "loyalty_tier", "payment_method"],
        ["units_sold", "revenue", "gross_margin"],
        "agg_sales_product_store_daily"))
    return results


def reconcile_dq_aggregates(spark):
    print("\\nGOLD DATA-QUALITY AGGREGATE RECONCILIATION")
    issues = spark.table(f"{GOLD}.fact_data_quality_issue")
    rules = spark.table(f"{GOLD}.dim_data_quality_rule").select("dq_rule_key", "severity")
    dates = spark.table(f"{GOLD}.dim_date").select("date_key", "full_date")
    expected_issues = (issues.join(rules, "dq_rule_key", "left")
        .withColumn("issue_date", F.to_date("_quarantine_datetime_utc"))
        .withColumn("dq_status", F.coalesce(F.col("_dq_status"), F.lit("Unknown")))
        .join(dates, F.col("issue_date") == F.col("full_date"), "left")
        .withColumn("geography_key", F.lit(-1).cast("long")))
    dq_keys = ["dq_date_key", "source_table", "source_file_name", "dq_rule_key",
               "severity", "dq_status", "geography_key"]
    expected_dq = expected_issues.groupBy(
        F.col("date_key").alias("dq_date_key"), "source_table", "source_file_name",
        "dq_rule_key", "severity", "dq_status", "geography_key").agg(
        F.count("*").cast("long").alias("dq_issue_count"),
        F.countDistinct(F.struct("source_table", "source_row_id")).cast("long")
            .alias("affected_record_count"))
    dq_result = compare_grouped(
        expected_dq, spark.table(f"{GOLD}.agg_data_quality_daily"), dq_keys,
        ["dq_issue_count", "affected_record_count"], "agg_data_quality_daily")

    # The record aggregate is a current per-file snapshot, not historical daily
    # history. Rebuild its expected current snapshot from Silver using the same
    # source-file grouping and today's evaluation_date_key.
    snapshot_frames = []
    for source in TABLES:
        clean = spark.table(f"{SILVER}.{source}")
        quarantine = spark.table(f"{SILVER}.quarantined_{source}")
        valid = clean.groupBy(F.col("_source_file_name").alias("source_file_name")).agg(
            F.count("*").cast("long").alias("valid_records"))
        incorrect = quarantine.groupBy(
            F.col("_source_file_name").alias("source_file_name")).agg(
            F.count("*").cast("long").alias("incorrect_records"))
        snapshot = (valid.join(incorrect, "source_file_name", "full")
            .fillna({"valid_records": 0, "incorrect_records": 0})
            .withColumn("source_table", F.lit(source))
            .withColumn("total_records", (F.col("valid_records") + F.col("incorrect_records")).cast("long"))
            .withColumn("evaluation_date_key", F.date_format(F.current_date(), "yyyyMMdd").cast("int"))
            .withColumn("geography_key", F.lit(-1).cast("long"))
            .select("evaluation_date_key", "source_table", "source_file_name", "geography_key",
                    "total_records", "valid_records", "incorrect_records"))
        snapshot_frames.append(snapshot)
    expected_records = snapshot_frames[0]
    for frame in snapshot_frames[1:]:
        expected_records = expected_records.unionByName(frame)
    record_result = compare_grouped(
        expected_records, spark.table(f"{GOLD}.agg_data_quality_record_daily"),
        ["evaluation_date_key", "source_table", "source_file_name", "geography_key"],
        ["total_records", "valid_records", "incorrect_records"],
        "agg_data_quality_record_daily")

    # Independently verify the core DQ arithmetic, including every row in the table.
    record_table = spark.table(f"{GOLD}.agg_data_quality_record_daily")
    arithmetic_failures = record_table.filter(
        F.col("total_records") != F.col("valid_records") + F.col("incorrect_records")).count()
    print(f"agg_data_quality_record_daily arithmetic: "
          f"{'PASS' if arithmetic_failures == 0 else 'FAIL'} — "
          f"{arithmetic_failures:,} rows where total != valid + incorrect")
    return [dq_result, record_result, (arithmetic_failures, 0)]

def main():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    print("=" * 100)
    print("MANUAL MEDALLION RECONCILIATION — READ ONLY")
    print(f"RAW: {RAW_PATH} | Bronze: {BRONZE} | Silver: {SILVER} | Gold: {GOLD}")
    print("=" * 100)

    raw_files = list_raw_files()
    print(f"RAW Parquet files recognized: {len(raw_files)}")
    dates = set()
    for _, path in raw_files:
        match = re.search(r"_(\d{4}_\d{2}_\d{2})_part-\d+\.parquet$", path)
        if match:
            dates.add(match.group(1))
    print(f"RAW extraction date batches detected: {len(dates)}")
    if len(dates) > 1:
        print(
            "WARNING: counts include files from every extraction date. If each "
            "date represents a full snapshot, RAW may exceed Bronze's upserted "
            "current-state row count; review extraction semantics before treating "
            "that difference as a defect."
        )

    results = [reconcile_entity(spark, raw_files, table) for table in TABLES]

    print("\nROW COUNTS BY SOURCE ENTITY")
    header = (f"{'TABLE':<16} {'RAW':>14} {'BRONZE':>14} {'SILVER VALID':>14} "
              f"{'SILVER QUAR.':>14} {'GOLD VALID':>14} {'GOLD QUAR.':>14}")
    print(header)
    print("-" * len(header))
    for r in results:
        print(f"{r['table']:<16} {r['raw']:>14,} {r['bronze']:>14,} "
              f"{r['silver_valid']:>14,} {r['silver_quarantine']:>14,} "
              f"{r['gold_valid']:>14,} {r['gold_quarantine']:>14,}")

    checks = []
    for r in results:
        checks.extend([
            (r["table"], "RAW = BRONZE", r["raw"], r["bronze"]),
            (r["table"], "BRONZE = SILVER + SILVER QUAR.",
             r["bronze"], r["silver_valid"] + r["silver_quarantine"]),
            (r["table"], "SILVER VALID = GOLD VALID",
             r["silver_valid"], r["gold_valid"]),
            (r["table"], "SILVER QUAR. = GOLD QUAR.",
             r["silver_quarantine"], r["gold_quarantine"]),
            (r["table"], "GOLD VALID + GOLD QUAR. = BRONZE",
             r["gold_valid"] + r["gold_quarantine"], r["bronze"]),
        ])

    print("\nRECONCILIATION CHECKS")
    print(f"{'TABLE':<16} {'CHECK':<38} {'LEFT':>14} {'RIGHT':>14} {'RESULT':>9}")
    print("-" * 100)
    failures = []
    for table, label, left, right in checks:
        passed = left == right
        print(f"{table:<16} {label:<38} {left:>14,} {right:>14,} "
              f"{'PASS' if passed else 'FAIL':>9}")
        if not passed:
            failures.append((table, label, left, right))

    totals = {key: sum(row[key] for row in results) for key in (
        "raw", "bronze", "silver_valid", "silver_quarantine", "gold_valid", "gold_quarantine")}
    print("\nTOTALS ACROSS THE FIVE SOURCE ENTITIES")
    for label, key in (
        ("RAW", "raw"), ("BRONZE", "bronze"), ("SILVER VALID", "silver_valid"),
        ("SILVER QUARANTINE", "silver_quarantine"), ("GOLD VALID", "gold_valid"),
        ("GOLD QUARANTINE", "gold_quarantine")):
        print(f"{label:<22} {totals[key]:>18,}")
    print(f"{'SILVER VALID + QUAR.':<22} "
          f"{totals['silver_valid'] + totals['silver_quarantine']:>18,}")
    print(f"{'GOLD VALID + QUAR.':<22} "
          f"{totals['gold_valid'] + totals['gold_quarantine']:>18,}")

    # Aggregate validation is independent of the source-layer row-count checks.
    aggregate_results = reconcile_sales_aggregates(spark) + reconcile_dq_aggregates(spark)
    aggregate_failures = sum(mismatch + duplicates for mismatch, duplicates in aggregate_results)

    print("\nFINAL RESULT")
    if failures or aggregate_failures:
        print(f"FAIL — {len(failures)} source-layer count check(s) and "
              f"{aggregate_failures:,} aggregate validation issue(s) detected.")
        print("This script is read-only. Review the differences before changing any data.")
        sys.exit(1)
    print("PASS — source-layer counts and all five Gold aggregate tables reconcile.")


if __name__ == "__main__":
    main()
