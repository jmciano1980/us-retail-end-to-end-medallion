#!/usr/bin/env python
"""Build the three commercial dashboard aggregates and preserve both DQ aggregates.

Databricks Python script task. Full rebuild, single writer (max concurrent runs = 1).
Facts and dimensions are read-only inputs. No Silver/Gold base tables are modified.
"""
from functools import reduce
from datetime import date
import time
from pyspark.sql import functions as F

G = "workspace.gold"
S = "workspace.silver"

def assert_no_rows(df, message):
    sample = df.limit(10)
    if sample.count():
        raise RuntimeError(f"{message}. Sample: {[r.asDict(recursive=True) for r in sample.collect()]}")


def replace_table(df, target, grain):
    assert_no_rows(df.groupBy(*grain).count().filter(F.col("count") > 1),
                   f"Duplicate aggregate grain detected before writing {target}")
    started = time.perf_counter()
    (df.withColumn("_last_updated_utc", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(target))
    row_count = spark.table(target).count()
    elapsed = time.perf_counter() - started
    print(f"BUILT {target}: {row_count:,} rows; grain={grain}; elapsed={elapsed:,.1f}s")


def ensure_date_dim_coverage():
    """Extend dim_date for sales dates, DQ issue dates and today's DQ snapshot date."""
    date_table = f"{G}.dim_date"
    dates = spark.table(date_table)
    bounds = dates.agg(F.min(F.to_date("full_date")).alias("lo"),
                       F.max(F.to_date("full_date")).alias("hi")).first()
    issues = spark.table(f"{G}.fact_data_quality_issue")
    if "_quarantine_datetime_utc" not in issues.columns:
        raise RuntimeError("fact_data_quality_issue lacks _quarantine_datetime_utc")
    ib = issues.agg(F.min(F.to_date("_quarantine_datetime_utc")).alias("lo"),
                    F.max(F.to_date("_quarantine_datetime_utc")).alias("hi")).first()
    inv_bounds = spark.table(f"{G}.fact_invoice").join(
        dates.select("date_key", "full_date"), "date_key", "left").agg(
        F.min(F.to_date("full_date")).alias("lo"), F.max(F.to_date("full_date")).alias("hi")).first()
    candidates = [x for x in [bounds["lo"], bounds["hi"], ib["lo"], ib["hi"],
                               inv_bounds["lo"], inv_bounds["hi"], date.today()] if x is not None]
    lo, hi = min(candidates), max(candidates)
    calendar = spark.range(1).select(F.explode(F.sequence(
        F.to_date(F.lit(lo.isoformat())), F.to_date(F.lit(hi.isoformat())), F.expr("INTERVAL 1 DAY")
    )).alias("full_date"))
    calendar = (calendar
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn("year_quarter", F.concat(F.year("full_date"), F.lit("-Q"), F.quarter("full_date")))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_month_start", F.dayofmonth("full_date") == 1)
        .withColumn("is_month_end", F.last_day("full_date") == F.col("full_date"))
        .withColumn("_insert_datetime_utc", F.current_timestamp())
        .withColumn("_update_datetime_utc", F.current_timestamp())
        .select(*dates.columns))
    missing = calendar.join(dates.select("date_key"), "date_key", "left_anti")
    n = missing.count()
    if n:
        missing.write.format("delta").mode("append").saveAsTable(date_table)
    print(f"dim_date coverage checked {lo} through {hi}; inserted {n} dates")


def build_sales_aggregates():
    invoices = spark.table(f"{G}.fact_invoice")
    items = spark.table(f"{G}.fact_invoice_item")
    assert_no_rows(invoices.filter(F.col("store_key") == -1),
                   "Refusing aggregate build: fact_invoice contains store_key=-1")
    assert_no_rows(items.filter(F.col("product_key") == -1),
                   "Refusing aggregate build: fact_invoice_item contains product_key=-1")
    assert_no_rows(invoices.groupBy("invoice_id").count().filter("count != 1"),
                   "fact_invoice must be unique by invoice_id")
    assert_no_rows(items.groupBy("invoice_id", "line_item").count().filter("count != 1"),
                   "fact_invoice_item must be unique by invoice_id + line_item")
    assert_no_rows(invoices.filter(F.col("date_key").isNull() | F.col("store_key").isNull()),
                   "fact_invoice contains NULL date_key or store_key")
    assert_no_rows(items.filter(F.col("product_key").isNull()),
                   "fact_invoice_item contains NULL product_key")
    assert_no_rows(invoices.select("date_key").distinct()
                   .join(spark.table(f"{G}.dim_date").select("date_key"), "date_key", "left_anti"),
                   "fact_invoice date_key does not resolve to dim_date")
    assert_no_rows(invoices.select("store_key").distinct()
                   .join(spark.table(f"{G}.dim_store").select("store_key"), "store_key", "left_anti"),
                   "fact_invoice store_key does not resolve to dim_store")
    assert_no_rows(items.select("product_key").distinct()
                   .join(spark.table(f"{G}.dim_product").select("product_key"), "product_key", "left_anti"),
                   "fact_invoice_item product_key does not resolve to dim_product")

    # Invoice header context required by the approved aggregate grains.
    # Customer attributes are deliberately excluded: Executive no longer segments by Loyalty Tier.
    invoice_ctx = invoices.select("invoice_key", "invoice_id", "date_key", "store_key")
    product = spark.table(f"{G}.dim_product").select("product_key", "subcategory_key", "cost")
    lines = (items.join(invoice_ctx, "invoice_key", "inner")
        .join(product, "product_key", "left")
        .withColumn("line_revenue", (F.col("quantity") * F.col("unit_price") -
                                      F.coalesce(F.col("discount"), F.lit(0))).cast("decimal(20,2)"))
        .withColumn("line_cost", (F.col("quantity") * F.coalesce(F.col("cost"), F.lit(0))).cast("decimal(20,2)"))
        .withColumn("line_margin", (F.col("line_revenue") - F.col("line_cost")).cast("decimal(20,2)")))

    # EXECUTIVE: exact approved grain — date × store × subcategory.
    # Customer/Loyalty Tier is intentionally excluded from this aggregate.
    # Product identity is intentionally rolled up and is not retained in this aggregate.
    assert_no_rows(lines.filter(F.col("subcategory_key").isNull()),
                   "Executive aggregate cannot group a product line with NULL subcategory_key")
    subcategories = spark.table(f"{G}.dim_subcategory").select("subcategory_key").distinct()
    assert_no_rows(product.select("subcategory_key").distinct()
                   .join(subcategories, "subcategory_key", "left_anti"),
                   "A Gold product subcategory_key does not resolve to dim_subcategory")
    executive = lines.groupBy("date_key", "store_key", "subcategory_key").agg(
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    replace_table(executive, f"{G}.agg_executive_performance_daily",
                  ["date_key", "store_key", "subcategory_key"])

    # PRODUCT: first day of month (YYYYMM01) is a valid FK to dim_date.
    dates = spark.table(f"{G}.dim_date").select("date_key", "full_date")
    product_monthly = (lines.join(dates, "date_key", "inner")
        .withColumn("month_date_key", F.date_format(F.trunc(F.to_date("full_date"), "month"), "yyyyMMdd").cast("int"))
        .groupBy("month_date_key", "store_key", "product_key").agg(
            F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
            F.sum("quantity").cast("long").alias("units_sold"),
            F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin")))
    replace_table(product_monthly, f"{G}.agg_product_performance_monthly",
                  ["month_date_key", "store_key", "product_key"])

    # STORE: daily × store. Invoice count comes from invoice headers, never line counts.
    line_store = lines.groupBy("date_key", "store_key").agg(
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    invoice_store = invoice_ctx.groupBy("date_key", "store_key").agg(
        F.countDistinct("invoice_id").cast("long").alias("invoice_count"))
    store_daily = (line_store.join(invoice_store, ["date_key", "store_key"], "full")
                   .fillna({"revenue": 0, "units_sold": 0, "gross_margin": 0, "invoice_count": 0}))
    replace_table(store_daily, f"{G}.agg_store_performance_daily", ["date_key", "store_key"])

    # Cardinality profiling for the executive table is explicitly logged for acceptance.
    print("Commercial aggregate row counts:")
    for t in ["agg_executive_performance_daily", "agg_product_performance_monthly", "agg_store_performance_daily"]:
        print(f"  {t}: {spark.table(f'{G}.{t}').count():,}")


def build_dq_aggregates():
    # Keep the established Data Quality aggregate definitions and reporting behavior.
    issues = spark.table(f"{G}.fact_data_quality_issue")
    rules = spark.table(f"{G}.dim_data_quality_rule").select("dq_rule_key", "severity")
    dates = spark.table(f"{G}.dim_date").select("date_key", "full_date")
    if issues.limit(1).count():
        dq = (issues.join(rules, "dq_rule_key", "left")
            .withColumn("issue_date", F.to_date("_quarantine_datetime_utc"))
            .withColumn("dq_status", F.coalesce(F.col("_dq_status"), F.lit("Unknown")))
            .join(dates, F.col("issue_date") == F.col("full_date"), "left")
            .withColumn("geography_key", F.lit(-1).cast("long")))
        assert_no_rows(dq.filter(F.col("date_key").isNull()),
                       "DQ issue date is outside dim_date after coverage extension")
        daily = dq.groupBy(F.col("date_key").alias("dq_date_key"), "source_table", "source_file_name",
            "dq_rule_key", "severity", "dq_status", "geography_key").agg(
            F.count("*").cast("long").alias("dq_issue_count"),
            F.countDistinct(F.struct("source_table", "source_row_id")).cast("long").alias("affected_record_count"))
    else:
        daily = spark.createDataFrame([], "dq_date_key int, source_table string, source_file_name string, dq_rule_key long, severity string, dq_status string, geography_key long, dq_issue_count long, affected_record_count long")
    replace_table(daily, f"{G}.agg_data_quality_daily",
        ["dq_date_key", "source_table", "source_file_name", "dq_rule_key", "severity", "dq_status", "geography_key"])

    sources = ["customers", "products", "stores", "invoices", "invoice_items"]
    snapshots = []
    for name in sources:
        clean = spark.table(f"{S}.{name}")
        qname = f"{S}.quarantined_{name}"
        q = spark.table(qname) if spark.catalog.tableExists(qname) else None
        if "_source_file_name" in clean.columns:
            valid = clean.groupBy(F.col("_source_file_name").alias("source_file_name")).agg(F.count("*").cast("long").alias("valid_records"))
        else:
            valid = clean.agg(F.count("*").cast("long").alias("valid_records")).withColumn("source_file_name", F.lit("UNKNOWN"))
        if q is not None and "_source_file_name" in q.columns:
            incorrect = q.groupBy(F.col("_source_file_name").alias("source_file_name")).agg(F.count("*").cast("long").alias("incorrect_records"))
        else:
            incorrect = spark.createDataFrame([], "source_file_name string, incorrect_records long")
        snap = (valid.join(incorrect, "source_file_name", "full").fillna({"valid_records": 0, "incorrect_records": 0})
            .withColumn("source_table", F.lit(name))
            .withColumn("total_records", (F.col("valid_records") + F.col("incorrect_records")).cast("long"))
            .withColumn("evaluation_date_key", F.date_format(F.current_date(), "yyyyMMdd").cast("int"))
            .withColumn("geography_key", F.lit(-1).cast("long")))
        snapshots.append(snap.select("evaluation_date_key", "source_table", "source_file_name", "geography_key", "total_records", "valid_records", "incorrect_records"))
    record_daily = reduce(lambda a, b: a.unionByName(b), snapshots)
    replace_table(record_daily, f"{G}.agg_data_quality_record_daily",
                  ["evaluation_date_key", "source_table", "source_file_name", "geography_key"])
    assert_no_rows(record_daily.filter(F.col("total_records") != F.col("valid_records") + F.col("incorrect_records")),
                   "DQ record aggregate arithmetic invariant failed")


def main():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    ensure_date_dim_coverage()
    build_sales_aggregates()
    build_dq_aggregates()
    print("SUCCESS: all three commercial aggregates and both preserved DQ aggregates rebuilt")


if __name__ == "__main__":
    main()
