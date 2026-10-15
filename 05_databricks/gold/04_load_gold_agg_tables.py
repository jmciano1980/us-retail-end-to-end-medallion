#!/usr/bin/env python
"""Rebuild the five Gold aggregates after the base model passes integrity checks.
Databricks Python script task; single writer; maximum concurrent runs = 1.
"""
from functools import reduce
from pyspark.sql import functions as F

G = "workspace.gold"
S = "workspace.silver"

def assert_no_rows(df, message):
    sample = df.limit(10)
    if sample.count():
        raise RuntimeError(f"{message}. Sample: {[r.asDict(recursive=True) for r in sample.collect()]}")

def replace_table(df, target):
    if df.groupBy(*[c for c in df.columns if c != "_last_updated_utc"]).count().filter("count > 1").limit(1).count():
        raise RuntimeError(f"Duplicate aggregate grain detected before writing {target}")
    (df.withColumn("_last_updated_utc", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(target))


def ensure_date_dim_coverage():
    """Extend dim_date to cover sales dates, DQ quarantine dates, and today's DQ snapshot.

    The base loader originally bounded dim_date only by Silver invoice_date. DQ issues
    are dated by _quarantine_datetime_utc, which can occur after the latest invoice date.
    Add missing calendar dates before any aggregate is built; never map a real date to -1.
    """
    date_table = f"{G}.dim_date"
    dates = spark.table(date_table)
    existing_bounds = dates.agg(
        F.min(F.to_date("full_date")).alias("lo"),
        F.max(F.to_date("full_date")).alias("hi")
    ).first()

    issue_table = f"{G}.fact_data_quality_issue"
    issues = spark.table(issue_table)
    if "_quarantine_datetime_utc" not in issues.columns:
        raise RuntimeError(
            f"{issue_table} has no _quarantine_datetime_utc column; "
            "cannot establish Data Quality reporting dates."
        )
    issue_bounds = issues.agg(
        F.min(F.to_date("_quarantine_datetime_utc")).alias("lo"),
        F.max(F.to_date("_quarantine_datetime_utc")).alias("hi")
    ).first()

    bounds = [d for d in (
        existing_bounds["lo"], existing_bounds["hi"],
        issue_bounds["lo"], issue_bounds["hi"]
    ) if d is not None]
    # agg_data_quality_record_daily uses current_date() as its evaluation date.
    from datetime import date
    bounds.append(date.today())
    lo, hi = min(bounds), max(bounds)

    calendar = spark.range(1).select(F.explode(
        F.sequence(F.to_date(F.lit(lo.isoformat())), F.to_date(F.lit(hi.isoformat())),
                   F.expr("INTERVAL 1 DAY"))
    ).alias("full_date"))
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
    count_missing = missing.count()
    if count_missing:
        (missing.write.format("delta").mode("append").saveAsTable(date_table))
    print(f"dim_date coverage: {lo} through {hi}; inserted {count_missing} missing date(s).")

def validate_base():
    invoices = spark.table(f"{G}.fact_invoice")
    items = spark.table(f"{G}.fact_invoice_item")
    assert_no_rows(invoices.filter(F.col("store_key") == -1),
        "Refusing to aggregate: fact_invoice contains store_key=-1")
    assert_no_rows(items.filter(F.col("product_key") == -1),
        "Refusing to aggregate: fact_invoice_item contains product_key=-1")
    assert_no_rows(invoices.groupBy("invoice_id").count().filter("count != 1"),
        "Refusing to aggregate: fact_invoice invoice_id is not unique")
    assert_no_rows(items.groupBy("invoice_id","line_item").count().filter("count != 1"),
        "Refusing to aggregate: fact_invoice_item grain is not unique")

def build_sales_aggregates():
    invoices = spark.table(f"{G}.fact_invoice").alias("i")
    items = spark.table(f"{G}.fact_invoice_item").alias("ii")
    dates = spark.table(f"{G}.dim_date").select("date_key","full_date")
    stores = spark.table(f"{G}.dim_store").select("store_key","store_id","is_current")
    customers = spark.table(f"{G}.dim_customer").select("customer_key","loyalty_tier")
    products = spark.table(f"{G}.dim_product").select("product_key","cost")
    invoice_context = (invoices.join(dates,"date_key","inner")
        .join(stores,"store_key","left")
        .join(customers,"customer_key","left")
        .select("invoice_key","invoice_id","date_key","store_key","payment_method",
            F.coalesce("loyalty_tier",F.lit("Unknown")).alias("loyalty_tier"),"full_date"))
    line = (items.join(invoice_context.select("invoice_key","date_key","store_key","payment_method","loyalty_tier","full_date"),
                       "invoice_key","inner")
        .join(products,"product_key","left")
        .withColumn("line_revenue",
            F.col("quantity")*F.col("unit_price")-F.coalesce(F.col("discount"),F.lit(0)))
        .withColumn("line_cost",F.col("quantity")*F.coalesce(F.col("cost"),F.lit(0)))
        .withColumn("line_margin",F.col("line_revenue")-F.col("line_cost")))
    revenue = line.groupBy("date_key","store_key","loyalty_tier","payment_method").agg(
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    invoice_counts = invoice_context.groupBy("date_key","store_key","loyalty_tier","payment_method").agg(
        F.countDistinct("invoice_id").cast("long").alias("invoice_count"))
    sales_daily = revenue.join(invoice_counts,["date_key","store_key","loyalty_tier","payment_method"],"full").fillna(
        {"invoice_count":0,"units_sold":0,"revenue":0,"gross_margin":0})
    replace_table(sales_daily.select("date_key","store_key","loyalty_tier","payment_method",
        "invoice_count","revenue","units_sold","gross_margin"),f"{G}.agg_sales_daily")
    product_daily = line.groupBy("date_key","product_key").agg(
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    replace_table(product_daily,f"{G}.agg_sales_product_daily")
    product_store_daily = line.groupBy("date_key","product_key","store_key","loyalty_tier","payment_method").agg(
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    replace_table(product_store_daily,f"{G}.agg_sales_product_store_daily")

def build_dq_aggregates():
    issues = spark.table(f"{G}.fact_data_quality_issue")
    rules = spark.table(f"{G}.dim_data_quality_rule").select("dq_rule_key","severity")
    dates = spark.table(f"{G}.dim_date").select("date_key","full_date")
    if issues.limit(1).count():
        dq = (issues.join(rules,"dq_rule_key","left")
            .withColumn("issue_date",F.to_date("_quarantine_datetime_utc"))
            .withColumn("dq_status",F.coalesce(F.col("_dq_status"),F.lit("Unknown")))
            .join(dates,F.col("issue_date")==F.col("full_date"),"left")
            .withColumn("geography_key",F.lit(-1).cast("long")))
        assert_no_rows(dq.filter(F.col("date_key").isNull()),
            "DQ issue date is outside dim_date; extend dim_date before building DQ aggregates")
        daily = dq.groupBy(F.col("date_key").alias("dq_date_key"),"source_table","source_file_name",
            "dq_rule_key","severity","dq_status","geography_key").agg(
            F.count("*").cast("long").alias("dq_issue_count"),
            F.countDistinct(F.struct("source_table","source_row_id")).cast("long").alias("affected_record_count"))
    else:
        daily = spark.createDataFrame([], """dq_date_key int, source_table string, source_file_name string,
            dq_rule_key long, severity string, dq_status string, geography_key long,
            dq_issue_count long, affected_record_count long""")
    replace_table(daily,f"{G}.agg_data_quality_daily")
    sources = ["customers","products","stores","invoices","invoice_items"]
    snapshots = []
    for name in sources:
        clean = spark.table(f"{S}.{name}")
        qname = f"{S}.quarantined_{name}"
        q = spark.table(qname) if spark.catalog.tableExists(qname) else None
        if "_source_file_name" in clean.columns:
            valid = clean.groupBy(F.col("_source_file_name").alias("source_file_name")).agg(
                F.count("*").cast("long").alias("valid_records"))
        else:
            valid = clean.agg(F.count("*").cast("long").alias("valid_records")).withColumn("source_file_name",F.lit("UNKNOWN"))
        if q is not None and "_source_file_name" in q.columns:
            incorrect = q.groupBy(F.col("_source_file_name").alias("source_file_name")).agg(
                F.count("*").cast("long").alias("incorrect_records"))
        else:
            incorrect = spark.createDataFrame([], "source_file_name string, incorrect_records long")
        snap = valid.join(incorrect,"source_file_name","full").fillna(
            {"valid_records":0,"incorrect_records":0}).withColumn("source_table",F.lit(name)) \
            .withColumn("total_records",(F.col("valid_records")+F.col("incorrect_records")).cast("long")) \
            .withColumn("evaluation_date_key",F.date_format(F.current_date(),"yyyyMMdd").cast("int")) \
            .withColumn("geography_key",F.lit(-1).cast("long"))
        snapshots.append(snap.select("evaluation_date_key","source_table","source_file_name","geography_key",
            "total_records","valid_records","incorrect_records"))
    record_daily = reduce(lambda a,b:a.unionByName(b),snapshots)
    replace_table(record_daily,f"{G}.agg_data_quality_record_daily")

def main():
    spark.conf.set("spark.sql.session.timeZone","UTC")
    validate_base()
    ensure_date_dim_coverage()
    build_sales_aggregates()
    build_dq_aggregates()
    print("PASS: all five Gold aggregates rebuilt after base key-integrity checks.")
if __name__ == "__main__":
    main()
