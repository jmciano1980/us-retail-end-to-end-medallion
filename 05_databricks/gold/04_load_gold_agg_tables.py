#!/usr/bin/env python
"""Build the five Gold aggregate tables.

Run as a Databricks Python script task after the Gold base-model loader.
This first implementation deliberately rebuilds aggregates in full so that
grain and measure semantics can be validated before incremental optimization.
Set Job maximum concurrent runs to 1.
"""
from pyspark.sql import functions as F
from functools import reduce

G = "workspace.gold"
S = "workspace.silver"

def replace_table(df, target):
    (df.withColumn("_last_updated_utc", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema","true")
       .saveAsTable(target))

def build_sales_aggregates():
    invoices = spark.table(f"{G}.fact_invoice").alias("i")
    items = spark.table(f"{G}.fact_invoice_item").alias("ii")
    dates = spark.table(f"{G}.dim_date").select("date_key","full_date")
    stores = spark.table(f"{G}.dim_store").select("store_key","store_id","is_current")
    customers = spark.table(f"{G}.dim_customer").select("customer_key","loyalty_tier")
    products = spark.table(f"{G}.dim_product").select("product_key","cost")

    # Gold facts already resolve the SCD2 versions. Join through invoice_key;
    # do not add date/store/customer keys to fact_invoice_item itself.
    invoice_context = (invoices.join(dates,"date_key","inner")
        .join(stores,"store_key","left")
        .join(customers,"customer_key","left")
        .select("invoice_key","invoice_id","date_key","store_key","payment_method",
            F.coalesce("loyalty_tier",F.lit("Unknown")).alias("loyalty_tier"),
            "full_date"))

    line = (items.join(invoice_context.select("invoice_key","date_key","store_key","payment_method","loyalty_tier","full_date"),
                       "invoice_key","inner")
        .join(products,"product_key","left")
        .withColumn("line_revenue",
            F.col("quantity")*F.col("unit_price")-F.coalesce(F.col("discount"),F.lit(0)))
        .withColumn("line_cost",F.col("quantity")*F.coalesce(F.col("cost"),F.lit(0)))
        .withColumn("line_margin",F.col("line_revenue")-F.col("line_cost")))

    # Aggregate 1: invoice counts are calculated from header facts, not invoice
    # lines, to avoid counting a multi-line invoice more than once.
    revenue = line.groupBy("date_key","store_key","loyalty_tier","payment_method").agg(
        F.sum("line_revenue").cast("decimal(20,2)").alias("revenue"),
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("line_margin").cast("decimal(20,2)").alias("gross_margin"))
    invoice_counts = invoice_context.groupBy("date_key","store_key","loyalty_tier","payment_method").agg(
        F.countDistinct("invoice_id").cast("long").alias("invoice_count"))
    sales_daily = revenue.join(invoice_counts,
        ["date_key","store_key","loyalty_tier","payment_method"],"full").fillna(
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
        daily = dq.groupBy(F.col("date_key").alias("dq_date_key"),"source_table","source_file_name",
            "dq_rule_key","severity","dq_status","geography_key").agg(
            F.count("*").cast("long").alias("dq_issue_count"),
            F.countDistinct(F.struct("source_table","source_row_id")).cast("long").alias("affected_record_count"))
    else:
        daily = spark.createDataFrame([], """dq_date_key int, source_table string, source_file_name string,
            dq_rule_key long, severity string, dq_status string, geography_key long,
            dq_issue_count long, affected_record_count long""")
    replace_table(daily,f"{G}.agg_data_quality_daily")

    # DQ percentages require an explicit evaluated-record denominator. Build a
    # current per-file snapshot from Silver clean and quarantine tables. This
    # does not infer denominator counts from the issue table.
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
        snap = valid.join(incorrect,"source_file_name","full").fillna({"valid_records":0,"incorrect_records":0}) \
            .withColumn("source_table",F.lit(name)) \
            .withColumn("total_records",(F.col("valid_records")+F.col("incorrect_records")).cast("long")) \
            .withColumn("evaluation_date_key",F.date_format(F.current_date(),"yyyyMMdd").cast("int")) \
            .withColumn("geography_key",F.lit(-1).cast("long"))
        snapshots.append(snap.select("evaluation_date_key","source_table","source_file_name","geography_key",
                                     "total_records","valid_records","incorrect_records"))
    record_daily = reduce(lambda a,b:a.unionByName(b),snapshots)
    replace_table(record_daily,f"{G}.agg_data_quality_record_daily")

def main():
    spark.conf.set("spark.sql.session.timeZone","UTC")
    build_sales_aggregates()
    build_dq_aggregates()
    print("Gold aggregate build completed. Validate counts and reconciliation before Power BI use.")

if __name__ == "__main__":
    main()
