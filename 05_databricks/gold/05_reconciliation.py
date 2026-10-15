#!/usr/bin/env python
"""Read-only integrity and reconciliation gate for the US Retail Gold layer.

Run after 03_load_gold_schema_tables.py and 04_load_gold_agg_tables.py.
This script does not modify data. Any failed assertion raises an exception.
"""
from pyspark.sql import functions as F

S = "workspace.silver"
G = "workspace.gold"

def assert_zero(df, label):
    sample = df.limit(10)
    if sample.count():
        raise RuntimeError(f"FAIL: {label}. Sample: {[r.asDict(recursive=True) for r in sample.collect()]}")

def assert_count_equal(source, target, label):
    a = spark.table(source).count()
    b = spark.table(target).count()
    if a != b:
        raise RuntimeError(f"FAIL: {label}: source={a}, target={b}")
    print(f"PASS: {label}: {a:,} rows")

def check_scd2(dim_name, natural_key, surrogate_key):
    d = spark.table(f"{G}.{dim_name}")
    business = d.filter((F.col(surrogate_key) != -1) & F.col(natural_key).isNotNull())
    assert_zero(business.filter(F.col("effective_from") >= F.col("effective_to")),
        f"{dim_name}: invalid SCD2 interval")
    assert_zero(business.filter(F.col("is_current") == True).groupBy(natural_key).count().filter("count != 1"),
        f"{dim_name}: expected exactly one current row per natural key")
    a, b = business.alias("a"), business.alias("b")
    overlap = a.join(b,
        (F.col(f"a.{natural_key}") == F.col(f"b.{natural_key}")) &
        (F.col(f"a.{surrogate_key}") < F.col(f"b.{surrogate_key}")) &
        (F.col("a.effective_from") < F.col("b.effective_to")) &
        (F.col("b.effective_from") < F.col("a.effective_to")), "inner")
    assert_zero(overlap, f"{dim_name}: overlapping SCD2 intervals")
    print(f"PASS: {dim_name} SCD2 integrity")

def main():
    spark.conf.set("spark.sql.session.timeZone","UTC")
    # Required tables
    for name in ["customers","stores","products","invoices","invoice_items"]:
        if not spark.catalog.tableExists(f"{S}.{name}"):
            raise RuntimeError(f"Missing required Silver table {S}.{name}")
    for name in ["dim_customer","dim_store","dim_product","dim_date","fact_invoice",
                 "fact_invoice_item","agg_sales_daily","agg_sales_product_daily",
                 "agg_sales_product_store_daily","agg_data_quality_daily","agg_data_quality_record_daily"]:
        if not spark.catalog.tableExists(f"{G}.{name}"):
            raise RuntimeError(f"Missing required Gold table {G}.{name}")

    assert_count_equal(f"{S}.invoices", f"{G}.fact_invoice", "Silver invoices vs Gold fact_invoice")
    assert_count_equal(f"{S}.invoice_items", f"{G}.fact_invoice_item", "Silver invoice_items vs Gold fact_invoice_item")
    assert_zero(spark.table(f"{G}.fact_invoice").groupBy("invoice_id").count().filter("count != 1"),
        "Gold fact_invoice must have exactly one row per invoice_id")
    assert_zero(spark.table(f"{G}.fact_invoice_item").groupBy("invoice_id","line_item").count().filter("count != 1"),
        "Gold fact_invoice_item must have exactly one row per (invoice_id,line_item)")

    check_scd2("dim_customer","customer_id","customer_key")
    check_scd2("dim_store","store_id","store_key")
    check_scd2("dim_product","product_id","product_key")

    # A populated source ID must never silently map to the unknown member.
    bad_customer = (spark.table(f"{S}.invoices").filter(F.col("customer_id").isNotNull()).alias("s")
        .join(spark.table(f"{G}.fact_invoice").alias("g"),"invoice_id","inner")
        .filter(F.col("g.customer_key") == -1))
    assert_zero(bad_customer, "Non-null Silver customer_id mapped to customer_key=-1")
    bad_store = (spark.table(f"{S}.invoices").filter(F.col("store_id").isNotNull()).alias("s")
        .join(spark.table(f"{G}.fact_invoice").alias("g"),"invoice_id","inner")
        .filter(F.col("g.store_key") == -1))
    assert_zero(bad_store, "Non-null Silver store_id mapped to store_key=-1")
    bad_product = (spark.table(f"{S}.invoice_items").filter(F.col("product_id").isNotNull()).alias("s")
        .join(spark.table(f"{G}.fact_invoice_item").alias("g"),
              ["invoice_id","line_item"],"inner").filter(F.col("g.product_key") == -1))
    assert_zero(bad_product, "Non-null Silver product_id mapped to product_key=-1")

    # Verify each non-null natural key has exactly one temporal match for the transaction date.
    for entity, src_table, natural, fact_table, fact_natural, skey, date_col in [
        ("customer","invoices","customer_id","fact_invoice","invoice_id","customer_key","invoice_date"),
        ("store","invoices","store_id","fact_invoice","invoice_id","store_key","invoice_date"),
    ]:
        source = spark.table(f"{S}.{src_table}").filter(F.col(natural).isNotNull()).alias("s")
        dim = spark.table(f"{G}.dim_{entity}").alias("d")
        matched = source.join(dim,
            (F.col(f"s.{natural}").cast("string")==F.col(f"d.{natural}").cast("string")) &
            (F.col(f"s.{date_col}").cast("timestamp")>=F.col("d.effective_from")) &
            (F.col(f"s.{date_col}").cast("timestamp")<F.col("d.effective_to")),"left") \
            .groupBy(F.col(f"s.{fact_natural}").alias("business_id")).agg(F.count(F.col(f"d.{skey}")).alias("matches"))
        assert_zero(matched.filter(F.col("matches") != 1),
            f"{entity}: non-null natural key must resolve to exactly one SCD2 version")
    source_items = (spark.table(f"{S}.invoice_items").alias("ii")
        .join(spark.table(f"{S}.invoices").select("invoice_id","invoice_date").alias("ih"),
              F.col("ii.invoice_id")==F.col("ih.invoice_id"),"inner"))
    dim_product = spark.table(f"{G}.dim_product").alias("d")
    pm = source_items.join(dim_product,
        (F.col("ii.product_id").cast("string")==F.col("d.product_id").cast("string")) &
        (F.col("ih.invoice_date").cast("timestamp")>=F.col("d.effective_from")) &
        (F.col("ih.invoice_date").cast("timestamp")<F.col("d.effective_to")),"left") \
        .groupBy(F.col("ii.invoice_id").alias("invoice_id"),F.col("ii.line_item").alias("line_item")) \
        .agg(F.count(F.col("d.product_key")).alias("matches"))
    assert_zero(pm.filter(F.col("matches") != 1),
        "Product: each Silver invoice line must resolve to exactly one SCD2 product version")

    # Date foreign-key integrity.
    assert_zero(spark.table(f"{G}.fact_invoice").filter(F.col("date_key").isNull()),
        "Gold fact_invoice has NULL date_key")
    assert_zero(spark.table(f"{G}.fact_invoice").alias("f")
        .join(spark.table(f"{G}.dim_date").select("date_key").alias("d"),"date_key","left_anti"),
        "Gold fact_invoice contains a date_key missing from dim_date")

    # Aggregate grain uniqueness.
    for table, grain in [
        ("agg_sales_daily",["date_key","store_key","loyalty_tier","payment_method"]),
        ("agg_sales_product_daily",["date_key","product_key"]),
        ("agg_sales_product_store_daily",["date_key","product_key","store_key","loyalty_tier","payment_method"]),
        ("agg_data_quality_daily",["dq_date_key","source_table","source_file_name","dq_rule_key","severity","dq_status","geography_key"]),
        ("agg_data_quality_record_daily",["evaluation_date_key","source_table","source_file_name","geography_key"]),
    ]:
        df = spark.table(f"{G}.{table}")
        assert_zero(df.groupBy(*grain).count().filter("count != 1"), f"{table}: duplicate aggregate grain")
        print(f"PASS: {table} grain uniqueness")

    # Revenue/units/margin aggregate reconciliation against Gold line facts.
    # Recompute with the same approved formula: quantity * unit_price - discount;
    # discount NULL is treated as zero.
    inv = spark.table(f"{G}.fact_invoice").select("invoice_key","date_key","store_key","customer_key","payment_method")
    cust = spark.table(f"{G}.dim_customer").select("customer_key","loyalty_tier")
    item = spark.table(f"{G}.fact_invoice_item").alias("ii")
    prod = spark.table(f"{G}.dim_product").select("product_key","cost")
    lines = (item.join(inv,"invoice_key","inner").join(cust,"customer_key","left").join(prod,"product_key","left")
        .withColumn("loyalty_tier",F.coalesce(F.col("loyalty_tier"),F.lit("Unknown")))
        .withColumn("revenue",F.col("quantity")*F.col("unit_price")-F.coalesce(F.col("discount"),F.lit(0)))
        .withColumn("margin",F.col("revenue")-F.col("quantity")*F.coalesce(F.col("cost"),F.lit(0))))
    expected_daily = lines.groupBy("date_key","store_key","loyalty_tier","payment_method").agg(
        F.sum("revenue").cast("decimal(20,2)").alias("expected_revenue"),
        F.sum("quantity").cast("long").alias("expected_units_sold"),
        F.sum("margin").cast("decimal(20,2)").alias("expected_gross_margin"))
    actual_daily = spark.table(f"{G}.agg_sales_daily").select("date_key","store_key","loyalty_tier","payment_method",
        F.col("revenue").alias("actual_revenue"),F.col("units_sold").alias("actual_units_sold"),
        F.col("gross_margin").alias("actual_gross_margin"))
    daily_compare = expected_daily.join(actual_daily,
        ["date_key","store_key","loyalty_tier","payment_method"],"full")
    assert_zero(daily_compare.filter(
        ~F.col("expected_revenue").eqNullSafe(F.col("actual_revenue")) |
        ~F.col("expected_units_sold").eqNullSafe(F.col("actual_units_sold")) |
        ~F.col("expected_gross_margin").eqNullSafe(F.col("actual_gross_margin"))),
        "agg_sales_daily measure mismatch")
    expected_product = lines.groupBy("date_key","product_key").agg(
        F.sum("quantity").cast("long").alias("expected_units_sold"),
        F.sum("revenue").cast("decimal(20,2)").alias("expected_revenue"),
        F.sum("margin").cast("decimal(20,2)").alias("expected_gross_margin"))
    actual_product = spark.table(f"{G}.agg_sales_product_daily").select("date_key","product_key",
        F.col("units_sold").alias("actual_units_sold"),F.col("revenue").alias("actual_revenue"),
        F.col("gross_margin").alias("actual_gross_margin"))
    product_compare = expected_product.join(actual_product,["date_key","product_key"],"full")
    assert_zero(product_compare.filter(
        ~F.col("expected_units_sold").eqNullSafe(F.col("actual_units_sold")) |
        ~F.col("expected_revenue").eqNullSafe(F.col("actual_revenue")) |
        ~F.col("expected_gross_margin").eqNullSafe(F.col("actual_gross_margin"))),
        "agg_sales_product_daily measure mismatch")
    expected_ps = lines.groupBy("date_key","product_key","store_key","loyalty_tier","payment_method").agg(
        F.sum("quantity").cast("long").alias("expected_units_sold"),
        F.sum("revenue").cast("decimal(20,2)").alias("expected_revenue"),
        F.sum("margin").cast("decimal(20,2)").alias("expected_gross_margin"))
    actual_ps = spark.table(f"{G}.agg_sales_product_store_daily").select(
        "date_key","product_key","store_key","loyalty_tier","payment_method",
        F.col("units_sold").alias("actual_units_sold"),F.col("revenue").alias("actual_revenue"),
        F.col("gross_margin").alias("actual_gross_margin"))
    ps_compare = expected_ps.join(actual_ps,
        ["date_key","product_key","store_key","loyalty_tier","payment_method"],"full")
    assert_zero(ps_compare.filter(
        ~F.col("expected_units_sold").eqNullSafe(F.col("actual_units_sold")) |
        ~F.col("expected_revenue").eqNullSafe(F.col("actual_revenue")) |
        ~F.col("expected_gross_margin").eqNullSafe(F.col("actual_gross_margin"))),
        "agg_sales_product_store_daily measure mismatch")
    print("PASS: base key integrity, temporal resolution, row counts, all sales aggregate measures and aggregate grains")

if __name__ == "__main__":
    main()
