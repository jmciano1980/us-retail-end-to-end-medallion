#!/usr/bin/env python
"""Manual, read-only reconciliation for Gold facts, dimensions and revised aggregates.
Run after 03_load_gold_schema_tables.py and 04_load_gold_agg_tables.py.
No data is inserted, updated, deleted, merged or overwritten.
"""
from pyspark.sql import functions as F
S, G = "workspace.silver", "workspace.gold"


def assert_zero(df, label):
    sample = df.limit(10)
    if sample.count():
        raise RuntimeError(f"FAIL: {label}. Sample: {[r.asDict(recursive=True) for r in sample.collect()]}")
    print(f"PASS: {label}")


def assert_count_equal(source, target, label):
    a, b = spark.table(source).count(), spark.table(target).count()
    if a != b:
        raise RuntimeError(f"FAIL: {label}: source={a:,}, target={b:,}")
    print(f"PASS: {label}: {a:,} rows")


def compare_aggregates(expected, actual, keys, measures, label):
    e, a = expected.alias("e"), actual.alias("a")
    joined = e.join(a, keys, "full")
    condition = F.lit(False)
    for m in measures:
        condition = condition | (~F.col(f"e.{m}").eqNullSafe(F.col(f"a.{m}")))
    assert_zero(joined.filter(condition), f"{label}: key coverage or measure mismatch")


def check_scd2(dim_name, natural_key, surrogate_key):
    d = spark.table(f"{G}.{dim_name}")
    business = d.filter((F.col(surrogate_key) != -1) & F.col(natural_key).isNotNull())
    assert_zero(business.filter(F.col("effective_from") >= F.col("effective_to")), f"{dim_name}: invalid SCD2 interval")
    assert_zero(business.filter(F.col("is_current") == True).groupBy(natural_key).count().filter("count != 1"),
                f"{dim_name}: expected one current version per natural key")
    a, b = business.alias("a"), business.alias("b")
    overlap = a.join(b,
        (F.col(f"a.{natural_key}") == F.col(f"b.{natural_key}")) &
        (F.col(f"a.{surrogate_key}") < F.col(f"b.{surrogate_key}")) &
        (F.col("a.effective_from") < F.col("b.effective_to")) &
        (F.col("b.effective_from") < F.col("a.effective_to")), "inner")
    assert_zero(overlap, f"{dim_name}: overlapping SCD2 intervals")


def main():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for name in ["customers", "stores", "products", "invoices", "invoice_items"]:
        if not spark.catalog.tableExists(f"{S}.{name}"):
            raise RuntimeError(f"Missing required Silver table {S}.{name}")
    required = ["dim_customer", "dim_store", "dim_product", "dim_subcategory", "dim_category", "dim_date", "fact_invoice", "fact_invoice_item",
        "agg_executive_performance_daily", "agg_product_performance_monthly", "agg_store_performance_daily",
        "agg_data_quality_daily", "agg_data_quality_record_daily"]
    for name in required:
        if not spark.catalog.tableExists(f"{G}.{name}"):
            raise RuntimeError(f"Missing required Gold table {G}.{name}")

    # Preserve base fact count, natural-grain, SCD2 and populated-natural-key safeguards.
    assert_count_equal(f"{S}.invoices", f"{G}.fact_invoice", "Silver invoices vs Gold fact_invoice")
    assert_count_equal(f"{S}.invoice_items", f"{G}.fact_invoice_item", "Silver invoice_items vs Gold fact_invoice_item")
    assert_zero(spark.table(f"{G}.fact_invoice").groupBy("invoice_id").count().filter("count != 1"), "fact_invoice unique by invoice_id")
    assert_zero(spark.table(f"{G}.fact_invoice_item").groupBy("invoice_id", "line_item").count().filter("count != 1"), "fact_invoice_item unique by invoice_id + line_item")
    check_scd2("dim_customer", "customer_id", "customer_key")
    check_scd2("dim_store", "store_id", "store_key")
    check_scd2("dim_product", "product_id", "product_key")
    assert_zero(spark.table(f"{G}.dim_product").filter(
        (F.col("product_key") != -1) & F.col("subcategory_key").isNull()),
        "A business product dimension row has NULL subcategory_key")

    bad_customer = (spark.table(f"{S}.invoices").filter(F.col("customer_id").isNotNull()).alias("s")
        .join(spark.table(f"{G}.fact_invoice").alias("g"), "invoice_id", "inner").filter(F.col("g.customer_key") == -1))
    assert_zero(bad_customer, "Non-null Silver customer_id mapped to customer_key=-1")
    bad_store = (spark.table(f"{S}.invoices").filter(F.col("store_id").isNotNull()).alias("s")
        .join(spark.table(f"{G}.fact_invoice").alias("g"), "invoice_id", "inner").filter(F.col("g.store_key") == -1))
    assert_zero(bad_store, "Non-null Silver store_id mapped to store_key=-1")
    bad_product = (spark.table(f"{S}.invoice_items").filter(F.col("product_id").isNotNull()).alias("s")
        .join(spark.table(f"{G}.fact_invoice_item").alias("g"), ["invoice_id", "line_item"], "inner")
        .filter(F.col("g.product_key") == -1))
    assert_zero(bad_product, "Non-null Silver product_id mapped to product_key=-1")

    # Preserve temporal SCD2 natural-key resolution checks at the transaction date.
    for entity, src_table, natural, business_id, surrogate, date_col in [
        ("customer", "invoices", "customer_id", "invoice_id", "customer_key", "invoice_date"),
        ("store", "invoices", "store_id", "invoice_id", "store_key", "invoice_date"),
    ]:
        source = spark.table(f"{S}.{src_table}").filter(F.col(natural).isNotNull()).alias("s")
        dim = spark.table(f"{G}.dim_{entity}").alias("d")
        matched = (source.join(dim,
            (F.col(f"s.{natural}").cast("string") == F.col(f"d.{natural}").cast("string")) &
            (F.col(f"s.{date_col}").cast("timestamp") >= F.col("d.effective_from")) &
            (F.col(f"s.{date_col}").cast("timestamp") < F.col("d.effective_to")), "left")
            .groupBy(F.col(f"s.{business_id}").alias("business_id"))
            .agg(F.count(F.col(f"d.{surrogate}")).alias("matches")))
        assert_zero(matched.filter(F.col("matches") != 1),
                    f"{entity}: each populated Silver natural key must resolve to exactly one SCD2 version")

    source_items = (spark.table(f"{S}.invoice_items").alias("ii")
        .join(spark.table(f"{S}.invoices").select("invoice_id", "invoice_date").alias("ih"),
              F.col("ii.invoice_id") == F.col("ih.invoice_id"), "inner"))
    dim_product = spark.table(f"{G}.dim_product").alias("d")
    product_matches = (source_items.join(dim_product,
        (F.col("ii.product_id").cast("string") == F.col("d.product_id").cast("string")) &
        (F.col("ih.invoice_date").cast("timestamp") >= F.col("d.effective_from")) &
        (F.col("ih.invoice_date").cast("timestamp") < F.col("d.effective_to")), "left")
        .groupBy(F.col("ii.invoice_id").alias("invoice_id"), F.col("ii.line_item").alias("line_item"))
        .agg(F.count(F.col("d.product_key")).alias("matches")))
    assert_zero(product_matches.filter(F.col("matches") != 1),
                "Product: each populated Silver invoice line must resolve to exactly one SCD2 product version")

    # Date and dimension-key referential integrity for facts.
    assert_zero(spark.table(f"{G}.fact_invoice").filter(F.col("date_key").isNull()), "fact_invoice has no NULL date_key")
    assert_zero(spark.table(f"{G}.fact_invoice").join(spark.table(f"{G}.dim_date").select("date_key"), "date_key", "left_anti"),
                "fact_invoice date_key exists in dim_date")
    assert_zero(spark.table(f"{G}.fact_invoice").filter(F.col("store_key") == -1), "fact_invoice has no unresolved store key")
    assert_zero(spark.table(f"{G}.fact_invoice_item").filter(F.col("product_key") == -1), "fact_invoice_item has no unresolved product key")
    assert_zero(spark.table(f"{G}.agg_executive_performance_daily").select("subcategory_key").distinct()
        .join(spark.table(f"{G}.dim_subcategory").select("subcategory_key").distinct(),
              "subcategory_key", "left_anti"),
        "Executive aggregate subcategory_key exists in dim_subcategory")
    assert_zero(spark.table(f"{G}.fact_invoice").select("store_key").distinct()
        .join(spark.table(f"{G}.dim_store").select("store_key").distinct(), "store_key", "left_anti"),
        "fact_invoice store_key exists in dim_store")
    assert_zero(spark.table(f"{G}.fact_invoice_item").select("product_key").distinct()
        .join(spark.table(f"{G}.dim_product").select("product_key").distinct(), "product_key", "left_anti"),
        "fact_invoice_item product_key exists in dim_product")

    inv = spark.table(f"{G}.fact_invoice").select("invoice_key", "invoice_id", "date_key", "store_key", "customer_key")
    items = spark.table(f"{G}.fact_invoice_item")
    prod = spark.table(f"{G}.dim_product").select("product_key", "subcategory_key", "cost")
    lines = (items.join(inv, "invoice_key", "inner").join(prod, "product_key", "left")
        .withColumn("revenue", (F.col("quantity") * F.col("unit_price") - F.coalesce(F.col("discount"), F.lit(0))).cast("decimal(20,2)"))
        .withColumn("margin", (F.col("revenue") - F.col("quantity") * F.coalesce(F.col("cost"), F.lit(0))).cast("decimal(20,2)")))

    # Grain uniqueness.
    grains = {
        "agg_executive_performance_daily": ["date_key", "store_key", "subcategory_key"],
        "agg_product_performance_monthly": ["month_date_key", "store_key", "product_key"],
        "agg_store_performance_daily": ["date_key", "store_key"],
        "agg_data_quality_daily": ["dq_date_key", "source_table", "source_file_name", "dq_rule_key", "severity", "dq_status", "geography_key"],
        "agg_data_quality_record_daily": ["evaluation_date_key", "source_table", "source_file_name", "geography_key"]}
    for table, grain in grains.items():
        assert_zero(spark.table(f"{G}.{table}").groupBy(*grain).count().filter("count != 1"), f"{table}: unique declared grain")

    measures = ["revenue", "units_sold", "gross_margin"]
    assert_zero(lines.filter(F.col("subcategory_key").isNull()),
                "Executive reconciliation source has NULL subcategory_key")
    exp_exec = lines.groupBy("date_key", "store_key", "subcategory_key").agg(
        F.sum("revenue").cast("decimal(20,2)").alias("revenue"), F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("margin").cast("decimal(20,2)").alias("gross_margin"))
    act_exec = spark.table(f"{G}.agg_executive_performance_daily").select(*grains["agg_executive_performance_daily"], *measures)
    compare_aggregates(exp_exec, act_exec, grains["agg_executive_performance_daily"], measures, "Executive aggregate")

    dates = spark.table(f"{G}.dim_date").select("date_key", "full_date")
    exp_product = (lines.join(dates, "date_key", "inner")
        .withColumn("month_date_key", F.date_format(F.trunc(F.to_date("full_date"), "month"), "yyyyMMdd").cast("int"))
        .groupBy("month_date_key", "store_key", "product_key").agg(
            F.sum("revenue").cast("decimal(20,2)").alias("revenue"), F.sum("quantity").cast("long").alias("units_sold"),
            F.sum("margin").cast("decimal(20,2)").alias("gross_margin")))
    act_product = spark.table(f"{G}.agg_product_performance_monthly").select(*grains["agg_product_performance_monthly"], *measures)
    compare_aggregates(exp_product, act_product, grains["agg_product_performance_monthly"], measures, "Product aggregate")
    assert_zero(act_product.join(dates.select(F.col("date_key").alias("month_date_key"), "full_date"),
                                  "month_date_key", "left_anti"),
                "Product month_date_key references dim_date")
    assert_zero(act_product.join(dates.select(F.col("date_key").alias("month_date_key"), "full_date"),
                                 "month_date_key", "inner").filter(F.dayofmonth("full_date") != 1),
                "Product month_date_key must represent the first calendar day of the month")
    assert_zero(act_product.select("product_key").distinct()
                .join(spark.table(f"{G}.dim_product").select("product_key").distinct(),
                      "product_key", "left_anti"),
                "Product aggregate product_key exists in dim_product")

    exp_store_measures = lines.groupBy("date_key", "store_key").agg(
        F.sum("revenue").cast("decimal(20,2)").alias("revenue"), F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("margin").cast("decimal(20,2)").alias("gross_margin"))
    exp_invoice_count = inv.groupBy("date_key", "store_key").agg(F.countDistinct("invoice_id").cast("long").alias("invoice_count"))
    exp_store = (exp_store_measures.join(exp_invoice_count, ["date_key", "store_key"], "full")
                 .fillna({"revenue": 0, "units_sold": 0, "gross_margin": 0, "invoice_count": 0}))
    act_store = spark.table(f"{G}.agg_store_performance_daily").select(*grains["agg_store_performance_daily"], *measures, "invoice_count")
    compare_aggregates(exp_store, act_store, grains["agg_store_performance_daily"], measures + ["invoice_count"], "Store aggregate")
    assert_zero(act_store.select("store_key").distinct()
                .join(spark.table(f"{G}.dim_store").select("store_key").distinct(), "store_key", "left_anti"),
                "Store aggregate store_key exists in dim_store")

    # DQ invariants retained: source counts, issue counts, and valid+incorrect arithmetic.
    dq_records = spark.table(f"{G}.agg_data_quality_record_daily")
    assert_zero(dq_records.filter(F.col("total_records") != F.col("valid_records") + F.col("incorrect_records")),
                "DQ aggregate total = valid + incorrect")
    dq_issues = spark.table(f"{G}.fact_data_quality_issue")
    dq_agg = spark.table(f"{G}.agg_data_quality_daily")
    source_issue_count, agg_issue_count = dq_issues.count(), dq_agg.agg(F.sum("dq_issue_count")).first()[0] or 0
    if source_issue_count != agg_issue_count:
        raise RuntimeError(f"FAIL: DQ issue instances: fact={source_issue_count:,}, aggregate sum={agg_issue_count:,}")
    print(f"PASS: DQ issue instance totals: {source_issue_count:,}")

    # Ensure the DQ snapshot is still based on all Silver clean + quarantine records by source file.
    for entity in ["customers", "products", "stores", "invoices", "invoice_items"]:
        clean = spark.table(f"{S}.{entity}")
        qname = f"{S}.quarantined_{entity}"
        q = spark.table(qname) if spark.catalog.tableExists(qname) else None
        expected = clean.count() + (q.count() if q is not None else 0)
        actual = dq_records.filter(F.col("source_table") == entity).agg(F.sum("total_records")).first()[0] or 0
        if expected != actual:
            raise RuntimeError(f"FAIL: DQ record snapshot for {entity}: Silver valid+quarantine={expected:,}, aggregate={actual:,}")
        print(f"PASS: DQ population {entity}: {actual:,}")

    print("ALL CHECKS PASSED: base fact integrity, SCD2, FK mapping, all 3 commercial aggregates and both DQ aggregates.")


if __name__ == "__main__":
    main()
