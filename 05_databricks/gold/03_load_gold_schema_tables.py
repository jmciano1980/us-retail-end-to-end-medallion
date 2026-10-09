#!/usr/bin/env python
"""Load the Gold base model from workspace.silver.

Run as a Databricks Python script task, not a notebook.
Requires 01_create_bronze_schema_tables.sql to have run successfully.
This is a single-writer job; set maximum concurrent runs to 1.
"""
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
from datetime import datetime

S = "workspace.silver"
G = "workspace.gold"
OPEN_END = "9999-12-30 23:59:59.999999"

def exists(name):
    return spark.catalog.tableExists(name)

def max_key(table, key):
    row = spark.table(table).agg(F.max(key).alias("m")).first()
    return int(row["m"] or 0)

def assign_keys(df, target, key):
    start = max_key(target, key) + 1
    return df.withColumn(key, F.row_number().over(Window.orderBy(*df.columns)).cast("long") + F.lit(start - 1))

def hash_cols(cols):
    return F.sha2(F.concat_ws("§", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in cols]), 256)

def seed_default(table, key, values, schema):
    if spark.table(table).filter(F.col(key) == -1).limit(1).count() == 0:
        spark.createDataFrame([values], schema).write.format("delta").mode("append").saveAsTable(table)

def seed_defaults():
    now = datetime.utcnow()
    seed_default(f"{G}.dim_geography", "geography_key",
        (-1, "Unknown", "Unknown", "Unknown", "Unknown", now, now),
        "geography_key long, region string, state string, city string, zip_code string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_customer", "customer_key",
        (-1, "NO_CUSTOMER", "No customer assigned", "No customer assigned", None, None, None, None, None, -1,
         datetime(1900,1,1), datetime(9999,12,30,23,59,59), True, "DEFAULT", now, now),
        "customer_key long, customer_id string, first_name string, last_name string, email string, phone string, street_address string, loyalty_tier string, join_date date, geography_key long, effective_from timestamp, effective_to timestamp, is_current boolean, attribute_hash string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_store", "store_key",
        (-1, "UNKNOWN_STORE", "Unknown store", None, None, None, -1, datetime(1900,1,1), datetime(9999,12,30,23,59,59), True, "DEFAULT", now, now),
        "store_key long, store_id string, store_name string, manager_name string, opened_date date, square_footage long, geography_key long, effective_from timestamp, effective_to timestamp, is_current boolean, attribute_hash string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_category", "category_key", (-1, "Unknown", now, now),
        "category_key long, category_name string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_subcategory", "subcategory_key", (-1, "Unknown", -1, now, now),
        "subcategory_key long, subcategory_name string, category_key long, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_product", "product_key",
        (-1, "UNKNOWN_PRODUCT", None, "Unknown product", -1, None, None, None, None,
         datetime(1900,1,1), datetime(9999,12,30,23,59,59), True, "DEFAULT", now, now),
        "product_key long, product_id string, sku string, product_name string, subcategory_key long, brand string, unit_price decimal(18,2), cost decimal(18,2), is_active boolean, effective_from timestamp, effective_to timestamp, is_current boolean, attribute_hash string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")

def upsert_simple(src, natural_cols, target, key_col, select_cols):
    src = src.select(*select_cols).dropDuplicates(natural_cols)
    tgt = spark.table(target)
    s = src.alias("s")
    t = tgt.select(*natural_cols).dropDuplicates().alias("t")
    cond = None
    for c in natural_cols:
        x = F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
        cond = x if cond is None else cond & x
    missing = s.join(t, cond, "left_anti")
    if missing.limit(1).count():
        new = assign_keys(missing, target, key_col)
        new = new.withColumn("_insert_datetime_utc", F.current_timestamp()).withColumn("_update_datetime_utc", F.current_timestamp())
        new.select(*spark.table(target).columns).write.format("delta").mode("append").saveAsTable(target)

def scd2_upsert(src, natural_key, tracked, target, key_col, target_cols):
    # Reject ambiguous natural keys instead of silently choosing a row.
    duplicates = src.groupBy(natural_key).count().filter("count > 1")
    if duplicates.limit(1).count():
        raise ValueError(f"Duplicate natural keys in source for {target}; refusing SCD2 load.")
    src = src.withColumn("attribute_hash", hash_cols(tracked))
    current = spark.table(target).filter("is_current = true").alias("t")
    joined = src.alias("s").join(current, F.col(f"s.{natural_key}") == F.col(f"t.{natural_key}"), "left")
    changed = joined.filter(F.col(f"t.{natural_key}").isNull() | (F.col("s.attribute_hash") != F.col("t.attribute_hash")))
    if not changed.limit(1).count():
        return
    close = changed.filter(F.col(f"t.{natural_key}").isNotNull()).select(F.col(f"t.{natural_key}").alias(natural_key)).dropDuplicates()
    if close.limit(1).count():
        DeltaTable.forName(spark, target).alias("t").merge(
            close.alias("c"), f"t.{natural_key}=c.{natural_key} AND t.is_current=true"
        ).whenMatchedUpdate(set={
            "effective_to": "current_timestamp()", "is_current": "false",
            "_update_datetime_utc": "current_timestamp()"
        }).execute()
    inserts = changed.select("s.*").withColumn("effective_from", F.current_timestamp()) \
        .withColumn("effective_to", F.to_timestamp(F.lit(OPEN_END))) \
        .withColumn("is_current", F.lit(True)) \
        .withColumn("_insert_datetime_utc", F.current_timestamp()) \
        .withColumn("_update_datetime_utc", F.current_timestamp())
    inserts = assign_keys(inserts, target, key_col)
    inserts.select(*target_cols).write.format("delta").mode("append").saveAsTable(target)

def load_date_dim():
    bounds = spark.table(f"{S}.invoices").agg(F.min("invoice_date").alias("lo"), F.max("invoice_date").alias("hi")).first()
    if bounds["lo"] is None:
        return
    dates = spark.range(1).select(F.explode(F.sequence(F.lit(bounds["lo"]), F.lit(bounds["hi"]), F.expr("INTERVAL 1 DAY"))).alias("full_date"))
    dates = (dates.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("full_date")).withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE")).withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("month", F.month("full_date")).withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date")).withColumn("year", F.year("full_date"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn("year_quarter", F.concat(F.year("full_date"), F.lit("-Q"), F.quarter("full_date")))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1,7))
        .withColumn("is_month_start", F.dayofmonth("full_date") == 1)
        .withColumn("is_month_end", F.last_day("full_date") == F.col("full_date"))
        .withColumn("_insert_datetime_utc", F.current_timestamp()).withColumn("_update_datetime_utc", F.current_timestamp()))
    missing = dates.join(spark.table(f"{G}.dim_date").select("date_key"), "date_key", "left_anti")
    missing.write.format("delta").mode("append").saveAsTable(f"{G}.dim_date")

def load_geography():
    stores = spark.table(f"{S}.stores").select("region","state","city","zip_code")
    # Silver customers do not carry region. Do not fabricate one for customer geography.
    customers = spark.table(f"{S}.customers").select(
        F.lit(None).cast("string").alias("region"), "state","city","zip_code")
    src = stores.unionByName(customers).dropDuplicates()
    tgt = spark.table(f"{G}.dim_geography")
    src_keys = src.select(*[F.coalesce(F.col(c).cast("string"), F.lit("∅")).alias(c) for c in src.columns]).dropDuplicates()
    tgt_keys = tgt.select(*[F.coalesce(F.col(c).cast("string"), F.lit("∅")).alias(c) for c in src.columns]).dropDuplicates()
    missing = src_keys.join(tgt_keys, src.columns, "left_anti")
    missing = missing.select(*[F.when(F.col(c)=="∅", None).otherwise(F.col(c)).alias(c) for c in src.columns])
    if missing.limit(1).count():
        out = assign_keys(missing, f"{G}.dim_geography", "geography_key")
        out.withColumn("_insert_datetime_utc",F.current_timestamp()).withColumn("_update_datetime_utc",F.current_timestamp()) \
            .select(*spark.table(f"{G}.dim_geography").columns).write.format("delta").mode("append").saveAsTable(f"{G}.dim_geography")

def load_dimensions():
    load_geography()
    seed_defaults()
    geo = spark.table(f"{G}.dim_geography")
    store_src = (spark.table(f"{S}.stores").alias("s").join(geo.alias("g"),
        F.col("s.region").eqNullSafe(F.col("g.region")) & F.col("s.state").eqNullSafe(F.col("g.state")) &
        F.col("s.city").eqNullSafe(F.col("g.city")) & F.col("s.zip_code").eqNullSafe(F.col("g.zip_code")), "left")
        .select("s.store_id","s.store_name","s.manager_name","s.opened_date","s.square_footage",
                F.coalesce(F.col("g.geography_key"),F.lit(-1)).alias("geography_key")))
    customer_src = (spark.table(f"{S}.customers").alias("c").join(geo.alias("g"),
        F.lit(None).cast("string").eqNullSafe(F.col("g.region")) & F.col("c.state").eqNullSafe(F.col("g.state")) &
        F.col("c.city").eqNullSafe(F.col("g.city")) & F.col("c.zip_code").eqNullSafe(F.col("g.zip_code")), "left")
        .select("c.customer_id","c.first_name","c.last_name","c.email","c.phone","c.street_address",
                "c.loyalty_tier","c.join_date",F.coalesce(F.col("g.geography_key"),F.lit(-1)).alias("geography_key")))
    scd2_upsert(customer_src,"customer_id",
        ["first_name","last_name","email","phone","street_address","loyalty_tier","join_date","geography_key"],
        f"{G}.dim_customer","customer_key",spark.table(f"{G}.dim_customer").columns)
    scd2_upsert(store_src,"store_id",
        ["store_name","manager_name","opened_date","square_footage","geography_key"],
        f"{G}.dim_store","store_key",spark.table(f"{G}.dim_store").columns)

    products = spark.table(f"{S}.products")
    upsert_simple(products.select(F.col("category").alias("category_name")).dropDuplicates(),
        ["category_name"],f"{G}.dim_category","category_key",["category_name"])
    cats = spark.table(f"{G}.dim_category")
    sub_src = products.select(F.col("category").alias("category_name"),F.col("subcategory").alias("subcategory_name")).dropDuplicates() \
        .join(cats,"category_name","inner").select("subcategory_name","category_key")
    upsert_simple(sub_src,["category_key","subcategory_name"],f"{G}.dim_subcategory","subcategory_key",["subcategory_name","category_key"])
    subs = spark.table(f"{G}.dim_subcategory")
    product_src = products.alias("p").join(cats.alias("c"),F.col("p.category")==F.col("c.category_name"),"inner") \
        .join(subs.alias("s"),(F.col("c.category_key")==F.col("s.category_key")) & (F.col("p.subcategory")==F.col("s.subcategory_name")),"inner") \
        .select("p.product_id","p.sku","p.product_name",F.col("s.subcategory_key"),"p.brand","p.unit_price","p.cost","p.is_active")
    scd2_upsert(product_src,"product_id",
        ["sku","product_name","subcategory_key","brand","unit_price","cost","is_active"],
        f"{G}.dim_product","product_key",spark.table(f"{G}.dim_product").columns)
    load_date_dim()

def load_facts():
    invoices = spark.table(f"{S}.invoices").alias("i")
    dates = spark.table(f"{G}.dim_date").select("date_key","full_date").alias("d")
    stores = spark.table(f"{G}.dim_store").alias("s")
    customers = spark.table(f"{G}.dim_customer").alias("c")
    inv = invoices.join(dates,F.col("i.invoice_date")==F.col("d.full_date"),"left") \
        .join(stores,(F.col("i.store_id")==F.col("s.store_id")) &
            (F.col("i.invoice_date").cast("timestamp")>=F.col("s.effective_from")) &
            (F.col("i.invoice_date").cast("timestamp")<F.col("s.effective_to")),"left") \
        .join(customers,(F.col("i.customer_id")==F.col("c.customer_id")) &
            (F.col("i.invoice_date").cast("timestamp")>=F.col("c.effective_from")) &
            (F.col("i.invoice_date").cast("timestamp")<F.col("c.effective_to")),"left") \
        .select("i.invoice_id","d.date_key",F.coalesce("c.customer_key",F.lit(-1)).alias("customer_key"),
            F.coalesce("s.store_key",F.lit(-1)).alias("store_key"),"i.payment_method",
            F.current_timestamp().alias("_insert_datetime_utc"),F.current_timestamp().alias("_update_datetime_utc"))
    if inv.filter(F.col("date_key").isNull()).limit(1).count():
        raise ValueError("Some Silver invoices have no matching dim_date row.")
    inv = inv.dropDuplicates(["invoice_id"])
    inv_target = DeltaTable.forName(spark,f"{G}.fact_invoice")
    existing = spark.table(f"{G}.fact_invoice").select("invoice_id","invoice_key")
    new = inv.join(existing.select("invoice_id"),"invoice_id","left_anti")
    if new.limit(1).count():
        new = assign_keys(new,f"{G}.fact_invoice","invoice_key")
        inv = inv.join(existing,"invoice_id","left").join(new.select("invoice_id","invoice_key").withColumnRenamed("invoice_key","new_key"),"invoice_id","left") \
            .withColumn("invoice_key",F.coalesce("invoice_key","new_key")).drop("new_key")
    else:
        inv = inv.join(existing,"invoice_id","inner")
    (inv_target.alias("t").merge(inv.alias("s"),"t.invoice_id=s.invoice_id")
        .whenMatchedUpdate(set={"date_key":"s.date_key","customer_key":"s.customer_key","store_key":"s.store_key",
            "payment_method":"s.payment_method","_update_datetime_utc":"current_timestamp()"})
        .whenNotMatchedInsertAll().execute())

    items = spark.table(f"{S}.invoice_items").alias("ii")
    im = spark.table(f"{G}.fact_invoice").select("invoice_id","invoice_key","date_key").alias("i")
    d = spark.table(f"{G}.dim_date").select("date_key","full_date").alias("d")
    p = spark.table(f"{G}.dim_product").alias("p")
    item = items.join(im,F.col("ii.invoice_id")==F.col("i.invoice_id"),"inner") \
        .join(d,F.col("i.date_key")==F.col("d.date_key"),"inner") \
        .join(p,(F.col("ii.product_id")==F.col("p.product_id")) &
            (F.col("d.full_date").cast("timestamp")>=F.col("p.effective_from")) &
            (F.col("d.full_date").cast("timestamp")<F.col("p.effective_to")),"left") \
        .select("ii.invoice_id","ii.line_item","i.invoice_key",F.coalesce("p.product_key",F.lit(-1)).alias("product_key"),
            "ii.quantity","ii.unit_price","ii.discount",F.current_timestamp().alias("_insert_datetime_utc"),
            F.current_timestamp().alias("_update_datetime_utc")).dropDuplicates(["invoice_id","line_item"])
    existing_i = spark.table(f"{G}.fact_invoice_item").select("invoice_id","line_item","invoice_item_key")
    new_i = item.join(existing_i.select("invoice_id","line_item"),["invoice_id","line_item"],"left_anti")
    if new_i.limit(1).count():
        new_i = assign_keys(new_i,f"{G}.fact_invoice_item","invoice_item_key")
        item = item.join(existing_i,["invoice_id","line_item"],"left").join(
            new_i.select("invoice_id","line_item","invoice_item_key").withColumnRenamed("invoice_item_key","new_key"),
            ["invoice_id","line_item"],"left").withColumn("invoice_item_key",F.coalesce("invoice_item_key","new_key")).drop("new_key")
    else:
        item = item.join(existing_i,["invoice_id","line_item"],"inner")
    target = DeltaTable.forName(spark,f"{G}.fact_invoice_item")
    (target.alias("t").merge(item.alias("s"),"t.invoice_id=s.invoice_id AND t.line_item=s.line_item")
        .whenMatchedUpdate(set={"invoice_key":"s.invoice_key","product_key":"s.product_key","quantity":"s.quantity",
            "unit_price":"s.unit_price","discount":"s.discount","_update_datetime_utc":"current_timestamp()"})
        .whenNotMatchedInsertAll().execute())


def load_dq_and_quarantine():
    """Load quarantine details and the DQ rule/issue model from Silver."""
    source_map = {
        "customers": "quarantined_customers",
        "products": "quarantined_products",
        "stores": "quarantined_stores",
        "invoices": "quarantined_invoices",
        "invoice_items": "quarantined_invoice_items",
    }
    frames = []
    for source_name, qtable in source_map.items():
        source = f"{S}.{qtable}"
        if not exists(source):
            continue
        q = spark.table(source)
        cols = set(q.columns)
        def field(name, fallback=None, dtype="string"):
            return F.col(name).cast(dtype) if name in cols else F.lit(fallback).cast(dtype)
        payload = F.to_json(F.struct(*[F.col(c) for c in q.columns]))
        normalized = q.select(
            F.lit(source_name).alias("source_table"),
            field("_source_row_id", None).alias("source_row_id"),
            field("_source_file_name", None).alias("source_file_name"),
            field("_dq_error_code", "UNSPECIFIED_DQ_RULE").alias("dq_error_code"),
            field("_dq_error_description", "Quarantined record").alias("dq_error_description"),
            field("_dq_status", "Quarantined").alias("dq_status"),
            field("_quarantine_datetime_utc", None, "timestamp").alias("quarantine_datetime"),
            payload.alias("source_payload_json"))
        frames.append((source_name, qtable, normalized))

    if not frames:
        print("No Silver quarantine tables found; DQ issue/quarantine load skipped.")
        return

    # Merge each quarantine table on its stable source-row identity plus rule.
    all_issues = None
    all_rules = None
    for source_name, qtable, q in frames:
        rule = q.select("source_table", "dq_error_code").dropDuplicates() \
            .withColumn("dq_rule_name", F.col("dq_error_code")) \
            .withColumn("dq_rule_description", F.lit("Rule metadata derived from Silver quarantine")) \
            .withColumn("severity", F.lit("Error"))
        all_rules = rule if all_rules is None else all_rules.unionByName(rule)
        issue = q.select("source_table","source_row_id","source_file_name","dq_error_code",
                         "dq_error_description","dq_status","quarantine_datetime")
        all_issues = issue if all_issues is None else all_issues.unionByName(issue)

        target_name = f"{G}.{qtable}"
        gold_q = q.select("source_row_id","source_file_name",
            F.col("quarantine_datetime").alias("quarantine_datetime_utc"),
            F.col("dq_status"),F.col("dq_error_code"),F.col("dq_error_description"),
            "source_payload_json").withColumn("_insert_datetime_utc",F.current_timestamp()) \
            .withColumn("_update_datetime_utc",F.current_timestamp())
        target = DeltaTable.forName(spark, target_name)
        (target.alias("t").merge(gold_q.alias("s"),
            "t.source_row_id=s.source_row_id AND t.dq_error_code=s.dq_error_code")
            .whenMatchedUpdate(set={"source_file_name":"s.source_file_name",
                "quarantine_datetime_utc":"s.quarantine_datetime_utc","dq_status":"s.dq_status",
                "dq_error_description":"s.dq_error_description","source_payload_json":"s.source_payload_json",
                "_update_datetime_utc":"current_timestamp()"})
            .whenNotMatchedInsertAll().execute())

    all_rules = all_rules.dropDuplicates(["source_table","dq_error_code"])
    target_rules = spark.table(f"{G}.dim_data_quality_rule")
    missing_rules = all_rules.join(target_rules.select("source_table","dq_error_code"),
                                   ["source_table","dq_error_code"],"left_anti")
    if missing_rules.limit(1).count():
        missing_rules = assign_keys(missing_rules, f"{G}.dim_data_quality_rule", "dq_rule_key") \
            .withColumn("_insert_datetime_utc",F.current_timestamp()) \
            .withColumn("_update_datetime_utc",F.current_timestamp())
        missing_rules.select(*target_rules.columns).write.format("delta").mode("append").saveAsTable(f"{G}.dim_data_quality_rule")

    rule_map = spark.table(f"{G}.dim_data_quality_rule").select("source_table","dq_error_code","dq_rule_key")
    issues = all_issues.join(rule_map,["source_table","dq_error_code"],"left") \
        .withColumn("_quarantine_datetime_utc",F.col("quarantine_datetime")) \
        .withColumn("_insert_datetime_utc",F.current_timestamp()) \
        .withColumn("_update_datetime_utc",F.current_timestamp())
    existing_issues = spark.table(f"{G}.fact_data_quality_issue").select(
        "source_table","source_row_id","dq_error_code","dq_issue_key")
    new_issues = issues.join(existing_issues.select("source_table","source_row_id","dq_error_code"),
                             ["source_table","source_row_id","dq_error_code"],"left_anti")
    if new_issues.limit(1).count():
        new_issues = assign_keys(new_issues,f"{G}.fact_data_quality_issue","dq_issue_key")
        issues = issues.join(existing_issues,["source_table","source_row_id","dq_error_code"],"left") \
            .join(new_issues.select("source_table","source_row_id","dq_error_code","dq_issue_key")
                  .withColumnRenamed("dq_issue_key","new_key"),
                  ["source_table","source_row_id","dq_error_code"],"left") \
            .withColumn("dq_issue_key",F.coalesce("dq_issue_key","new_key")).drop("new_key")
    else:
        issues = issues.join(existing_issues,["source_table","source_row_id","dq_error_code"],"inner")
    # Project the merge source to the exact target schema. The source data uses
    # dq_status/quarantine_datetime, while the Gold fact uses _dq_status and
    # _quarantine_datetime_utc. InsertAll requires target column names to resolve.
    issue_merge_source = issues.select(
        "dq_issue_key",
        "source_table",
        "source_row_id",
        "source_file_name",
        "dq_rule_key",
        "dq_error_code",
        "dq_error_description",
        F.col("dq_status").alias("_dq_status"),
        F.col("_quarantine_datetime_utc"),
        F.col("_insert_datetime_utc"),
        F.col("_update_datetime_utc"),
    )

    target = DeltaTable.forName(spark, f"{G}.fact_data_quality_issue")
    (target.alias("t").merge(
        issue_merge_source.alias("s"),
        "t.source_table = s.source_table AND "
        "t.source_row_id = s.source_row_id AND "
        "t.dq_error_code = s.dq_error_code"
    )
        .whenMatchedUpdate(set={
            "source_file_name": "s.source_file_name",
            "dq_rule_key": "s.dq_rule_key",
            "dq_error_description": "s.dq_error_description",
            "_dq_status": "s._dq_status",
            "_quarantine_datetime_utc": "s._quarantine_datetime_utc",
            "_update_datetime_utc": "current_timestamp()",
        })
        .whenNotMatchedInsertAll()
        .execute())


def main():
    spark.conf.set("spark.sql.session.timeZone","UTC")
    load_dimensions()
    load_facts()
    load_dq_and_quarantine()
    print("Gold base dimensions, facts, DQ issues and quarantine details loaded.")

if __name__ == "__main__":
    main()
