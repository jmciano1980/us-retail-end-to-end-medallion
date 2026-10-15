#!/usr/bin/env python
"""
US Retail Gold base loader — integrity-first replacement.

Run as a Databricks Python script task (not pasted into a notebook).
Prerequisites: 01_create_gold_schema_tables.sql and 02_create_gold_agg_tables.sql.
Single writer only; set maximum concurrent runs = 1.

Key protections:
- Initial SCD2 versions begin no later than the earliest known transaction date for
  each entity (Silver does not provide complete attribute-change history).
- Every non-null customer/store/product natural key must resolve to exactly one
  dimension version at the transaction date. Missing or ambiguous matches fail
  the job; they are NEVER silently replaced by -1.
- Only a NULL Silver customer_id maps to customer_key = -1.
- Natural-key duplicates and fact-grain duplicates fail instead of being dropped.
"""
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

S = "workspace.silver"
G = "workspace.gold"
# Safe upper-bound sentinel: avoid end-of-day fractions that may overflow during Spark Connect conversion.
OPEN_END = "9998-12-31 00:00:00"
DEFAULT_START = "1900-01-01 00:00:00"

def table_exists(name):
    return spark.catalog.tableExists(name)

def require_tables(names):
    missing = [n for n in names if not table_exists(n)]
    if missing:
        raise RuntimeError("Required tables are missing: " + ", ".join(missing))

def assert_no_rows(df, message, sample_cols=None):
    sample = df.limit(10)
    if sample.count():
        cols = sample_cols or sample.columns
        rows = [r.asDict(recursive=True) for r in sample.select(*cols).collect()]
        raise RuntimeError(f"{message}. Sample (up to 10): {rows}")

def assert_unique(df, cols, label):
    dup = df.groupBy(*cols).count().filter(F.col("count") > 1)
    assert_no_rows(dup, f"Duplicate keys found for {label}", cols + ["count"])

def max_key(table, key):
    row = spark.table(table).agg(F.max(F.col(key)).alias("m")).first()
    return int(row["m"] or 0)

def assign_keys(df, target, key_col, order_cols):
    if not df.limit(1).count():
        return df.withColumn(key_col, F.lit(None).cast("long"))
    start = max_key(target, key_col) + 1
    w = Window.orderBy(*[F.col(c).asc_nulls_first() for c in order_cols])
    return df.withColumn(key_col, (F.row_number().over(w) + F.lit(start - 1)).cast("long"))

def hash_cols(cols):
    # Explicit null marker; stable across runs and independent of metadata.
    return F.sha2(F.to_json(F.struct(*[F.col(c).alias(c) for c in cols])), 256)

def seed_default(table, key_col, values, schema):
    if spark.table(table).filter(F.col(key_col) == -1).limit(1).count() == 0:
        spark.createDataFrame([values], schema=schema).write.format("delta").mode("append").saveAsTable(table)

def seed_defaults():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    seed_default(f"{G}.dim_geography", "geography_key",
        (-1, "Unknown", "Unknown", "Unknown", "Unknown", now, now),
        "geography_key long, region string, state string, city string, zip_code string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_customer", "customer_key",
        (-1, "NO_CUSTOMER", "No customer assigned", "No customer assigned", None, None, None, None, None, -1,
         datetime(1900,1,1), datetime(9998,12,31,0,0,0), True, "DEFAULT", now, now),
        "customer_key long, customer_id string, first_name string, last_name string, email string, phone string, street_address string, loyalty_tier string, join_date date, geography_key long, effective_from timestamp, effective_to timestamp, is_current boolean, attribute_hash string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_store", "store_key",
        (-1, "UNKNOWN_STORE", "Unknown store", None, None, None, -1, datetime(1900,1,1), datetime(9998,12,31,0,0,0), True, "DEFAULT", now, now),
        "store_key long, store_id string, store_name string, manager_name string, opened_date date, square_footage long, geography_key long, effective_from timestamp, effective_to timestamp, is_current boolean, attribute_hash string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_category", "category_key", (-1, "Unknown", now, now),
        "category_key long, category_name string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_subcategory", "subcategory_key", (-1, "Unknown", -1, now, now),
        "subcategory_key long, subcategory_name string, category_key long, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")
    seed_default(f"{G}.dim_product", "product_key",
        (-1, "UNKNOWN_PRODUCT", None, "Unknown product", -1, None, None, None, None,
         datetime(1900,1,1), datetime(9998,12,31,0,0,0), True, "DEFAULT", now, now),
        "product_key long, product_id string, sku string, product_name string, subcategory_key long, brand string, unit_price decimal(18,2), cost decimal(18,2), is_active boolean, effective_from timestamp, effective_to timestamp, is_current boolean, attribute_hash string, _insert_datetime_utc timestamp, _update_datetime_utc timestamp")

def upsert_simple(src, natural_cols, target, key_col, select_cols):
    src = src.select(*select_cols)
    assert_unique(src, natural_cols, f"{target} source natural key")
    tgt = spark.table(target)
    tkeys = tgt.select(*natural_cols).dropDuplicates()
    cond = None
    for c in natural_cols:
        eq = F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
        cond = eq if cond is None else cond & eq
    missing = src.alias("s").join(tkeys.alias("t"), cond, "left_anti")
    if missing.limit(1).count():
        new = assign_keys(missing, target, key_col, natural_cols)
        new = (new.withColumn("_insert_datetime_utc", F.current_timestamp())
                  .withColumn("_update_datetime_utc", F.current_timestamp()))
        new.select(*spark.table(target).columns).write.format("delta").mode("append").saveAsTable(target)

def scd2_upsert(src, natural_key, tracked, target, key_col, initial_start_col):
    """
    For a brand-new entity, use its earliest known transaction date as the start
    of its initial version. For a changed existing entity, use a single UTC load
    timestamp for both closing the prior version and opening the new version.
    """
    src = src.withColumn(natural_key, F.col(natural_key).cast("string"))
    assert_no_rows(src.filter(F.col(natural_key).isNull()),
                   f"NULL natural key in source for {target}", [natural_key])
    assert_unique(src, [natural_key], f"{target} source")
    src = src.withColumn("attribute_hash", hash_cols(tracked))
    target_df = spark.table(target)
    current = target_df.filter(F.col("is_current") == True).alias("t")
    joined = src.alias("s").join(current,
        F.col(f"s.{natural_key}") == F.col(f"t.{natural_key}"), "left")
    changed = joined.filter(
        F.col(f"t.{natural_key}").isNull() |
        (~F.col("s.attribute_hash").eqNullSafe(F.col("t.attribute_hash")))
    )
    if not changed.limit(1).count():
        return
    now = F.current_timestamp()
    close_keys = (changed.filter(F.col(f"t.{natural_key}").isNotNull())
        .select(F.col(f"t.{natural_key}").alias(natural_key)).dropDuplicates())
    if close_keys.limit(1).count():
        (DeltaTable.forName(spark, target).alias("t")
         .merge(close_keys.alias("c"),
                f"t.{natural_key}=c.{natural_key} AND t.is_current=true")
         .whenMatchedUpdate(set={
             "effective_to": now,
             "is_current": F.lit(False),
             "_update_datetime_utc": now
         }).execute())
    # New entities start at earliest known transaction date. Changed entities
    # start at the same timestamp used to close their previous version.
    inserts = changed.select("s.*", F.col(f"t.{natural_key}").alias("_existing_nk"))
    inserts = inserts.withColumn(
        "effective_from",
        F.when(F.col("_existing_nk").isNull(),
               F.coalesce(F.col(initial_start_col).cast("timestamp"),
                          F.to_timestamp(F.lit(DEFAULT_START))))
         .otherwise(now)
    ).withColumn("effective_to", F.to_timestamp(F.lit(OPEN_END))) \
     .withColumn("is_current", F.lit(True)) \
     .withColumn("_insert_datetime_utc", F.current_timestamp()) \
     .withColumn("_update_datetime_utc", F.current_timestamp()) \
     .drop("_existing_nk", initial_start_col)
    inserts = assign_keys(inserts, target, key_col, [natural_key])
    inserts.select(*target_df.columns).write.format("delta").mode("append").saveAsTable(target)

def load_date_dim():
    bounds = spark.table(f"{S}.invoices").agg(
        F.min("invoice_date").alias("lo"), F.max("invoice_date").alias("hi")).first()
    if bounds["lo"] is None:
        raise RuntimeError("Silver invoices is empty; refusing to build an empty date dimension.")
    dates = spark.range(1).select(F.explode(
        F.sequence(F.lit(bounds["lo"]), F.lit(bounds["hi"]), F.expr("INTERVAL 1 DAY"))
    ).alias("full_date"))
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
        .withColumn("_insert_datetime_utc", F.current_timestamp())
        .withColumn("_update_datetime_utc", F.current_timestamp()))
    missing = dates.join(spark.table(f"{G}.dim_date").select("date_key"), "date_key", "left_anti")
    if missing.limit(1).count():
        missing.write.format("delta").mode("append").saveAsTable(f"{G}.dim_date")

def load_geography():
    stores = spark.table(f"{S}.stores").select("region","state","city","zip_code")
    customers = spark.table(f"{S}.customers").select(
        F.lit(None).cast("string").alias("region"), "state","city","zip_code")
    src = stores.unionByName(customers).dropDuplicates()
    tgt = spark.table(f"{G}.dim_geography")
    null_marker = "\u0001NULL\u0001"
    src_norm = src.select(*[F.coalesce(F.col(c).cast("string"), F.lit(null_marker)).alias(c) for c in src.columns]).dropDuplicates()
    tgt_norm = tgt.select(*[F.coalesce(F.col(c).cast("string"), F.lit(null_marker)).alias(c) for c in src.columns]).dropDuplicates()
    missing = src_norm.join(tgt_norm, src.columns, "left_anti")
    missing = missing.select(*[F.when(F.col(c)==null_marker, F.lit(None).cast("string")).otherwise(F.col(c)).alias(c) for c in src.columns])
    if missing.limit(1).count():
        new = assign_keys(missing, f"{G}.dim_geography", "geography_key", src.columns)
        (new.withColumn("_insert_datetime_utc",F.current_timestamp())
            .withColumn("_update_datetime_utc",F.current_timestamp())
            .select(*spark.table(f"{G}.dim_geography").columns)
            .write.format("delta").mode("append").saveAsTable(f"{G}.dim_geography"))

def load_dimensions():
    load_geography()
    seed_defaults()
    geo = spark.table(f"{G}.dim_geography")
    store_min = (spark.table(f"{S}.invoices").groupBy("store_id")
        .agg(F.min(F.col("invoice_date").cast("timestamp")).alias("_initial_effective_from")))
    store_src = (spark.table(f"{S}.stores").alias("s")
        .join(geo.alias("g"),
          F.col("s.region").eqNullSafe(F.col("g.region")) &
          F.col("s.state").eqNullSafe(F.col("g.state")) &
          F.col("s.city").eqNullSafe(F.col("g.city")) &
          F.col("s.zip_code").eqNullSafe(F.col("g.zip_code")), "left")
        .join(store_min.alias("m"), F.col("s.store_id").cast("string")==F.col("m.store_id").cast("string"), "left")
        .select(F.col("s.store_id").cast("string").alias("store_id"), "s.store_name","s.manager_name",
          "s.opened_date","s.square_footage",
          F.coalesce(F.col("g.geography_key"),F.lit(-1)).cast("long").alias("geography_key"),
          F.col("m._initial_effective_from")))
    customer_min = (spark.table(f"{S}.invoices").filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id").agg(F.min(F.col("invoice_date").cast("timestamp")).alias("_initial_effective_from")))
    customer_src = (spark.table(f"{S}.customers").alias("c")
        .join(geo.alias("g"),
          F.lit(None).cast("string").eqNullSafe(F.col("g.region")) &
          F.col("c.state").eqNullSafe(F.col("g.state")) &
          F.col("c.city").eqNullSafe(F.col("g.city")) &
          F.col("c.zip_code").eqNullSafe(F.col("g.zip_code")), "left")
        .join(customer_min.alias("m"), F.col("c.customer_id").cast("string")==F.col("m.customer_id").cast("string"), "left")
        .select(F.col("c.customer_id").cast("string").alias("customer_id"), "c.first_name","c.last_name",
          "c.email","c.phone","c.street_address","c.loyalty_tier","c.join_date",
          F.coalesce(F.col("g.geography_key"),F.lit(-1)).cast("long").alias("geography_key"),
          F.col("m._initial_effective_from")))
    scd2_upsert(customer_src,"customer_id",
        ["first_name","last_name","email","phone","street_address","loyalty_tier","join_date","geography_key"],
        f"{G}.dim_customer","customer_key","_initial_effective_from")
    scd2_upsert(store_src,"store_id",
        ["store_name","manager_name","opened_date","square_footage","geography_key"],
        f"{G}.dim_store","store_key","_initial_effective_from")

    products = spark.table(f"{S}.products")
    upsert_simple(products.select(F.col("category").alias("category_name")).dropDuplicates(),
        ["category_name"],f"{G}.dim_category","category_key",["category_name"])
    cats = spark.table(f"{G}.dim_category")
    sub_src = (products.select(F.col("category").alias("category_name"),
            F.col("subcategory").alias("subcategory_name")).dropDuplicates()
        .join(cats,"category_name","inner").select("subcategory_name","category_key"))
    assert_unique(sub_src, ["category_key","subcategory_name"], "subcategory source")
    upsert_simple(sub_src,["category_key","subcategory_name"],f"{G}.dim_subcategory","subcategory_key",
        ["subcategory_name","category_key"])
    subs = spark.table(f"{G}.dim_subcategory")
    product_min = (spark.table(f"{S}.invoice_items").alias("ii")
        .join(spark.table(f"{S}.invoices").select("invoice_id","invoice_date").alias("i"),
              F.col("ii.invoice_id")==F.col("i.invoice_id"),"inner")
        .groupBy("product_id").agg(F.min(F.col("invoice_date").cast("timestamp")).alias("_initial_effective_from")))
    product_src = (products.alias("p")
        .join(cats.alias("c"),F.col("p.category")==F.col("c.category_name"),"inner")
        .join(subs.alias("s"),(F.col("c.category_key")==F.col("s.category_key")) &
              (F.col("p.subcategory")==F.col("s.subcategory_name")),"inner")
        .join(product_min.alias("m"),F.col("p.product_id").cast("string")==F.col("m.product_id").cast("string"),"left")
        .select(F.col("p.product_id").cast("string").alias("product_id"),"p.sku","p.product_name",
          F.col("s.subcategory_key").cast("long").alias("subcategory_key"),"p.brand","p.unit_price","p.cost","p.is_active",
          F.col("m._initial_effective_from")))
    scd2_upsert(product_src,"product_id",
        ["sku","product_name","subcategory_key","brand","unit_price","cost","is_active"],
        f"{G}.dim_product","product_key","_initial_effective_from")
    load_date_dim()

def assert_temporal_cardinality(facts, dim, natural_col, date_col, key_col, label, row_id_cols):
    # Count matches per source business row, not per natural-key/date combination:
    # multiple invoice lines may legitimately contain the same product.
    f = facts.alias("f")
    d = dim.alias("d")
    cond = (F.col(f"f.{natural_col}").cast("string")==F.col(f"d.{natural_col}").cast("string")) & \
           (F.col(f"f.{date_col}").cast("timestamp") >= F.col("d.effective_from")) & \
           (F.col(f"f.{date_col}").cast("timestamp") < F.col("d.effective_to"))
    ids = [F.col(f"f.{c}").alias(c) for c in row_id_cols]
    matched = f.join(d, cond, "left").groupBy(*ids).agg(
        F.count(F.col(f"d.{key_col}")).alias("_match_count"))
    assert_no_rows(matched.filter(F.col("_match_count") != 1),
        f"{label}: source row must match exactly one SCD2 row", matched.columns)

def load_facts():
    invoices = spark.table(f"{S}.invoices")
    items = spark.table(f"{S}.invoice_items")
    assert_unique(invoices, ["invoice_id"], "Silver invoices invoice_id")
    assert_unique(items, ["invoice_id","line_item"], "Silver invoice_items (invoice_id,line_item)")
    assert_no_rows(invoices.filter(F.col("invoice_id").isNull()), "NULL invoice_id in Silver invoices")
    assert_no_rows(items.filter(F.col("invoice_id").isNull() | F.col("line_item").isNull()), "NULL invoice line business key")

    # Preflight all non-null natural-key temporal lookups BEFORE writing facts.
    assert_temporal_cardinality(invoices.filter(F.col("customer_id").isNotNull()),
        spark.table(f"{G}.dim_customer"), "customer_id", "invoice_date", "customer_key", "Customer SCD2 lookup", ["invoice_id"])
    assert_temporal_cardinality(invoices.filter(F.col("store_id").isNotNull()),
        spark.table(f"{G}.dim_store"), "store_id", "invoice_date", "store_key", "Store SCD2 lookup", ["invoice_id"])
    item_context = items.join(invoices.select("invoice_id","invoice_date"),
                              "invoice_id","left")
    assert_no_rows(item_context.filter(F.col("invoice_date").isNull()), "Invoice item has no Silver invoice header")
    assert_temporal_cardinality(item_context.filter(F.col("product_id").isNotNull()),
        spark.table(f"{G}.dim_product"), "product_id", "invoice_date", "product_key", "Product SCD2 lookup", ["invoice_id", "line_item"])

    dates = spark.table(f"{G}.dim_date").select("date_key","full_date")
    stores = spark.table(f"{G}.dim_store").select("store_id","store_key","effective_from","effective_to")
    customers = spark.table(f"{G}.dim_customer").select("customer_id","customer_key","effective_from","effective_to")
    inv0 = invoices.alias("i")
    d = dates.alias("d")
    st = stores.alias("s")
    cu = customers.alias("c")
    inv = (inv0.join(d,F.col("i.invoice_date")==F.col("d.full_date"),"left")
        .join(st,(F.col("i.store_id").cast("string")==F.col("s.store_id")) &
             (F.col("i.invoice_date").cast("timestamp")>=F.col("s.effective_from")) &
             (F.col("i.invoice_date").cast("timestamp")<F.col("s.effective_to")),"left")
        .join(cu,(F.col("i.customer_id").cast("string")==F.col("c.customer_id")) &
             (F.col("i.invoice_date").cast("timestamp")>=F.col("c.effective_from")) &
             (F.col("i.invoice_date").cast("timestamp")<F.col("c.effective_to")),"left")
        .select(F.col("i.invoice_id").cast("string").alias("invoice_id"),
            F.col("d.date_key").alias("date_key"),
            F.when(F.col("i.customer_id").isNull(),F.lit(-1)).otherwise(F.col("c.customer_key")).cast("long").alias("customer_key"),
            F.col("s.store_key").cast("long").alias("store_key"),
            F.col("i.payment_method").alias("payment_method")))
    assert_no_rows(inv.filter(F.col("date_key").isNull()), "Silver invoice has no dim_date match")
    assert_no_rows(inv.filter(F.col("customer_key").isNull()), "Non-null customer_id failed surrogate-key lookup")
    assert_no_rows(inv.filter(F.col("store_key").isNull()), "Non-null store_id failed surrogate-key lookup")
    assert_no_rows(inv.filter(F.col("store_key")==-1), "Store -1 is forbidden for a non-null Silver store_id")
    # Only explicitly NULL customer_id may use -1.
    assert_no_rows(inv.join(invoices.select(F.col("invoice_id").cast("string").alias("invoice_id"),
            "customer_id"),"invoice_id").filter(F.col("customer_id").isNotNull() & (F.col("customer_key")==-1)),
        "Non-null Silver customer_id mapped to customer_key=-1")
    assert_unique(inv, ["invoice_id"], "resolved fact_invoice source")

    fact_target = DeltaTable.forName(spark,f"{G}.fact_invoice")
    existing = spark.table(f"{G}.fact_invoice").select("invoice_id","invoice_key")
    new = inv.join(existing.select("invoice_id"),"invoice_id","left_anti")
    if new.limit(1).count():
        new = assign_keys(new,f"{G}.fact_invoice","invoice_key",["invoice_id"])
        inv = inv.join(existing,"invoice_id","left").join(
            new.select("invoice_id","invoice_key").withColumnRenamed("invoice_key","_new_key"),
            "invoice_id","left").withColumn("invoice_key",F.coalesce("invoice_key","_new_key")).drop("_new_key")
    else:
        inv = inv.join(existing,"invoice_id","inner")
    inv = (inv.withColumn("_insert_datetime_utc",F.current_timestamp())
        .withColumn("_update_datetime_utc",F.current_timestamp())
        .select(*spark.table(f"{G}.fact_invoice").columns))
    (fact_target.alias("t").merge(inv.alias("s"),"t.invoice_id=s.invoice_id")
        .whenMatchedUpdate(set={"date_key":"s.date_key","customer_key":"s.customer_key",
            "store_key":"s.store_key","payment_method":"s.payment_method",
            "_update_datetime_utc":"current_timestamp()"})
        .whenNotMatchedInsertAll().execute())

    # Resolve product version per invoice date; no silent -1 fallback for non-null product IDs.
    invoice_keys = spark.table(f"{G}.fact_invoice").select("invoice_id","invoice_key","date_key")
    d2 = dates.alias("d")
    p = spark.table(f"{G}.dim_product").select("product_id","product_key","effective_from","effective_to").alias("p")
    item_context = items.alias("ii").join(invoices.select("invoice_id","invoice_date").alias("ih"),
        F.col("ii.invoice_id")==F.col("ih.invoice_id"),"inner")
    prod_match = item_context.join(p,
        (F.col("ii.product_id").cast("string")==F.col("p.product_id")) &
        (F.col("ih.invoice_date").cast("timestamp")>=F.col("p.effective_from")) &
        (F.col("ih.invoice_date").cast("timestamp")<F.col("p.effective_to")),"left")
    assert_no_rows(prod_match.filter(F.col("ii.product_id").isNotNull() & F.col("p.product_key").isNull()),
        "Non-null Silver product_id failed SCD2 lookup")
    assert_no_rows(prod_match.filter(F.col("ii.product_id").isNotNull() & (F.col("p.product_key")==-1)),
        "Non-null Silver product_id mapped to product_key=-1")
    # Check product match multiplicity before projecting rows.
    mult = prod_match.groupBy(F.col("ii.invoice_id").alias("invoice_id"),
        F.col("ii.line_item").alias("line_item")).agg(F.count("p.product_key").alias("_matches"))
    assert_no_rows(mult.filter(F.col("_matches") != 1), "Each invoice item must match exactly one product SCD2 version")
    item = (prod_match.join(invoice_keys.alias("ik"),F.col("ii.invoice_id")==F.col("ik.invoice_id"),"inner")
        .select(F.col("ii.invoice_id").cast("string").alias("invoice_id"),
            F.col("ii.line_item").cast("long").alias("line_item"),
            F.col("ik.invoice_key").cast("long").alias("invoice_key"),
            F.col("p.product_key").cast("long").alias("product_key"),
            F.col("ii.quantity").cast("long").alias("quantity"),
            F.col("ii.unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.col("ii.discount").cast("decimal(18,2)").alias("discount")))
    assert_unique(item, ["invoice_id","line_item"], "resolved fact_invoice_item source")
    existing_i = spark.table(f"{G}.fact_invoice_item").select("invoice_id","line_item","invoice_item_key")
    new_i = item.join(existing_i.select("invoice_id","line_item"),["invoice_id","line_item"],"left_anti")
    if new_i.limit(1).count():
        new_i = assign_keys(new_i,f"{G}.fact_invoice_item","invoice_item_key",["invoice_id","line_item"])
        item = item.join(existing_i,["invoice_id","line_item"],"left").join(
            new_i.select("invoice_id","line_item","invoice_item_key").withColumnRenamed("invoice_item_key","_new_key"),
            ["invoice_id","line_item"],"left").withColumn("invoice_item_key",F.coalesce("invoice_item_key","_new_key")).drop("_new_key")
    else:
        item = item.join(existing_i,["invoice_id","line_item"],"inner")
    item = (item.withColumn("_insert_datetime_utc",F.current_timestamp())
        .withColumn("_update_datetime_utc",F.current_timestamp())
        .select(*spark.table(f"{G}.fact_invoice_item").columns))
    target = DeltaTable.forName(spark,f"{G}.fact_invoice_item")
    (target.alias("t").merge(item.alias("s"),"t.invoice_id=s.invoice_id AND t.line_item=s.line_item")
        .whenMatchedUpdate(set={"invoice_key":"s.invoice_key","product_key":"s.product_key",
            "quantity":"s.quantity","unit_price":"s.unit_price","discount":"s.discount",
            "_update_datetime_utc":"current_timestamp()"})
        .whenNotMatchedInsertAll().execute())

def load_dq_and_quarantine():
    source_map = {
        "customers": "quarantined_customers", "products": "quarantined_products",
        "stores": "quarantined_stores", "invoices": "quarantined_invoices",
        "invoice_items": "quarantined_invoice_items",
    }
    issue_frames = []
    rule_frames = []
    for source_name, qtable in source_map.items():
        source = f"{S}.{qtable}"
        if not table_exists(source):
            continue
        q = spark.table(source)
        cols = set(q.columns)
        def field(name, fallback=None, dtype="string"):
            return F.col(name).cast(dtype) if name in cols else F.lit(fallback).cast(dtype)
        payload = F.to_json(F.struct(*[F.col(c) for c in q.columns]))
        norm = q.select(
            F.lit(source_name).alias("source_table"),
            field("_source_row_id").alias("source_row_id"),
            field("_source_file_name").alias("source_file_name"),
            field("_dq_error_code","UNSPECIFIED_DQ_RULE").alias("dq_error_code"),
            field("_dq_error_description","Quarantined record").alias("dq_error_description"),
            field("_dq_status","Quarantined").alias("dq_status"),
            field("_quarantine_datetime_utc",None,"timestamp").alias("quarantine_datetime_utc"),
            payload.alias("source_payload_json"))
        target_name = f"{G}.{qtable}"
        gold_q = norm.select("source_row_id","source_file_name","quarantine_datetime_utc",
            "dq_status","dq_error_code","dq_error_description","source_payload_json") \
            .withColumn("_insert_datetime_utc",F.current_timestamp()).withColumn("_update_datetime_utc",F.current_timestamp())
        target = DeltaTable.forName(spark,target_name)
        (target.alias("t").merge(gold_q.alias("s"),
            "t.source_row_id=s.source_row_id AND t.dq_error_code=s.dq_error_code")
            .whenMatchedUpdate(set={"source_file_name":"s.source_file_name",
                "quarantine_datetime_utc":"s.quarantine_datetime_utc","dq_status":"s.dq_status",
                "dq_error_description":"s.dq_error_description","source_payload_json":"s.source_payload_json",
                "_update_datetime_utc":"current_timestamp()"})
            .whenNotMatchedInsertAll().execute())
        issue_frames.append(norm.select("source_table","source_row_id","source_file_name","dq_error_code",
            "dq_error_description","dq_status","quarantine_datetime_utc"))
        rule_frames.append(norm.select("source_table","dq_error_code").dropDuplicates()
            .withColumn("dq_rule_name",F.col("dq_error_code"))
            .withColumn("dq_rule_description",F.lit("Rule metadata derived from Silver quarantine"))
            .withColumn("severity",F.lit("Error")))
    if not issue_frames:
        print("No Silver quarantine tables found.")
        return
    rules = issue_frames[0].select("source_table","dq_error_code").limit(0)
    all_rules = rule_frames[0]
    for f in rule_frames[1:]:
        all_rules = all_rules.unionByName(f)
    all_rules = all_rules.dropDuplicates(["source_table","dq_error_code"])
    existing_rules = spark.table(f"{G}.dim_data_quality_rule")
    missing_rules = all_rules.join(existing_rules.select("source_table","dq_error_code"),
        ["source_table","dq_error_code"],"left_anti")
    if missing_rules.limit(1).count():
        missing_rules = assign_keys(missing_rules,f"{G}.dim_data_quality_rule","dq_rule_key",
            ["source_table","dq_error_code"]).withColumn("_insert_datetime_utc",F.current_timestamp()) \
            .withColumn("_update_datetime_utc",F.current_timestamp())
        missing_rules.select(*existing_rules.columns).write.format("delta").mode("append").saveAsTable(f"{G}.dim_data_quality_rule")
    rule_map = spark.table(f"{G}.dim_data_quality_rule").select("source_table","dq_error_code","dq_rule_key")
    issues = issue_frames[0]
    for f in issue_frames[1:]:
        issues = issues.unionByName(f)
    issues = issues.join(rule_map,["source_table","dq_error_code"],"left")
    assert_no_rows(issues.filter(F.col("dq_rule_key").isNull()),"DQ issue has no dim_data_quality_rule mapping")
    existing = spark.table(f"{G}.fact_data_quality_issue").select(
        "source_table","source_row_id","dq_error_code","dq_issue_key")
    new = issues.join(existing.select("source_table","source_row_id","dq_error_code"),
        ["source_table","source_row_id","dq_error_code"],"left_anti")
    if new.limit(1).count():
        new = assign_keys(new,f"{G}.fact_data_quality_issue","dq_issue_key",
            ["source_table","source_row_id","dq_error_code"])
        issues = issues.join(existing,["source_table","source_row_id","dq_error_code"],"left").join(
            new.select("source_table","source_row_id","dq_error_code","dq_issue_key").withColumnRenamed("dq_issue_key","_new_key"),
            ["source_table","source_row_id","dq_error_code"],"left").withColumn(
            "dq_issue_key",F.coalesce("dq_issue_key","_new_key")).drop("_new_key")
    else:
        issues = issues.join(existing,["source_table","source_row_id","dq_error_code"],"inner")
    issue_src = issues.select("dq_issue_key","source_table","source_row_id","source_file_name",
        "dq_rule_key","dq_error_code","dq_error_description",
        F.col("dq_status").alias("_dq_status"),
        F.col("quarantine_datetime_utc").alias("_quarantine_datetime_utc")) \
        .withColumn("_insert_datetime_utc",F.current_timestamp()).withColumn("_update_datetime_utc",F.current_timestamp())
    target = DeltaTable.forName(spark,f"{G}.fact_data_quality_issue")
    (target.alias("t").merge(issue_src.alias("s"),
        "t.source_table=s.source_table AND t.source_row_id=s.source_row_id AND t.dq_error_code=s.dq_error_code")
        .whenMatchedUpdate(set={"source_file_name":"s.source_file_name","dq_rule_key":"s.dq_rule_key",
            "dq_error_description":"s.dq_error_description","_dq_status":"s._dq_status",
            "_quarantine_datetime_utc":"s._quarantine_datetime_utc","_update_datetime_utc":"current_timestamp()"})
        .whenNotMatchedInsertAll().execute())

def validate_base_model():
    # Current-row uniqueness.
    for dim, nk in [("dim_customer","customer_id"),("dim_store","store_id"),("dim_product","product_id")]:
        df = spark.table(f"{G}.{dim}").filter(F.col(nk).isNotNull() & (F.col(nk) !=
            F.when(F.lit(dim=="dim_customer"),F.lit("NO_CUSTOMER")).otherwise(
            F.when(F.lit(dim=="dim_store"),F.lit("UNKNOWN_STORE")).otherwise(F.lit("UNKNOWN_PRODUCT")))))
        assert_no_rows(df.filter(F.col("is_current")==True).groupBy(nk).count().filter("count != 1"),
            f"{dim} must have exactly one current row per natural key")
        overlaps = (df.alias("a").join(df.alias("b"),
            (F.col(f"a.{nk}")==F.col(f"b.{nk}")) & (F.col("a." + {"dim_customer":"customer_key","dim_store":"store_key","dim_product":"product_key"}[dim]) <
             F.col("b." + {"dim_customer":"customer_key","dim_store":"store_key","dim_product":"product_key"}[dim])) &
            (F.col("a.effective_from") < F.col("b.effective_to")) &
            (F.col("b.effective_from") < F.col("a.effective_to")),"inner"))
        assert_no_rows(overlaps, f"Overlapping SCD2 intervals in {dim}", [f"a.{nk}"])
    assert_no_rows(spark.table(f"{G}.fact_invoice").filter(F.col("store_key")==-1),
        "Gold fact_invoice contains store_key=-1; this project has no valid unknown-store mapping rule")
    # Reconcile source grain counts after merge.
    silver_invoice_count = spark.table(f"{S}.invoices").count()
    gold_invoice_count = spark.table(f"{G}.fact_invoice").count()
    if silver_invoice_count != gold_invoice_count:
        raise RuntimeError(f"Invoice row-count mismatch: Silver={silver_invoice_count}, Gold={gold_invoice_count}")
    silver_item_count = spark.table(f"{S}.invoice_items").count()
    gold_item_count = spark.table(f"{G}.fact_invoice_item").count()
    if silver_item_count != gold_item_count:
        raise RuntimeError(f"Invoice-item row-count mismatch: Silver={silver_item_count}, Gold={gold_item_count}")

def main():
    spark.conf.set("spark.sql.session.timeZone","UTC")
    require_tables([f"{S}.customers",f"{S}.stores",f"{S}.products",f"{S}.invoices",f"{S}.invoice_items",
        f"{G}.dim_date",f"{G}.dim_geography",f"{G}.dim_customer",f"{G}.dim_store",
        f"{G}.dim_category",f"{G}.dim_subcategory",f"{G}.dim_product",f"{G}.dim_data_quality_rule",
        f"{G}.fact_invoice",f"{G}.fact_invoice_item",f"{G}.fact_data_quality_issue",
        f"{G}.quarantined_customers",f"{G}.quarantined_products",f"{G}.quarantined_stores",
        f"{G}.quarantined_invoices",f"{G}.quarantined_invoice_items"])
    load_dimensions()
    load_facts()
    load_dq_and_quarantine()
    validate_base_model()
    print("PASS: Gold base load completed with SCD2 lookup, surrogate-key, grain and row-count validations.")

if __name__ == "__main__":
    main()
