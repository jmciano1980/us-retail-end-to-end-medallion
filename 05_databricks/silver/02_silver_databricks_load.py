from __future__ import annotations

import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
CONTROL_TABLE = f"{CATALOG}.{SILVER_SCHEMA}._silver_file_ingestion_log"

TABLES = ["customers", "products", "stores", "invoices", "invoice_items"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "M7 Silver quality load. Validates Bronze data, enforces referential "
            "integrity and invoice atomicity, and writes valid/quarantined Delta rows."
        )
    )
    parser.add_argument(
        "--table",
        action="append",
        choices=TABLES,
        help="Process only selected table(s). Repeat the option for multiple tables. Default: all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Run all validation and print detailed DQ diagnostics without writing "
            "Silver or quarantine tables."
        ),
    )
    return parser.parse_args()


def ensure_objects(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")


def not_blank(column: str) -> F.Column:
    value = F.col(column).cast("string")
    return value.isNotNull() & (F.length(F.trim(value)) > 0)


def parsed_date(column: str) -> F.Column:
    """Parse common PostgreSQL/Parquet date representations without failing the Job.

    Use the PySpark try_to_timestamp API rather than SQL expressions so that
    datetime format literals containing a quoted `T` are passed to Spark
    correctly. Invalid values become NULL and are quarantined by the caller.
    """
    value = F.trim(F.col(column).cast("string"))
    return F.coalesce(
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd HH:mm:ss.SSSSSS")),
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")),
        F.try_to_timestamp(value, F.lit("MM/dd/yyyy")),
    ).cast("date")


def parsed_decimal(column: str) -> F.Column:
    return F.expr(f"try_cast(trim(`{column}`) AS DECIMAL(18,2))")


def parsed_long(column: str) -> F.Column:
    return F.expr(f"try_cast(trim(`{column}`) AS BIGINT)")


def parsed_boolean(column: str) -> F.Column:
    value = F.lower(F.trim(F.col(column).cast("string")))
    return (
        F.when(value.isNull() | (value == ""), F.lit(None).cast("boolean"))
        .when(value.isin("true", "t", "1", "yes", "y"), F.lit(True))
        .when(value.isin("false", "f", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def quality_columns(df: DataFrame, conditions: list[tuple[str, F.Column]]) -> DataFrame:
    """Attach all applicable DQ error codes, not only the first failing rule."""
    codes = [F.when(condition, F.lit(code)) for code, condition in conditions]
    return df.withColumn("_dq_error_code", F.concat_ws(";", *codes)).withColumn(
        "_dq_error_description", F.concat_ws(";", *codes)
    )


def add_quarantine_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_quarantine_datetime_utc", F.current_timestamp())
        .withColumn("_dq_status", F.lit("QUARANTINED"))
    )


def valid_dimension_ids(df: DataFrame, id_column: str) -> DataFrame:
    """Return unique, valid business IDs for referential-integrity joins."""
    return (
        df.where(F.col("_valid") & not_blank(id_column))
        .select(F.trim(F.col(id_column)).alias(id_column))
        .groupBy(id_column)
        .count()
        .where(F.col("count") == 1)
        .select(id_column)
    )


def write_delta_merge(
    spark: SparkSession,
    df: DataFrame,
    target_name: str,
    key_column: str = "_source_row_id",
) -> None:
    if df.limit(1).count() == 0:
        return

    target = DeltaTable.forName(spark, target_name)
    target_columns = spark.table(target_name).columns
    source_columns = df.columns

    missing = [c for c in target_columns if c not in source_columns]
    extra = [c for c in source_columns if c not in target_columns]
    if missing or extra:
        raise RuntimeError(
            f"Schema mismatch for {target_name}. Missing={missing}; Extra={extra}"
        )

    source = df.select(*target_columns)
    condition = f"t.`{key_column}` = s.`{key_column}`"
    update_map = {
        c: f"s.`{c}`" for c in target_columns if c != "_insert_datetime_utc"
    }
    update_map["_update_datetime_utc"] = "current_timestamp()"
    insert_map = {c: f"s.`{c}`" for c in target_columns}

    (
        target.alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )


def append_distinct_quarantine(
    spark: SparkSession,
    df: DataFrame,
    target_name: str,
) -> None:
    if df.limit(1).count() == 0:
        return

    existing = spark.table(target_name).select("_source_row_id").dropDuplicates()
    new_rows = df.join(existing, "_source_row_id", "left_anti")
    if new_rows.limit(1).count() > 0:
        new_rows.write.format("delta").mode("append").saveAsTable(target_name)


def prepare_dimensions(spark: SparkSession) -> dict[str, DataFrame]:
    customers = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    products = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.products")
    stores = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.stores")

    # ------------------------------------------------------------------
    # Customers
    # ------------------------------------------------------------------
    c = customers.withColumn("_join_date", parsed_date("join_date"))
    customer_conditions = [
        ("MISSING_CUSTOMER_ID", ~not_blank("customer_id")),
        ("MISSING_FIRST_NAME", ~not_blank("first_name")),
        ("MISSING_LAST_NAME", ~not_blank("last_name")),
        ("MISSING_STREET_ADDRESS", ~not_blank("street_address")),
        ("MISSING_CITY", ~not_blank("city")),
        ("MISSING_STATE", ~not_blank("state")),
        ("MISSING_ZIP_CODE", ~not_blank("zip_code")),
        ("INVALID_JOIN_DATE", not_blank("join_date") & F.col("_join_date").isNull()),
    ]
    c = quality_columns(c, customer_conditions).withColumn(
        "_valid", F.length(F.col("_dq_error_code")) == 0
    )

    customer_dupes = (
        c.where(not_blank("customer_id"))
        .groupBy("customer_id")
        .count()
        .where(F.col("count") > 1)
        .select("customer_id")
        .withColumn("_duplicate_id", F.lit(True))
    )
    c = (
        c.join(customer_dupes, "customer_id", "left")
        .withColumn("_duplicate_id", F.coalesce(F.col("_duplicate_id"), F.lit(False)))
        .withColumn("_valid", F.col("_valid") & ~F.col("_duplicate_id"))
        .withColumn(
            "_dq_error_code",
            F.when(
                F.col("_duplicate_id"),
                F.concat_ws(";", F.col("_dq_error_code"), F.lit("DUPLICATE_CUSTOMER_ID")),
            ).otherwise(F.col("_dq_error_code")),
        )
        .withColumn(
            "_dq_error_description",
            F.when(
                F.col("_duplicate_id"),
                F.concat_ws(";", F.col("_dq_error_description"), F.lit("DUPLICATE_CUSTOMER_ID")),
            ).otherwise(F.col("_dq_error_description")),
        )
    )

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------
    p = (
        products.withColumn("_unit_price", parsed_decimal("unit_price"))
        .withColumn("_cost", parsed_decimal("cost"))
        .withColumn("_is_active", parsed_boolean("is_active"))
    )
    product_conditions = [
        ("MISSING_PRODUCT_ID", ~not_blank("product_id")),
        ("MISSING_SKU", ~not_blank("sku")),
        ("MISSING_PRODUCT_NAME", ~not_blank("product_name")),
        ("MISSING_CATEGORY", ~not_blank("category")),
        ("MISSING_SUBCATEGORY", ~not_blank("subcategory")),
        ("MISSING_BRAND", ~not_blank("brand")),
        ("MISSING_UNIT_PRICE", ~not_blank("unit_price")),
        ("INVALID_UNIT_PRICE", not_blank("unit_price") & F.col("_unit_price").isNull()),
        ("NEGATIVE_UNIT_PRICE", F.col("_unit_price") < 0),
        ("MISSING_COST", ~not_blank("cost")),
        ("INVALID_COST", not_blank("cost") & F.col("_cost").isNull()),
        ("NEGATIVE_COST", F.col("_cost") < 0),
    ]
    p = quality_columns(p, product_conditions).withColumn(
        "_valid", F.length(F.col("_dq_error_code")) == 0
    )

    product_dupes = (
        p.where(not_blank("product_id"))
        .groupBy("product_id")
        .count()
        .where(F.col("count") > 1)
        .select("product_id")
        .withColumn("_duplicate_id", F.lit(True))
    )
    p = (
        p.join(product_dupes, "product_id", "left")
        .withColumn("_duplicate_id", F.coalesce(F.col("_duplicate_id"), F.lit(False)))
        .withColumn("_valid", F.col("_valid") & ~F.col("_duplicate_id"))
        .withColumn(
            "_dq_error_code",
            F.when(
                F.col("_duplicate_id"),
                F.concat_ws(";", F.col("_dq_error_code"), F.lit("DUPLICATE_PRODUCT_ID")),
            ).otherwise(F.col("_dq_error_code")),
        )
        .withColumn(
            "_dq_error_description",
            F.when(
                F.col("_duplicate_id"),
                F.concat_ws(";", F.col("_dq_error_description"), F.lit("DUPLICATE_PRODUCT_ID")),
            ).otherwise(F.col("_dq_error_description")),
        )
    )

    # ------------------------------------------------------------------
    # Stores
    # ------------------------------------------------------------------
    s = (
        stores.withColumn("_opened_date", parsed_date("opened_date"))
        .withColumn("_square_footage", parsed_long("square_footage"))
    )
    store_conditions = [
        ("MISSING_STORE_ID", ~not_blank("store_id")),
        ("MISSING_STORE_NAME", ~not_blank("store_name")),
        ("MISSING_CITY", ~not_blank("city")),
        ("MISSING_STATE", ~not_blank("state")),
        ("MISSING_ZIP_CODE", ~not_blank("zip_code")),
        ("MISSING_REGION", ~not_blank("region")),
        ("MISSING_MANAGER_NAME", ~not_blank("manager_name")),
        ("MISSING_OPENED_DATE", ~not_blank("opened_date")),
        ("INVALID_OPENED_DATE", not_blank("opened_date") & F.col("_opened_date").isNull()),
        ("INVALID_SQUARE_FOOTAGE", not_blank("square_footage") & F.col("_square_footage").isNull()),
        ("NEGATIVE_SQUARE_FOOTAGE", F.col("_square_footage") < 0),
    ]
    s = quality_columns(s, store_conditions).withColumn(
        "_valid", F.length(F.col("_dq_error_code")) == 0
    )

    store_dupes = (
        s.where(not_blank("store_id"))
        .groupBy("store_id")
        .count()
        .where(F.col("count") > 1)
        .select("store_id")
        .withColumn("_duplicate_id", F.lit(True))
    )
    s = (
        s.join(store_dupes, "store_id", "left")
        .withColumn("_duplicate_id", F.coalesce(F.col("_duplicate_id"), F.lit(False)))
        .withColumn("_valid", F.col("_valid") & ~F.col("_duplicate_id"))
        .withColumn(
            "_dq_error_code",
            F.when(
                F.col("_duplicate_id"),
                F.concat_ws(";", F.col("_dq_error_code"), F.lit("DUPLICATE_STORE_ID")),
            ).otherwise(F.col("_dq_error_code")),
        )
        .withColumn(
            "_dq_error_description",
            F.when(
                F.col("_duplicate_id"),
                F.concat_ws(";", F.col("_dq_error_description"), F.lit("DUPLICATE_STORE_ID")),
            ).otherwise(F.col("_dq_error_description")),
        )
    )

    return {"customers": c, "products": p, "stores": s}


def prepare_invoices_and_items(
    spark: SparkSession,
    dimensions: dict[str, DataFrame],
) -> tuple[DataFrame, DataFrame]:
    invoices = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.invoices")
    items = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.invoice_items")

    # Use distributed Spark joins for referential integrity. Do NOT collect
    # dimension IDs into the driver and use Python isin() for the 55M-row facts.
    valid_store_ids = valid_dimension_ids(dimensions["stores"], "store_id")
    valid_customer_ids = valid_dimension_ids(dimensions["customers"], "customer_id")
    valid_product_ids = valid_dimension_ids(dimensions["products"], "product_id")

    # ------------------------------------------------------------------
    # Invoice headers
    # ------------------------------------------------------------------
    i = invoices.withColumn("_invoice_date", parsed_date("invoice_date"))
    invoice_conditions = [
        ("MISSING_INVOICE_ID", ~not_blank("invoice_id")),
        ("MISSING_STORE_ID", ~not_blank("store_id")),
        # A NULL customer_id represents an anonymous / unidentified retail
        # transaction. It is intentionally accepted and must NOT quarantine
        # the invoice. A non-NULL customer_id is still subject to validation.
        # A non-NULL blank/whitespace value is still considered missing.
        (
            "MISSING_CUSTOMER_ID",
            F.col("customer_id").isNotNull() & ~not_blank("customer_id"),
        ),
        ("MISSING_INVOICE_DATE", ~not_blank("invoice_date")),
        ("INVALID_INVOICE_DATE", not_blank("invoice_date") & F.col("_invoice_date").isNull()),
        ("MISSING_PAYMENT_METHOD", ~not_blank("payment_method")),
    ]
    i = quality_columns(i, invoice_conditions).withColumn(
        "_header_valid", F.length(F.col("_dq_error_code")) == 0
    )

    # Referential checks against clean, unique dimension IDs.
    i = (
        i.join(
            valid_store_ids.withColumn("_store_ref_ok", F.lit(True)),
            on="store_id",
            how="left",
        )
        .join(
            valid_customer_ids.withColumn("_customer_ref_ok", F.lit(True)),
            on="customer_id",
            how="left",
        )
        .withColumn(
            "_store_ref_ok",
            F.coalesce(F.col("_store_ref_ok"), F.lit(False)),
        )
        .withColumn(
            "_customer_ref_ok",
            # NULL Customer_ID is an accepted anonymous transaction. For a
            # supplied/non-blank Customer_ID, the ID must resolve to exactly
            # one valid customer.
            F.when(F.col("customer_id").isNull(), F.lit(True))
            .otherwise(F.coalesce(F.col("_customer_ref_ok"), F.lit(False))),
        )
        .withColumn(
            "_dq_error_code",
            F.concat_ws(
                ";",
                F.col("_dq_error_code"),
                F.when(
                    not_blank("store_id") & ~F.col("_store_ref_ok"),
                    F.lit("INVALID_STORE_REFERENCE"),
                ),
                F.when(
                    not_blank("customer_id") & ~F.col("_customer_ref_ok"),
                    F.lit("INVALID_CUSTOMER_REFERENCE"),
                ),
            ),
        )
        .withColumn(
            "_dq_error_description",
            F.concat_ws(
                ";",
                F.col("_dq_error_description"),
                F.when(
                    not_blank("store_id") & ~F.col("_store_ref_ok"),
                    F.lit("INVALID_STORE_REFERENCE"),
                ),
                F.when(
                    not_blank("customer_id") & ~F.col("_customer_ref_ok"),
                    F.lit("INVALID_CUSTOMER_REFERENCE"),
                ),
            ),
        )
        .withColumn(
            "_header_valid",
            F.col("_header_valid")
            & F.col("_store_ref_ok")
            & F.col("_customer_ref_ok"),
        )
        .drop("_store_ref_ok", "_customer_ref_ok")
    )

    invoice_dupes = (
        i.where(not_blank("invoice_id"))
        .groupBy("invoice_id")
        .count()
        .where(F.col("count") > 1)
        .select("invoice_id")
        .withColumn("_duplicate_invoice", F.lit(True))
    )
    i = (
        i.join(invoice_dupes, "invoice_id", "left")
        .withColumn(
            "_duplicate_invoice",
            F.coalesce(F.col("_duplicate_invoice"), F.lit(False)),
        )
        .withColumn(
            "_header_valid",
            F.col("_header_valid") & ~F.col("_duplicate_invoice"),
        )
        .withColumn(
            "_dq_error_code",
            F.when(
                F.col("_duplicate_invoice"),
                F.concat_ws(";", F.col("_dq_error_code"), F.lit("DUPLICATE_INVOICE_ID")),
            ).otherwise(F.col("_dq_error_code")),
        )
        .withColumn(
            "_dq_error_description",
            F.when(
                F.col("_duplicate_invoice"),
                F.concat_ws(";", F.col("_dq_error_description"), F.lit("DUPLICATE_INVOICE_ID")),
            ).otherwise(F.col("_dq_error_description")),
        )
    )

    # ------------------------------------------------------------------
    # Invoice items
    # ------------------------------------------------------------------
    it = (
        items.withColumn("_line_item", parsed_long("line_item"))
        .withColumn("_quantity", parsed_long("quantity"))
        .withColumn("_unit_price", parsed_decimal("unit_price"))
        .withColumn("_discount", parsed_decimal("discount"))
    )
    item_conditions = [
        ("MISSING_INVOICE_ID", ~not_blank("invoice_id")),
        ("MISSING_LINE_ITEM", ~not_blank("line_item")),
        ("INVALID_LINE_ITEM", not_blank("line_item") & F.col("_line_item").isNull()),
        # Do not reject line_item = 0. The user's explicit rule is that the field
        # must exist; zero can be a legitimate zero-based source line number.
        ("MISSING_PRODUCT_ID", ~not_blank("product_id")),
        ("MISSING_QUANTITY", ~not_blank("quantity")),
        ("INVALID_QUANTITY", not_blank("quantity") & F.col("_quantity").isNull()),
        ("NON_POSITIVE_QUANTITY", F.col("_quantity") <= 0),
        ("MISSING_UNIT_PRICE", ~not_blank("unit_price")),
        ("INVALID_UNIT_PRICE", not_blank("unit_price") & F.col("_unit_price").isNull()),
        ("NEGATIVE_UNIT_PRICE", F.col("_unit_price") < 0),
        ("INVALID_DISCOUNT", not_blank("discount") & F.col("_discount").isNull()),
        ("NEGATIVE_DISCOUNT", F.col("_discount") < 0),
    ]
    it = quality_columns(it, item_conditions).withColumn(
        "_item_valid", F.length(F.col("_dq_error_code")) == 0
    )

    # Product reference check.
    it = (
        it.join(
            valid_product_ids.withColumn("_product_ref_ok", F.lit(True)),
            on="product_id",
            how="left",
        )
        .withColumn(
            "_product_ref_ok",
            F.coalesce(F.col("_product_ref_ok"), F.lit(False)),
        )
        .withColumn(
            "_dq_error_code",
            F.concat_ws(
                ";",
                F.col("_dq_error_code"),
                F.when(
                    not_blank("product_id") & ~F.col("_product_ref_ok"),
                    F.lit("INVALID_PRODUCT_REFERENCE"),
                ),
            ),
        )
        .withColumn(
            "_dq_error_description",
            F.concat_ws(
                ";",
                F.col("_dq_error_description"),
                F.when(
                    not_blank("product_id") & ~F.col("_product_ref_ok"),
                    F.lit("INVALID_PRODUCT_REFERENCE"),
                ),
            ),
        )
        .withColumn(
            "_item_valid",
            F.col("_item_valid") & F.col("_product_ref_ok"),
        )
        .drop("_product_ref_ok")
    )

    # Duplicate line numbers within an invoice are invalid.
    item_dupes = (
        it.where(not_blank("invoice_id") & F.col("_line_item").isNotNull())
        .groupBy("invoice_id", "_line_item")
        .count()
        .where(F.col("count") > 1)
        .select("invoice_id", "_line_item")
        .withColumn("_duplicate_line", F.lit(True))
    )
    it = (
        it.join(item_dupes, ["invoice_id", "_line_item"], "left")
        .withColumn(
            "_duplicate_line",
            F.coalesce(F.col("_duplicate_line"), F.lit(False)),
        )
        .withColumn(
            "_item_valid",
            F.col("_item_valid") & ~F.col("_duplicate_line"),
        )
        .withColumn(
            "_dq_error_code",
            F.when(
                F.col("_duplicate_line"),
                F.concat_ws(";", F.col("_dq_error_code"), F.lit("DUPLICATE_INVOICE_LINE")),
            ).otherwise(F.col("_dq_error_code")),
        )
        .withColumn(
            "_dq_error_description",
            F.when(
                F.col("_duplicate_line"),
                F.concat_ws(";", F.col("_dq_error_description"), F.lit("DUPLICATE_INVOICE_LINE")),
            ).otherwise(F.col("_dq_error_description")),
        )
    )

    # Invoice-item -> invoice reference.
    # IMPORTANT: reference existence is NOT the same thing as header validity.
    # An item whose invoice header exists but is itself invalid must remain a
    # directly-valid item here and be quarantined later by invoice atomicity.
    # Otherwise millions of perfectly valid item rows get mislabeled as
    # INVALID_INVOICE_REFERENCE simply because their header has another DQ issue.
    existing_invoice_ids = (
        invoices
        .where(not_blank("invoice_id"))
        .select(F.col("invoice_id").alias("_invoice_ref_id"))
        .dropDuplicates()
    )
    it = (
        it.join(
            existing_invoice_ids.withColumn("_invoice_ref_ok", F.lit(True)),
            it.invoice_id == F.col("_invoice_ref_id"),
            "left",
        )
        .withColumn(
            "_invoice_ref_ok",
            F.coalesce(F.col("_invoice_ref_ok"), F.lit(False)),
        )
        .withColumn(
            "_dq_error_code",
            F.concat_ws(
                ";",
                F.col("_dq_error_code"),
                F.when(
                    not_blank("invoice_id") & ~F.col("_invoice_ref_ok"),
                    F.lit("INVALID_INVOICE_REFERENCE"),
                ),
            ),
        )
        .withColumn(
            "_dq_error_description",
            F.concat_ws(
                ";",
                F.col("_dq_error_description"),
                F.when(
                    not_blank("invoice_id") & ~F.col("_invoice_ref_ok"),
                    F.lit("INVALID_INVOICE_REFERENCE"),
                ),
            ),
        )
        .withColumn(
            "_item_valid",
            F.col("_item_valid") & F.col("_invoice_ref_ok"),
        )
        .drop("_invoice_ref_id", "_invoice_ref_ok")
    )

    # Preserve direct/root DQ reasons BEFORE invoice atomicity adds cascade reasons.
    # These columns are intentionally kept separate from the final _dq_error_code:
    # one bad item can quarantine an otherwise-valid invoice and every item on it.
    i = (
        i.withColumn("_header_dq_error_code", F.col("_dq_error_code"))
        .withColumn("_header_dq_error_description", F.col("_dq_error_description"))
        .withColumn("_header_direct_valid", F.col("_header_valid"))
    )
    it = (
        it.withColumn("_item_dq_error_code", F.col("_dq_error_code"))
        .withColumn("_item_dq_error_description", F.col("_dq_error_description"))
        .withColumn("_item_direct_valid", F.col("_item_valid"))
    )

    # ------------------------------------------------------------------
    # Invoice atomicity
    # ------------------------------------------------------------------
    # If one line is bad, the entire invoice is quarantined. Conversely, an
    # invoice enters clean Silver only if its header and every line are valid.
    item_summary = (
        it.groupBy("invoice_id")
        .agg(
            F.count("*").alias("item_count"),
            F.sum(F.when(F.col("_item_valid"), 0).otherwise(1)).alias(
                "invalid_item_count"
            ),
        )
    )
    i = (
        i.join(item_summary, "invoice_id", "left")
        .withColumn("item_count", F.coalesce(F.col("item_count"), F.lit(0)))
        .withColumn(
            "invalid_item_count",
            F.coalesce(F.col("invalid_item_count"), F.lit(0)),
        )
        .withColumn(
            "_invoice_valid",
            F.col("_header_valid")
            & (F.col("item_count") > 0)
            & (F.col("invalid_item_count") == 0),
        )
        .withColumn(
            "_dq_error_code",
            F.concat_ws(
                ";",
                F.col("_dq_error_code"),
                F.when(
                    F.col("item_count") == 0,
                    F.lit("NO_INVOICE_ITEMS"),
                ),
                F.when(
                    F.col("invalid_item_count") > 0,
                    F.lit("INVALID_INVOICE_ITEM"),
                ),
            ),
        )
        .withColumn(
            "_dq_error_description",
            F.concat_ws(
                ";",
                F.col("_dq_error_description"),
                F.when(
                    F.col("item_count") == 0,
                    F.lit("NO_INVOICE_ITEMS"),
                ),
                F.when(
                    F.col("invalid_item_count") > 0,
                    F.lit("INVOICE_QUARANTINED_BY_BAD_ITEM"),
                ),
            ),
        )
        .withColumn(
            "_invoice_direct_bad",
            ~F.col("_header_direct_valid")
            | (F.col("item_count") == 0),
        )
        .withColumn(
            "_invoice_bad_item",
            F.col("invalid_item_count") > 0,
        )
    )

    # Every line belonging to a quarantined invoice is quarantined too.
    invoice_status = i.select("invoice_id", "_invoice_valid").dropDuplicates(
        ["invoice_id"]
    )
    it = (
        it.join(invoice_status, "invoice_id", "left")
        .withColumn(
            "_invoice_valid",
            F.coalesce(F.col("_invoice_valid"), F.lit(False)),
        )
        .withColumn(
            "_item_valid_final",
            F.col("_item_valid") & F.col("_invoice_valid"),
        )
        .withColumn(
            "_dq_error_code",
            F.when(
                ~F.col("_invoice_valid"),
                F.concat_ws(";", F.col("_dq_error_code"), F.lit("INVOICE_QUARANTINED_BY_BAD_HEADER_OR_ITEM")),
            ).otherwise(F.col("_dq_error_code")),
        )
        .withColumn(
            "_dq_error_description",
            F.when(
                ~F.col("_invoice_valid"),
                F.concat_ws(
                    ";",
                    F.col("_dq_error_description"),
                    F.lit("INVOICE_QUARANTINED_BY_BAD_HEADER_OR_ITEM"),
                ),
            ).otherwise(F.col("_dq_error_description")),
        )
    )

    return i, it


def customer_output(df: DataFrame, valid: bool) -> DataFrame:
    base = df.where(F.col("_valid") == valid)
    if valid:
        return base.select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "street_address",
            "city",
            "state",
            "zip_code",
            "loyalty_tier",
            F.col("_join_date").alias("join_date"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
        )
    return add_quarantine_metadata(
        base.select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "street_address",
            "city",
            "state",
            "zip_code",
            "loyalty_tier",
            F.col("_join_date").alias("join_date"),
            F.col("join_date").alias("join_date_raw"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
            "_dq_error_code",
            "_dq_error_description",
        )
    )


def product_output(df: DataFrame, valid: bool) -> DataFrame:
    base = df.where(F.col("_valid") == valid)
    if valid:
        return base.select(
            "product_id",
            "sku",
            "product_name",
            "category",
            "subcategory",
            "brand",
            F.col("_unit_price").alias("unit_price"),
            F.col("_cost").alias("cost"),
            F.col("_is_active").alias("is_active"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
        )
    return add_quarantine_metadata(
        base.select(
            "product_id",
            "sku",
            "product_name",
            "category",
            "subcategory",
            "brand",
            F.col("_unit_price").alias("unit_price"),
            F.col("unit_price").alias("unit_price_raw"),
            F.col("_cost").alias("cost"),
            F.col("cost").alias("cost_raw"),
            F.col("_is_active").alias("is_active"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
            "_dq_error_code",
            "_dq_error_description",
        )
    )


def store_output(df: DataFrame, valid: bool) -> DataFrame:
    base = df.where(F.col("_valid") == valid)
    if valid:
        return base.select(
            "store_id",
            "store_name",
            "city",
            "state",
            "zip_code",
            "region",
            "manager_name",
            F.col("_opened_date").alias("opened_date"),
            F.col("_square_footage").alias("square_footage"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
        )
    return add_quarantine_metadata(
        base.select(
            "store_id",
            "store_name",
            "city",
            "state",
            "zip_code",
            "region",
            "manager_name",
            F.col("_opened_date").alias("opened_date"),
            F.col("opened_date").alias("opened_date_raw"),
            F.col("_square_footage").alias("square_footage"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
            "_dq_error_code",
            "_dq_error_description",
        )
    )


def invoice_output(df: DataFrame, valid: bool) -> DataFrame:
    base = df.where(F.col("_invoice_valid") == valid)
    if valid:
        return base.select(
            "invoice_id",
            "store_id",
            "customer_id",
            F.col("_invoice_date").alias("invoice_date"),
            "payment_method",
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
        )
    return add_quarantine_metadata(
        base.select(
            "invoice_id",
            "store_id",
            "customer_id",
            F.col("_invoice_date").alias("invoice_date"),
            F.col("invoice_date").alias("invoice_date_raw"),
            "payment_method",
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
            "_dq_error_code",
            "_dq_error_description",
        )
    )


def item_output(df: DataFrame, valid: bool) -> DataFrame:
    base = df.where(F.col("_item_valid_final") == valid)
    if valid:
        return base.select(
            "invoice_id",
            F.col("_line_item").alias("line_item"),
            "product_id",
            F.col("_quantity").alias("quantity"),
            F.col("_unit_price").alias("unit_price"),
            F.col("_discount").alias("discount"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
        )
    return add_quarantine_metadata(
        base.select(
            "invoice_id",
            F.col("_line_item").alias("line_item"),
            F.col("line_item").alias("line_item_raw"),
            "product_id",
            F.col("_quantity").alias("quantity"),
            F.col("quantity").alias("quantity_raw"),
            F.col("_unit_price").alias("unit_price"),
            F.col("unit_price").alias("unit_price_raw"),
            F.col("_discount").alias("discount"),
            F.col("discount").alias("discount_raw"),
            "_insert_datetime_utc",
            "_update_datetime_utc",
            "_source_file_name",
            "_source_row_id",
            "_dq_error_code",
            "_dq_error_description",
        )
    )


def _reason_counts(df: DataFrame, code_column: str, valid_filter: F.Column | None = None) -> DataFrame:
    """Count root DQ codes and the number of distinct source rows carrying each code."""
    work = df
    if valid_filter is not None:
        work = work.where(valid_filter)
    return (
        work.select(
            F.explode_outer(F.split(F.col(code_column), ";")).alias("dq_error_code"),
            F.col("_source_row_id"),
        )
        .where(F.length(F.trim(F.col("dq_error_code"))) > 0)
        .groupBy("dq_error_code")
        .agg(F.countDistinct("_source_row_id").alias("rows"))
        .orderBy(F.desc("rows"), F.asc("dq_error_code"))
    )


def print_reason_counts(
    df: DataFrame,
    code_column: str,
    title: str,
    valid_filter: F.Column | None = None,
) -> None:
    print(f"\n===== {title} =====")
    result = _reason_counts(df, code_column, valid_filter)
    result.show(100, truncate=False)


def print_invoice_item_root_causes(items: DataFrame) -> None:
    """Show item-level root causes plus how many distinct invoices each cause affects."""
    print("\n===== INVOICE ITEM ROOT CAUSES: ROWS + DISTINCT INVOICES =====")
    result = (
        items.where(~F.col("_item_direct_valid"))
        .select(
            F.explode_outer(F.split(F.col("_item_dq_error_code"), ";")).alias("dq_error_code"),
            F.col("_source_row_id"),
            F.col("invoice_id"),
        )
        .where(F.length(F.trim(F.col("dq_error_code"))) > 0)
        .groupBy("dq_error_code")
        .agg(
            F.countDistinct("_source_row_id").alias("bad_item_rows"),
            F.countDistinct("invoice_id").alias("distinct_invoices_affected"),
        )
        .orderBy(F.desc("bad_item_rows"), F.asc("dq_error_code"))
    )
    result.show(100, truncate=False)


def print_invoice_impact_summary(invoices: DataFrame, items: DataFrame) -> None:
    """Explain invoice quarantine amplification instead of treating cascade as a root DQ rule."""
    print("\n===== INVOICE QUARANTINE IMPACT SUMMARY =====")

    total_headers = invoices.count()
    direct_bad_headers = invoices.where(~F.col("_header_direct_valid")).count()
    anonymous_invoices = invoices.where(F.col("customer_id").isNull()).count()
    identified_invoices = invoices.where(F.col("customer_id").isNotNull()).count()
    invalid_customer_fk = invoices.where(
        F.col("_header_dq_error_code").contains("INVALID_CUSTOMER_REFERENCE")
    ).count()
    no_item_headers = invoices.where(F.col("item_count") == 0).count()
    invoices_with_bad_items = invoices.where(F.col("_invoice_bad_item")).count()
    final_quarantined_headers = invoices.where(~F.col("_invoice_valid")).count()
    clean_headers_before_atomicity = invoices.where(F.col("_header_direct_valid")).count()

    total_item_rows = items.count()
    direct_bad_item_rows = items.where(~F.col("_item_direct_valid")).count()
    final_quarantined_item_rows = items.where(~F.col("_item_valid_final")).count()
    valid_item_rows = items.where(F.col("_item_direct_valid")).count()

    print(f"Invoice headers (Bronze):                    {total_headers:,}")
    print(f"Invoices with NULL Customer_ID (accepted):     {anonymous_invoices:,}")
    print(f"Invoices with Customer_ID supplied:            {identified_invoices:,}")
    print(f"Invoices with invalid supplied Customer_ID:    {invalid_customer_fk:,}")
    print(f"Directly invalid invoice headers:             {direct_bad_headers:,}")
    print(f"Headers with no invoice items:                 {no_item_headers:,}")
    print(f"Distinct invoices with >=1 bad item:          {invoices_with_bad_items:,}")
    print(f"Final quarantined invoices:                    {final_quarantined_headers:,}")
    print(f"Directly valid headers before atomicity:       {clean_headers_before_atomicity:,}")
    print()
    print(f"Invoice item rows (Bronze):                   {total_item_rows:,}")
    print(f"Directly invalid item rows:                    {direct_bad_item_rows:,}")
    print(f"Directly valid item rows:                      {valid_item_rows:,}")
    print(f"Final quarantined item rows:                   {final_quarantined_item_rows:,}")

    def pct(n: int, d: int) -> float:
        return (100.0 * n / d) if d else 0.0

    print("\n--- Customer identification / acceptance ---")
    print(f"Anonymous (NULL Customer_ID) rate:             {pct(anonymous_invoices, total_headers):.4f}%")
    print(f"Identified-customer invoice rate:              {pct(identified_invoices, total_headers):.4f}%")
    print(f"Invalid supplied Customer_ID rate:             {pct(invalid_customer_fk, total_headers):.4f}%")

    print("\n--- Rates ---")
    print(f"Direct bad invoice-header rate:                {pct(direct_bad_headers, total_headers):.4f}%")
    print(f"Invoices affected by bad-item rate:            {pct(invoices_with_bad_items, total_headers):.4f}%")
    print(f"Final invoice quarantine rate:                  {pct(final_quarantined_headers, total_headers):.4f}%")
    print(f"Direct bad item-row rate:                       {pct(direct_bad_item_rows, total_item_rows):.4f}%")
    print(f"Final item quarantine rate:                     {pct(final_quarantined_item_rows, total_item_rows):.4f}%")

    print("\n--- 1% DQ target sanity reference ---")
    print(f"1% of invoice headers:                          {total_headers * 0.01:,.0f} rows")
    print(f"1% of invoice item rows:                        {total_item_rows * 0.01:,.0f} rows")
    print(f"Observed direct bad headers / 1% target:        {direct_bad_headers / (total_headers * 0.01) if total_headers else 0:.2f}x")
    print(f"Observed direct bad items / 1% target:          {direct_bad_item_rows / (total_item_rows * 0.01) if total_item_rows else 0:.2f}x")
    print("NOTE: this is a reference comparison, not a pass/fail rule. The 1% requirement must be checked against the generator's intended corruption model.")

    if direct_bad_item_rows:
        amplification = invoices_with_bad_items / direct_bad_item_rows
        print(f"Item-error invoice amplification factor:       {amplification:.4f} invoices per bad item row")

    # Reconcile the two main invoice quarantine paths.
    overlap = invoices.where(
        (~F.col("_header_direct_valid")) & F.col("_invoice_bad_item")
    ).count()
    header_or_no_item = invoices.where(
        (~F.col("_header_direct_valid")) | (F.col("item_count") == 0)
    ).count()
    print("\n--- Quarantine path reconciliation ---")
    print(f"Headers bad and also affected by bad items:     {overlap:,}")
    print(f"Header/no-item quarantine path:                 {header_or_no_item:,}")
    print(f"Bad-item quarantine path:                       {invoices_with_bad_items:,}")
    print(
        f"Union of paths (expected final quarantine):    "
        f"{header_or_no_item + invoices_with_bad_items - overlap:,}"
    )


def print_detailed_samples(invoices: DataFrame, items: DataFrame, limit: int = 20) -> None:
    print("\n===== DIRECTLY BAD INVOICE HEADERS — SAMPLES =====")
    invoices.where(~F.col("_header_direct_valid")).select(
        "invoice_id", "store_id", "customer_id", "invoice_date", "payment_method",
        "_header_dq_error_code", "_source_file_name", "_source_row_id",
    ).show(limit, truncate=False)

    print("\n===== ACCEPTED ANONYMOUS INVOICES (NULL CUSTOMER_ID) — SAMPLES =====")
    invoices.where(
        F.col("customer_id").isNull() & F.col("_header_direct_valid")
    ).select(
        "invoice_id", "store_id", "customer_id", "invoice_date", "payment_method",
        "item_count", "invalid_item_count", "_source_file_name", "_source_row_id",
    ).show(limit, truncate=False)

    print("\n===== INVOICES QUARANTINED ONLY BECAUSE OF BAD ITEMS — SAMPLES =====")
    invoices.where(
        F.col("_header_direct_valid") & F.col("_invoice_bad_item")
    ).select(
        "invoice_id", "store_id", "customer_id", "invoice_date", "payment_method",
        "item_count", "invalid_item_count", "_header_dq_error_code",
        "_source_file_name", "_source_row_id",
    ).show(limit, truncate=False)

    print("\n===== DIRECTLY BAD INVOICE ITEMS — SAMPLES =====")
    items.where(~F.col("_item_direct_valid")).select(
        "invoice_id", "line_item", "product_id", "quantity", "unit_price", "discount",
        "_item_dq_error_code", "_source_file_name", "_source_row_id",
    ).show(limit, truncate=False)


def print_dimension_summary(df: DataFrame, table_name: str) -> None:
    total = df.count()
    valid = df.where(F.col("_valid")).count()
    bad = total - valid
    print(f"\n{table_name}: Bronze={total:,}; Silver-valid={valid:,}; Quarantine={bad:,}; "
          f"Quarantine-rate={(100.0 * bad / total if total else 0):.4f}%")
    if bad:
        print_reason_counts(df, "_dq_error_code", f"{table_name.upper()} ROOT DQ REASONS", ~F.col("_valid"))


def record_run(
    spark: SparkSession,
    table_name: str,
    status: str,
    source_file_name: str = "M7-BRONZE-CURRENT-STATE",
    source_file_path: str = "",
) -> None:
    now = datetime.now(timezone.utc)
    row = [
        (
            source_file_path,
            source_file_name,
            table_name,
            now,
            status,
            now,
            now,
            source_file_name,
            source_file_name,
        )
    ]
    df = spark.createDataFrame(
        row,
        "source_file_path string, source_file_name string, source_table string, processed_at_utc timestamp, status string, "
        "_insert_datetime_utc timestamp, _update_datetime_utc timestamp, _source_file_name string, _source_row_id string",
    )
    df.write.format("delta").mode("append").saveAsTable(CONTROL_TABLE)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("M7-Silver-Data-Quality-Load").getOrCreate()

    try:
        ensure_objects(spark)
        dimensions = prepare_dimensions(spark)
        invoices, items = prepare_invoices_and_items(spark, dimensions)

        outputs = {
            "customers": (
                customer_output(dimensions["customers"], True),
                customer_output(dimensions["customers"], False),
                f"{CATALOG}.{SILVER_SCHEMA}.customers",
                f"{CATALOG}.{SILVER_SCHEMA}.quarantined_customers",
            ),
            "products": (
                product_output(dimensions["products"], True),
                product_output(dimensions["products"], False),
                f"{CATALOG}.{SILVER_SCHEMA}.products",
                f"{CATALOG}.{SILVER_SCHEMA}.quarantined_products",
            ),
            "stores": (
                store_output(dimensions["stores"], True),
                store_output(dimensions["stores"], False),
                f"{CATALOG}.{SILVER_SCHEMA}.stores",
                f"{CATALOG}.{SILVER_SCHEMA}.quarantined_stores",
            ),
            "invoices": (
                invoice_output(invoices, True),
                invoice_output(invoices, False),
                f"{CATALOG}.{SILVER_SCHEMA}.invoices",
                f"{CATALOG}.{SILVER_SCHEMA}.quarantined_invoices",
            ),
            "invoice_items": (
                item_output(items, True),
                item_output(items, False),
                f"{CATALOG}.{SILVER_SCHEMA}.invoice_items",
                f"{CATALOG}.{SILVER_SCHEMA}.quarantined_invoice_items",
            ),
        }

        selected = args.table or TABLES

        # Always print the current-run quality counts. In dry-run mode these are
        # the only Silver/quarantine counts reported; we deliberately do NOT read
        # target tables because they may contain results from an earlier test run.
        for table_name in selected:
            good_df, quarantine_df, _, _ = outputs[table_name]
            good_count = good_df.count()
            quarantine_count = quarantine_df.count()
            print(f"\n=== {table_name} ===")
            print(f"Silver-valid rows: {good_count:,}")
            print(f"Quarantined rows: {quarantine_count:,}")

        # Detailed diagnostics: root causes are deliberately separated from
        # invoice/item cascade effects. This is the main debugging mode for M7.
        if args.dry_run:
            print("\n============================================================")
            print("M7 SILVER DQ DIAGNOSTIC REPORT — ROOT CAUSES + CASCADES")
            print("============================================================")

            print_dimension_summary(dimensions["customers"], "customers")
            print_dimension_summary(dimensions["products"], "products")
            print_dimension_summary(dimensions["stores"], "stores")

            print_reason_counts(
                invoices,
                "_header_dq_error_code",
                "INVOICE HEADER ROOT DQ REASONS (DIRECT ONLY)",
                ~F.col("_header_direct_valid"),
            )
            print_reason_counts(
                items,
                "_item_dq_error_code",
                "INVOICE ITEM ROOT DQ REASONS (DIRECT ONLY)",
                ~F.col("_item_direct_valid"),
            )
            print_invoice_item_root_causes(items)
            print_invoice_impact_summary(invoices, items)

            print_reason_counts(
                invoices,
                "_dq_error_code",
                "FINAL INVOICE DQ/CASCADE CODES",
                ~F.col("_invoice_valid"),
            )
            print_reason_counts(
                items,
                "_dq_error_code",
                "FINAL ITEM DQ/CASCADE CODES",
                ~F.col("_item_valid_final"),
            )

            print_detailed_samples(invoices, items)

            print("\nIMPORTANT INTERPRETATION:")
            print("- NULL Customer_ID is an accepted anonymous retail transaction and is NOT a DQ error.")
            print("- A supplied Customer_ID that does not resolve to exactly one valid customer IS a DQ error.")
            print("- ROOT DQ counts are the actual records that directly failed a rule.")
            print("- INVOICE_QUARANTINED_BY_BAD_ITEM is a cascade, not a new root error.")
            print("- Final invoice/item quarantine can therefore be much larger than the number of directly bad records.")
            print("- The customer-identification metrics above distinguish anonymous sales from invalid customer references.")
            print("- Compare the direct rates above with the synthetic-data DQ target (1%) to determine whether the source generator or the Silver rules are responsible for the observed volume.")
            print("\nM7 DRY RUN completed. No Silver or quarantine rows were written.")
            return

        # ------------------------------------------------------------------
        # Production write
        # ------------------------------------------------------------------
        for table_name in selected:
            good_df, quarantine_df, good_target, quarantine_target = outputs[table_name]
            write_delta_merge(spark, good_df, good_target)
            append_distinct_quarantine(spark, quarantine_df, quarantine_target)
            record_run(spark, table_name, "SUCCESS")

        # Current-run reconciliation. Since the outputs are computed from the
        # complete Bronze tables, Bronze must equal current Silver + quarantine.
        for table_name in selected:
            bronze_count = spark.table(
                f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
            ).count()
            good_count = outputs[table_name][0].count()
            quarantine_count = outputs[table_name][1].count()
            reconciled = bronze_count == good_count + quarantine_count
            print(
                f"RECONCILIATION {table_name}: Bronze={bronze_count:,}; "
                f"Silver={good_count:,}; Quarantine={quarantine_count:,}; "
                f"Current-run-reconciled={reconciled}"
            )
            if not reconciled:
                raise RuntimeError(
                    f"M7 reconciliation failed for {table_name}: "
                    f"Bronze={bronze_count}, Silver={good_count}, Quarantine={quarantine_count}"
                )

        print("\nM7 Silver quality load completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
