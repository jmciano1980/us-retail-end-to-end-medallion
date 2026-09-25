# PostgreSQL Source System — Permissive `retail_raw` Schema

## 1. Purpose

PostgreSQL represents the **simulated operational source system** for the US retail platform.

The purpose of this layer is not to provide clean analytical data. Instead, it intentionally contains realistic data-quality issues that will be detected and handled later in the Medallion architecture.

The processing flow is:

```text
Synthetic Data
      ↓
PostgreSQL Source System
      ↓
Parquet Extraction
      ↓
Databricks RAW
      ↓
Bronze
      ↓
Silver + Quarantine
      ↓
Gold
```

The PostgreSQL source must therefore preserve the characteristics of the generated source data, including intentionally invalid or incomplete records.

---

## 2. Design Principle — Permissive Source Schema

The `retail_raw` schema is intentionally permissive.

Business validation and data-quality correction are **not performed in PostgreSQL**.

The source schema therefore avoids database constraints that would reject the intentionally generated erroneous records.

### Design decisions

* No primary keys on business identifiers.
* No foreign keys between business entities.
* No `CHECK` constraints enforcing business rules.
* Business columns are nullable where the source data may contain missing values.
* Several business attributes are stored using permissive types such as `VARCHAR`.
* A technical `raw_id BIGSERIAL` is used as the physical primary key.

This allows the source system to contain situations such as:

* Missing customer identifiers.
* Invalid product references.
* Missing dates.
* Missing payment methods.
* Duplicate business identifiers.
* Invalid or incomplete line-item information.

These conditions are expected to survive the extraction process and be detected during the Silver-layer data-quality process.

---

## 3. Tables

The current source model contains the following tables:

### `stores`

Stores the generated store master data.

```text
raw_id     BIGSERIAL PRIMARY KEY
store_id   INT
```

The `store_id` is a business identifier and is therefore allowed to contain duplicates or invalid values.

### `products`

Stores the generated product master data.

```text
raw_id       BIGSERIAL PRIMARY KEY
product_id   ...
unit_price   VARCHAR(50)
```

`unit_price` is intentionally permissive so that source values can be preserved without P_
