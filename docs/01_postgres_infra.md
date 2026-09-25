# PostgreSQL Source System — Permissive `retail_raw` Schema

## 1. Purpose

PostgreSQL represents the **simulated operational source system** for the US retail platform.

The purpose of this layer is not to provide clean analytical data.

Instead, it intentionally contains realistic data-quality issues that will be detected and handled later in the Medallion architecture.

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

The source schema therefore avoids database constraints that would reject intentionally generated erroneous records.

### Design decisions

* No primary keys on business identifiers.
* No foreign keys between business entities.
* No `CHECK` constraints enforcing business rules.
* Business columns are nullable where the source data may contain missing values.
* Several business attributes use permissive data types where necessary to preserve source values.
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

## 3. Source Tables

The source model contains the following tables:

### `stores`

Stores generated store master data.

```text
raw_id     BIGSERIAL PRIMARY KEY
store_id   INT
```

`store_id` is a business identifier and is therefore not used as the physical primary key.

### `products`

Stores generated product master data.

```text
raw_id       BIGSERIAL PRIMARY KEY
product_id   ...
unit_price   VARCHAR(50)
```

`unit_price` is intentionally permissive so that source values can be preserved without PostgreSQL rejecting them.

### `customers`

Stores generated customer master data.

```text
raw_id      BIGSERIAL PRIMARY KEY
customer_id ...
join_date   VARCHAR
```

The source schema does not enforce business validation on customer attributes.

### `invoices`

Stores invoice/header-level information.

```text
raw_id       BIGSERIAL PRIMARY KEY
invoice_id   ...
invoice_date VARCHAR
store_id     ...
customer_id  ...
```

No foreign key is defined between `invoices` and `stores` or `customers`.

This is intentional: invalid or missing references must be allowed to reach the downstream data-quality process.

### `invoice_items`

Stores invoice line-item information.

```text
raw_id      BIGSERIAL PRIMARY KEY
invoice_id  ...
line_item   ...
product_id  ...
quantity    VARCHAR
unit_price  ...
discount    ...
```

No foreign key is defined between `invoice_items` and `invoices` or `products`.

This allows orphaned or invalid references to be preserved as source data.

---

## 4. Why `raw_id` Is the Technical Primary Key

Business identifiers must not be used as database primary keys in the permissive source schema.

For example, using:

```text
invoice_id PRIMARY KEY
```

would prevent the source database from accepting duplicate invoice identifiers.

That would be undesirable because duplicate business identifiers are one of the data-quality conditions that the downstream pipeline must be able to detect.

Instead, every source record receives a technical identifier:

```text
raw_id BIGSERIAL PRIMARY KEY
```

This provides a unique physical identifier for each loaded record while allowing business identifiers such as:

```text
invoice_id
store_id
product_id
customer_id
```

to remain unconstrained.

A duplicate business identifier can therefore exist in PostgreSQL and later be identified during Silver-layer validation.

---

## 5. Data Types and Source Preservation

The source schema intentionally favors **data preservation over business validation**.

Where necessary, source attributes use permissive types such as:

```text
unit_price
quantity
join_date
invoice_date
```

This prevents PostgreSQL from rejecting values that are intentionally included to simulate source-system quality problems.

The responsibility for interpreting and validating these attributes belongs to downstream layers.

The separation of responsibilities is:

| Layer      | Responsibility                                                |
| ---------- | ------------------------------------------------------------- |
| PostgreSQL | Store the simulated source data                               |
| Parquet    | Extract source data without business correction               |
| RAW        | Immutable landing representation                              |
| Bronze     | Structured/typed source representation and technical metadata |
| Silver     | Data-quality validation and correction                        |
| Quarantine | Isolate records that cannot be accepted into clean Silver     |
| Gold       | Provide analytics-ready business models                       |
| Power BI   | Consume analytical and data-quality models                    |

The key principle is:

> **Do not fix business data-quality problems in the source system.**

---

## 6. Verification of the Source Model

Basic checks can be executed against PostgreSQL to confirm that the permissive source model can contain invalid relationships.

For example, an orphaned store reference can be identified with:

```sql
SELECT
    i.raw_id,
    i.invoice_id,
    i.store_id
FROM retail_raw.invoices i
LEFT JOIN retail_raw.stores s
    ON i.store_id = s.store_id
WHERE s.store_id IS NULL;
```

The purpose of this query is not to correct the records.

It verifies that the source layer can contain records that will later require data-quality treatment.

Additional validation queries can be added as the source model evolves.

---

## 7. PostgreSQL Infrastructure

PostgreSQL runs as part of the local Docker-based development environment.

The infrastructure is based on:

```text
Host Windows
      ↓
WSL2 Ubuntu
      ↓
Docker Engine
      ↓
PostgreSQL
```

The database initialization scripts are maintained under:

```text
01_infra/postgres/
```

The initialization process is responsible for creating the database and the `retail_raw` schema.

The generated source data is subsequently loaded into PostgreSQL by the source-loading process.

---

## 8. Source Loading Principles

The loading process is responsible for making the generated source data available in PostgreSQL without introducing business corrections.

The loader should:

* Load the generated source data into the appropriate tables.
* Preserve intentional NULLs and invalid business values.
* Avoid applying business validation rules.
* Avoid silently removing erroneous records.
* Maintain technical record identity through `raw_id`.
* Provide a repeatable development process.

Technical transformations required only to make the data loadable are acceptable, but they must not change the business meaning of the source record.

---

## 9. Data-Quality Responsibility

A key architectural decision is that **data quality belongs downstream from the source**.

The platform intentionally separates:

```text
Source preservation
        ↓
Technical processing
        ↓
Business validation
        ↓
Correction / quarantine
        ↓
Analytics
```

Therefore:

* PostgreSQL does not clean the generated data.
* Parquet extraction does not clean the generated data.
* RAW does not clean the generated data.
* Bronze does not correct business errors.
* Silver determines whether records are valid.
* Invalid records are sent to Quarantine.
* Valid records continue toward Gold.
* Gold exposes analytical and data-quality information for Power BI.

This separation allows the project to demonstrate both **data engineering** and **data-quality architecture**.

---

## 10. Reproducibility

The PostgreSQL environment should remain reproducible during development.

The infrastructure configuration, initialization scripts, and loading process are maintained in the repository so that the source system can be recreated when necessary.

The objective is to make the following process repeatable:

```text
Initialize PostgreSQL
        ↓
Create retail_raw schema
        ↓
Generate synthetic data
        ↓
Load source data
        ↓
Validate source
        ↓
Extract to Parquet
```

---

## 11. Milestone Outcome

At the end of this milestone:

* PostgreSQL represents the simulated operational source system.
* The `retail_raw` schema accepts intentionally imperfect source records.
* Business identifiers are not enforced as physical primary keys.
* Technical `raw_id` values provide record identity.
* Business validation remains outside the source database.
* Intentional source errors are preserved.
* The source is ready for the next stage:

```text
PostgreSQL → Parquet extraction
```

The next milestone will define and implement the extraction process while preserving the source data as it exists in PostgreSQL.
