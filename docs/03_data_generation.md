# 03 - Data Generation

**Script:** `02_data_gen/generate_retail_data.py`

**Output:** `data/raw/`

The full generated dataset is stored locally and excluded from Git. A smaller representative dataset is stored under `data/samples/` for documentation, testing, and GitHub visibility.

---

## 1. Objective

Generate a synthetic US retail dataset that represents the operational source system used by the project.

The generated data is intentionally **not perfect**. Approximately 1% of the generated transactional data contains predefined data-quality issues.

These errors are intentional and are part of the project design.

They will be preserved during source ingestion and addressed later in the Medallion architecture through:

* Data-quality validation
* Data cleansing
* Error classification
* Quarantine
* Data lineage
* Error-quality reporting

The overall flow is:

```text
Synthetic Data Generation
          ↓
CSV Source Data
          ↓
PostgreSQL
Operational Source System
          ↓
Parquet Extraction
          ↓
Databricks RAW
          ↓
Bronze
          ↓
Silver + Quarantine
          ↓
Gold + Data Quality Model
          ↓
Power BI
```

---

## 2. Generated Dataset

The target full dataset contains:

| Entity             | Target Volume |
| ------------------ | ------------: |
| Stores             |            50 |
| Products           |         2,500 |
| Customers          |       100,000 |
| Invoices           |    55,000,000 |
| Transactional rows |  ~200,000,000 |

The transactional dataset is intentionally large enough to demonstrate techniques relevant to large-scale data engineering.

The approximately 200 million transactional rows represent invoice-level and line-item-level data generated from the 55 million invoices.

---

## 3. Time Distribution

Invoice dates are distributed across three years:

| Year | Approximate Share |
| ---- | ----------------: |
| 2023 |               30% |
| 2024 |               33% |
| 2025 |               37% |

This provides a multi-year transactional history for downstream analytical processing.

---

## 4. Transactional Source Schema

The main transactional output is:

```text
data/raw/retail_raw.csv
```

The documented schema is:

```text
invoice_id
store_id
customer_id
invoice_date
payment_method
line_item
product_id
quantity
unit_price
discount
```

The columns represent both invoice-level and line-item-level information.

### Invoice-level attributes

```text
invoice_id
store_id
customer_id
invoice_date
payment_method
```

### Line-item attributes

```text
line_item
product_id
quantity
unit_price
discount
```

The denormalized transactional file is later split into the appropriate source tables when loaded into PostgreSQL.

---

## 5. Intentional Data-Quality Errors

Approximately 1% of the generated data contains predefined data-quality issues.

The documented error scenarios are:

| Error          | Description                               |
| -------------- | ----------------------------------------- |
| `null_date`    | Invoice date is NULL                      |
| `null_payment` | Payment method is NULL                    |
| `no_customer`  | Invoice has no associated customer        |
| `no_items`     | Invoice has no associated line items      |
| `bad_product`  | Transaction references an invalid product |

These conditions are intentionally generated to provide realistic data-quality scenarios for the downstream pipeline.

### Important

The generator does **not** attempt to correct these conditions.

They are expected to flow through the source system and later stages so that the project can demonstrate how a modern data platform identifies and handles bad data.

For example:

```text
Source
  ↓
Invalid record
  ↓
PostgreSQL
  ↓
Parquet
  ↓
Databricks RAW
  ↓
Bronze
  ↓
Data Quality Rules
  ↓
Valid ─────────→ Silver
  │
  └────────────→ Quarantine
```

---

## 6. Output Files

The generator produces the following files under:

```text
data/raw/
```

```text
stores.csv
products.csv
customers.csv
retail_raw.csv
```

A smaller dataset is maintained under:

```text
data/samples/
```

for GitHub and development purposes:

```text
stores.csv
products.csv
customers.csv
invoices_sample.csv
```

---

## 7. Running the Generator

### Test Dataset

Generate a smaller dataset containing **1,000,000 invoices**:

```bash
python 02_data_gen/generate_retail_data.py 1000000
```

This option is intended for development and testing.

---

### Full Dataset

Generate the target dataset containing **55,000,000 invoices**:

```bash
python 02_data_gen/generate_retail_data.py 55000000
```

The full dataset is intended for the complete pipeline execution.

Because of its size, the generated files are not committed to Git.

---

## 8. Git Strategy

The complete generated dataset is excluded from Git:

```text
data/raw/*.csv
```

Only representative sample data is committed:

```text
data/samples/
```

This keeps the repository lightweight while still allowing someone reviewing the project to inspect the structure and contents of the generated data.

---

## 9. Role in the Data Pipeline

This milestone establishes the synthetic operational dataset used throughout the project.

The generator is responsible for:

* Creating the synthetic retail entities
* Creating large-scale transactional data
* Distributing transactions across multiple years
* Introducing predefined data-quality issues
* Producing CSV files for the simulated source system

It is **not** responsible for:

* Data cleansing
* Data-quality correction
* Quarantine
* Analytical transformations
* Business aggregations

Those responsibilities belong to later stages of the Medallion architecture.

---

## 10. Milestone Outcome

At the end of this milestone, the project has a synthetic but intentionally imperfect retail dataset representing:

```text
50 stores
2,500 products
100,000 customers
55,000,000 invoices
~200,000,000 transactional rows
```

The generated CSV files provide the source data for **M3 - Load Source Data into PostgreSQL**.

From there, the data will progress through:

```text
PostgreSQL
    ↓
Parquet
    ↓
Databricks RAW
    ↓
Bronze
    ↓
Silver + Quarantine
    ↓
Gold
    ↓
Power BI
```

The intentional source-data errors are preserved throughout ingestion so that the downstream pipeline can demonstrate complete data-quality management.
