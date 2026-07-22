# Postgres OLTP - retail_raw as Dirty Landing (Bronze)

#### Design Principle: Permissive Landing
`retail_raw` is NOT a clean OLTP. It's a raw landing zone that mimics real source system errors.
All cleaning is deferred to Databricks Silver.

To allow 1-2% error injection, this schema has:
- No PRIMARY KEY on business keys, only `raw_id BIGSERIAL` as technical row id
- No FOREIGN KEY constraints (allows orphan invoice_items, invalid store_id)
- No CHECK constraints (allows invalid region, payment_method, negative qty)
- All business columns nullable and mostly VARCHAR (allows 'abc', '$-10', '2025-13-40')

#### Tables
- stores: raw_id PK, store_id INT nullable duplicate allowed
- products: raw_id PK, unit_price VARCHAR(50) to allow invalid price formats
- customers: raw_id PK, join_date VARCHAR to allow invalid dates
- invoices: raw_id PK, invoice_date VARCHAR, store_id no FK
- invoice_items: raw_id PK, invoice_id no FK, quantity VARCHAR

#### Why raw_id?
If invoice_id is PK, we cannot load duplicate invoice_id errors. With raw_id as PK, the same invoice_id can appear twice with two different raw_id, and we can detect duplicate in Silver with `ROW_NUMBER() OVER (PARTITION BY invoice_id)`.

#### Verification of dirty model
```sql
\d retail_raw.stores -- should show no FK, no CHECK
SELECT * FROM retail_raw.invoices WHERE store_id NOT IN (SELECT store_id FROM retail_raw.stores);
-- should be possible, will return orphans# Postgres OLTP Design - US Retail

#### Model - 3NF Normalized
- stores (50 rows, US Census Regions: Northeast, Midwest, South, West)
- products (2,500 SKUs, categories: Grocery, Produce, Dairy, Meat, Beverages, Household)
- customers (100k, loyalty tiers)
- invoices (250k, FK to stores, nullable FK to customers for guest checkout)
- invoice_items (1M, FK to invoices and products)

#### Key Constraints
- CHECK region IN (...)
- CHECK payment_method IN ('Cash','Credit Card','Debit Card','EBT','Mobile Pay')
- CHECK quantity <> 0, unit_price >=0
- Indexes on invoice_date, store_id for future incremental loads

#### Why Docker Volume for init.sql
./01_infra/postgres/init.sql:/docker-entrypoint-initdb.d/01_init.sql ensures infra is reproducible. Any clone can rebuild DB with one command.

#### Verification
psql -h localhost -U retail_admin -d retail_db -c "\dt retail_raw.*"
