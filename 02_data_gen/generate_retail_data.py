"""
M2 - US Retail Data Generator
1M rows, 1-2% error injection, en_US
Output: data/raw/retail_raw.csv + dims for reference
"""
import random
import csv
from datetime import date, timedelta
from pathlib import Path
from faker import Faker
from tqdm import tqdm

fake = Faker('en_US')
Faker.seed(42)
random.seed(42)

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "raw"
OUT_DIR.mkdir(parents=True, exist_ok=True)

NUM_STORES = 50
NUM_PRODUCTS = 2500
NUM_CUSTOMERS = 100_000
NUM_INVOICES = 250_000
NUM_ITEMS = 1_000_000 # this = final retail_raw rows

print(f"Generating to {OUT_DIR}")

# --- 1. STORES ---
print("1/5 Stores...")
stores = []
us_states = [
    ("CA","West"), ("TX","South"), ("NY","Northeast"), ("FL","South"), ("IL","Midwest"),
    ("PA","Northeast"), ("OH","Midwest"), ("GA","South"), ("NC","South"), ("MI","Midwest")
]
for i in range(1, NUM_STORES+1):
    state, region = random.choice(us_states)
    stores.append({
        "store_id": i,
        "store_name": f"Store {i} - {fake.city()}",
        "city": fake.city(),
        "state": state,
        "region": region
    })

# --- 2. PRODUCTS ---
print("2/5 Products...")
categories = {
    "Electronics": ["Phones", "Laptops", "Accessories"],
    "Grocery": ["Beverages", "Snacks", "Dairy"],
    "Apparel": ["Men", "Women", "Kids"],
    "Home": ["Kitchen", "Decor", "Garden"]
}
products = []
for i in range(1, NUM_PRODUCTS+1):
    cat = random.choice(list(categories.keys()))
    sub = random.choice(categories[cat])
    price = round(random.uniform(5, 800), 2)
    products.append({
        "product_id": i,
        "product_name": f"{cat} {sub} {fake.word().title()} {i}",
        "category": cat,
        "subcategory": sub,
        "unit_price": price
    })

# --- 3. CUSTOMERS ---
print("3/5 Customers 100k...")
customers = []
for i in tqdm(range(1, NUM_CUSTOMERS+1)):
    customers.append({
        "customer_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": random.choice([s[0] for s in us_states]),
        "zip": fake.zipcode()
    })

# helpers
product_map = {p["product_id"]: p for p in products}

# --- 4 & 5. INVOICES + ITEMS = RETAIL_RAW 1M ---
print("4/5 Invoices + Items -> retail_raw 1M...")

payment_methods = ["Credit Card", "Debit Card", "Cash", "PayPal", "Gift Card"]

# pre-generate invoice headers
invoice_headers = []
start_date = date(2022, 1, 1)
end_date = date(2024, 12, 31)
date_range = (end_date - start_date).days

for inv_id in range(1, NUM_INVOICES+1):
    invoice_headers.append({
        "invoice_id": inv_id,
        "store_id": random.randint(1, NUM_STORES),
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "invoice_date": start_date + timedelta(days=random.randint(0, date_range)),
        "payment_method": random.choice(payment_methods)
    })

# now generate 1M denormalized rows
with open(OUT_DIR / "retail_raw.csv", "w", newline="", encoding="utf-8") as f:
    fieldnames = [
        "raw_id", "invoice_id", "line_item", "store_id", "store_name", "region",
        "product_id", "product_name", "category", "subcategory",
        "customer_id", "customer_email",
        "quantity", "unit_price", "discount", "line_total",
        "invoice_date", "payment_method"
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    raw_id = 1
    # distribute 1M items across 250k invoices = avg 4 per invoice
    items_per_invoice = [0]*NUM_INVOICES
    for _ in range(NUM_ITEMS):
        items_per_invoice[random.randint(0, NUM_INVOICES-1)] += 1

    for idx, header in tqdm(enumerate(invoice_headers), total=NUM_INVOICES):
        n_lines = items_per_invoice[idx]
        if n_lines == 0:
            n_lines = random.randint(1,2)
        for line in range(1, n_lines+1):
            if raw_id > NUM_ITEMS:
                break
            prod = random.choice(products)
            qty = random.randint(1, 10)
            discount = round(random.choice([0,0,0,0.05,0.1,0.2]),2)
            unit_price = prod["unit_price"]
            line_total = round(qty * unit_price * (1-discount),2)

            # --- ERROR INJECTION 1-2% ---
            r = random.random()
            # 0.4% null customer_id
            cust_id = header["customer_id"]
            if r < 0.004:
                cust_id = ""
            # 0.3% invalid store_id
            store_id = header["store_id"]
            store_name = stores[store_id-1]["store_name"]
            region = stores[store_id-1]["region"]
            if 0.004 <= r < 0.007:
                store_id = 99999
                store_name = "INVALID_STORE"
            # 0.3% negative qty
            if 0.007 <= r < 0.010:
                qty = -qty
            # 0.3% invalid product_id
            product_id = prod["product_id"]
            product_name = prod["product_name"]
            category = prod["category"]
            subcategory = prod["subcategory"]
            if 0.010 <= r < 0.013:
                product_id = 9999999
            # 0.3% zero price
            if 0.013 <= r < 0.016:
                unit_price = 0
                line_total = 0
            # 0.4% future date
            inv_date = header["invoice_date"]
            if 0.016 <= r < 0.020:
                inv_date = date(2026, 6, 15)

            writer.writerow({
                "raw_id": raw_id,
                "invoice_id": header["invoice_id"],
                "line_item": line,
                "store_id": store_id,
                "store_name": store_name,
                "region": region,
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "subcategory": subcategory,
                "customer_id": cust_id,
                "customer_email": customers[cust_id-1]["email"] if str(cust_id).isdigit() and int(cust_id) <= NUM_CUSTOMERS else "",
                "quantity": qty,
                "unit_price": unit_price,
                "discount": discount,
                "line_total": line_total,
                "invoice_date": inv_date,
                "payment_method": header["payment_method"]
            })
            raw_id += 1
            if raw_id > NUM_ITEMS:
                break
        if raw_id > NUM_ITEMS:
            break

# also dump dims for documentation (not loaded to postgres, only retail_raw is)
print("Saving dims for reference...")
import pandas as pd
pd.DataFrame(stores).to_csv(OUT_DIR / "stores.csv", index=False)
pd.DataFrame(products).to_csv(OUT_DIR / "products.csv", index=False)
pd.DataFrame(customers).to_csv(OUT_DIR / "customers.csv", index=False)
# sample invoices header
pd.DataFrame(invoice_headers[:10000]).to_csv(OUT_DIR / "invoices_sample.csv", index=False)

print(f"DONE. Files in {OUT_DIR}")
print(f"retail_raw.csv rows should be ~{NUM_ITEMS}")