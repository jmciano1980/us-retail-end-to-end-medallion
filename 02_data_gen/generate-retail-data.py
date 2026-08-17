
"""
generate_retail_data.py - SERIOUS RETAIL VERSION
Outputs:
  PROJECT_ROOT/data/raw/     -> FULL (gitignored) stores.csv, products.csv, customers.csv, retail_raw.csv
  PROJECT_ROOT/data/samples/ -> SAMPLE for GitHub stores.csv, products.csv, customers.csv, invoices_sample.csv

  retail_raw.csv is DENORMALIZED: invoice header + line item in same row
  schema: invoice_id,store_id,customer_id,invoice_date,payment_method,line_item,product_id,quantity,unit_price,discount

  50 Stores - real US cities
  2500 Products - real names: Coca-Cola 12x12oz, Organic Whole Milk Gallon, Boneless Chicken Breast, etc.
  100k Customers - real first_name, last_name, street_address, city, state, zip, email, phone
  55M Invoices / ~200M rows - 3 years 2023 30%, 2024 33%, 2025 37% + 1% errors for Bronze DQ

Run:
  pip install faker
  python 02_data_generation/generate_retail_data.py 1000000   -> TEST 1M invoices ~4M rows
  python 02_data_generation/generate_retail_data.py 55000000 -> FULL 55M invoices
"""
import csv, random, sys, time, os
from datetime import datetime, timedelta
from pathlib import Path

try:
    from faker import Faker
    fake = Faker('en_US')
    Faker.seed(42)
except ImportError:
    print("ERROR: pip install faker")
    sys.exit(1)

random.seed(42)

# CONFIG
NUM_STORES = 50
NUM_PRODUCTS = 2500
NUM_CUSTOMERS = 100000
NUM_INVOICES = 55000000
SAMPLE_INVOICES = 10000
SAMPLE_CUSTOMERS = 1000
ERROR_RATE = 0.01

# Resolve to project root /data/raw and /data/samples (not inside 02_data_generation)
PROJECT_ROOT = Path(__file__).resolve().parents[1] if Path(__file__).resolve().parents[1].name == "us-retail-end-to-end-medallion" else Path(__file__).resolve().parent.parent
# fallback: if script is in 02_data_generation, parent is project root
if (Path(__file__).resolve().parent.name == "02_data_generation"):
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
else:
    # if run from project root, use cwd
    PROJECT_ROOT = Path.cwd()

RAW_DIR = PROJECT_ROOT / "data" / "raw"
SAMPLE_DIR = PROJECT_ROOT / "data" / "samples"
RAW_DIR.mkdir(parents=True, exist_ok=True)
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

PAYMENT = ['Credit Card','Debit Card','Cash','PayPal','EBT','Mobile Pay','Gift Card']
YEAR_WEIGHTS = []
for y,w in [(2023,30),(2024,33),(2025,37)]:
    YEAR_WEIGHTS.extend([y]*w)

# Real US store locations
STORE_LOCATIONS = [
    ("Houston","TX","77002","South"),("Dallas","TX","75201","South"),("Austin","TX","78701","South"),
    ("Miami","FL","33101","South"),("Orlando","FL","32801","South"),("Atlanta","GA","30303","South"),
    ("Charlotte","NC","28202","South"),("Nashville","TN","37201","South"),("New Orleans","LA","70112","South"),("Tampa","FL","33602","South"),
    ("New York","NY","10001","Northeast"),("Boston","MA","02101","Northeast"),("Philadelphia","PA","19102","Northeast"),
    ("Newark","NJ","07102","Northeast"),("Providence","RI","02901","Northeast"),("Hartford","CT","06101","Northeast"),
    ("Buffalo","NY","14202","Northeast"),("Pittsburgh","PA","15201","Northeast"),("Baltimore","MD","21201","Northeast"),("Washington","DC","20001","South"),
    ("Chicago","IL","60601","Midwest"),("Columbus","OH","43215","Midwest"),("Detroit","MI","48201","Midwest"),
    ("Milwaukee","WI","53202","Midwest"),("Minneapolis","MN","55401","Midwest"),("St Louis","MO","63101","Midwest"),
    ("Indianapolis","IN","46201","Midwest"),("Kansas City","MO","64101","Midwest"),("Cleveland","OH","44101","Midwest"),("Cincinnati","OH","45202","Midwest"),
    ("Los Angeles","CA","90001","West"),("San Diego","CA","92101","West"),("San Francisco","CA","94102","West"),
    ("San Jose","CA","95101","West"),("Seattle","WA","98101","West"),("Portland","OR","97201","West"),
    ("Phoenix","AZ","85001","West"),("Denver","CO","80202","West"),("Las Vegas","NV","89101","West"),
    ("Salt Lake City","UT","84101","West"),("Albuquerque","NM","87101","West"),("Boise","ID","83702","West"),
    ("Honolulu","HI","96801","West"),("Anchorage","AK","99501","West"),("Sacramento","CA","95814","West"),
    ("Fresno","CA","93721","West"),("Tucson","AZ","85701","West"),("Colorado Springs","CO","80903","West"),
    ("Oklahoma City","OK","73102","South"),("Louisville","KY","40202","South"),("Memphis","TN","38103","South")
]

PRODUCT_CATALOG = {
    "Grocery": ["Long Grain White Rice 2lb Bag","Spaghetti Pasta 16oz","Peanut Butter Creamy 16oz","Tomato Sauce 15oz","Canned Black Beans 15oz","Olive Oil Extra Virgin 16.9oz","Honey Wheat Bread Loaf","Corn Flakes Cereal 12oz","Granulated Sugar 4lb Bag","All-Purpose Flour 5lb Bag","Mac & Cheese Dinner 7.25oz","Chicken Broth 32oz","Salsa Medium 16oz","Ketchup 20oz","Mayonnaise 30oz"],
    "Produce": ["Organic Bananas Bunch","Hass Avocados 4ct","Gala Apples 3lb Bag","Baby Spinach 5oz Clamshell","Roma Tomatoes 2lb Package","Yellow Onions 3lb Bag","Russet Potatoes 5lb Bag","Fresh Strawberries 16oz","Organic Carrots 2lb Bag","Lemons 2lb Bag","Green Seedless Grapes 2lb","Clementines 3lb Bag","Romaine Hearts 3ct","Cucumber 3ct","Blueberries 6oz"],
    "Dairy": ["Whole Milk Gallon","Large Grade A Eggs 12ct","Sharp Cheddar Cheese Block 8oz","Greek Yogurt Plain 32oz","Salted Butter Sticks 16oz","Sour Cream 16oz","Mozzarella Shredded 8oz","Half and Half Quart","Cream Cheese 8oz Bar","Vanilla Ice Cream Gallon","Almond Milk Unsweetened 64oz","String Cheese 12ct","Cottage Cheese 16oz"],
    "Meat": ["Boneless Chicken Breast 1lb","Ground Beef 80/20 1lb","Atlantic Salmon Fillet 1lb","Pork Chops Bone-In 1lb","Bacon Sliced 12oz","Ground Turkey 1lb","Ribeye Steak 12oz","Italian Sausage Links 12oz","Rotisserie Chicken Whole","Deli Ham Sliced 1lb","Chicken Thighs Family Pack","Ground Pork 1lb"],
    "Beverages": ["Coca-Cola Classic 12x12oz Cans","Pepsi Cola 12x12oz Cans","Orange Juice Pulp Free 52oz","Spring Water 24x16.9oz Bottles","Coffee Ground Medium Roast 12oz","Green Tea Bags 20ct","Energy Drink Original 4x16oz","Sparkling Water Lime 8x12oz","Apple Juice 64oz Bottle","Sports Drink Variety 6x20oz","Cold Brew Coffee 48oz"],
    "Household": ["Paper Towels 6 Rolls","Laundry Detergent Liquid 100oz","Dish Soap Liquid 24oz","Trash Bags Tall Kitchen 80ct","Paper Plates 40ct","Surface Cleaner Spray 22oz","Sponges Scrub 6ct","Aluminum Foil 200sq ft","LED Light Bulbs 4ct","Batteries AA 24ct","Mop Refills 2ct","Fabric Softener 60oz"],
    "Bakery": ["Croissants 4ct","Bagels Plain 6ct","Chocolate Chip Cookies 12ct","Sourdough Bread Loaf","Donuts Glazed 12ct","Blueberry Muffins 4ct","Tortillas Flour 10ct","Cinnamon Rolls 8ct","Baguette French","Whole Wheat Bread"],
    "Frozen": ["Frozen Pizza Pepperoni 12in","Vanilla Ice Cream Bars 6ct","Frozen Mixed Vegetables 12oz","Chicken Nuggets 24oz","Frozen Waffles 10ct","French Fries Crinkle 32oz","Fish Sticks 15oz","Frozen Berries Blend 16oz","Frozen Lasagna Family Size"],
    "Snacks": ["Potato Chips Classic 10oz","Tortilla Chips Restaurant Style 13oz","Popcorn Microwave Butter 6ct","Pretzels Mini 16oz","Mixed Nuts Roasted 15oz","Granola Bars Chewy 6ct","Chocolate Bar Milk 1.55oz","Beef Jerky Original 2.85oz","Cookies Sandwich 15.25oz"]
}

BRANDS = ["Great Value","Kirkland Signature","Horizon Organic","Coca-Cola","Pepsi","Kraft","Nestle","General Mills","Tyson","Dole","Chiquita","Bounty","Tide","Private Selection","365 Whole Foods","Nature Valley","Lays","Doritos","Heinz","Hellmanns","Wonder Bread","Oscar Mayer","Folgers"]
SUBS = {"Grocery":"Pantry","Produce":"Fresh","Dairy":"Dairy","Meat":"Butcher","Beverages":"Drinks","Household":"Cleaning","Bakery":"Bakery","Frozen":"Frozen","Snacks":"Snacks"}

def rand_date(y):
    start = datetime(y,1,1)
    end = datetime(y,12,31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days), hours=random.randint(6,21), minutes=random.randint(0,59))

def gen_stores():
    print(f"Generating stores.csv ({NUM_STORES}) -> {RAW_DIR}")
    rows=[]
    for i in range(1, NUM_STORES+1):
        city, state, zipc, region = STORE_LOCATIONS[i-1]
        name = f"{random.choice(['SuperMart','Fresh Market','Value Foods','City Grocers','Family Mart'])} - {city} {random.choice(['West','East','North','South','Central'])}"
        manager = fake.name()
        opened = fake.date_between(start_date='-8y', end_date='-1y')
        sqft = random.randint(18000, 65000)
        rows.append([i, name, city, state, zipc, region, manager, opened.isoformat(), sqft])
    # write both raw and sample (same stores)
    for base in [RAW_DIR, SAMPLE_DIR]:
        with open(base/"stores.csv","w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["store_id","store_name","city","state","zip_code","region","manager_name","opened_date","square_footage"])
            w.writerows(rows)
    print("  stores.csv done")
    return rows

def gen_products():
    print(f"Generating products.csv ({NUM_PRODUCTS}) -> {RAW_DIR}")
    rows=[]
    price_map={}
    for i in range(1, NUM_PRODUCTS+1):
        cat = random.choice(list(PRODUCT_CATALOG.keys()))
        base = random.choice(PRODUCT_CATALOG[cat])
        brand = random.choice(BRANDS)
        full_name = f"{brand} {base}"
        sku = f"{cat[:3].upper()}-{i:06d}-{random.randint(100,999)}"
        # price by category
        if cat in ("Meat","Household"): price = round(random.uniform(4.99, 34.99),2)
        elif cat in ("Beverages","Frozen"): price = round(random.uniform(3.49, 14.99),2)
        else: price = round(random.uniform(1.29, 12.99),2)
        cost = round(price * random.uniform(0.55, 0.78),2)
        is_active = random.choices([True, False], weights=[95,5])[0]
        rows.append([i, sku, full_name, cat, SUBS[cat], brand, price, cost, is_active])
        price_map[i]=price
    for base in [RAW_DIR, SAMPLE_DIR]:
        with open(base/"products.csv","w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["product_id","sku","product_name","category","subcategory","brand","unit_price","cost","is_active"])
            w.writerows(rows)
    print("  products.csv done")
    return price_map

def gen_customers():
    print(f"Generating customers.csv ({NUM_CUSTOMERS}) -> {RAW_DIR} + sample {SAMPLE_CUSTOMERS}")
    with open(RAW_DIR/"customers.csv","w",newline="",encoding="utf-8") as f_full, open(SAMPLE_DIR/"customers.csv","w",newline="",encoding="utf-8") as f_samp:
        w_full=csv.writer(f_full)
        w_samp=csv.writer(f_samp)
        header=["customer_id","first_name","last_name","email","phone","street_address","city","state","zip_code","loyalty_tier","join_date"]
        w_full.writerow(header)
        w_samp.writerow(header)
        for i in range(1, NUM_CUSTOMERS+1):
            first = fake.first_name()
            last = fake.last_name()
            street = fake.street_address()
            city = fake.city()
            state = fake.state_abbr()
            zipc = fake.zipcode()
            email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(['gmail.com','yahoo.com','outlook.com','hotmail.com'])}"
            phone = fake.phone_number()
            tier = random.choice(["Bronze","Silver","Gold","Platinum"])
            join = fake.date_between(start_date='-4y', end_date='-30d')
            row=[i, first, last, email, phone, street, city, state, zipc, tier, join.isoformat()]
            w_full.writerow(row)
            if i <= SAMPLE_CUSTOMERS:
                w_samp.writerow(row)
    print("  customers.csv done")

def gen_retail_raw(price_map):
    num_inv = int(sys.argv[1]) if len(sys.argv)>1 else NUM_INVOICES
    print(f"Generating retail_raw.csv for {num_inv} invoices -> {RAW_DIR}/retail_raw.csv")
    print(f"  -> ~{num_inv*4} rows, will also create data/samples/invoices_sample.csv ({SAMPLE_INVOICES} invoices)")

    raw_path = RAW_DIR / "retail_raw.csv"
    samp_path = SAMPLE_DIR / "invoices_sample.csv"

    with open(raw_path,"w",newline="",encoding="utf-8", buffering=20*1024*1024) as f_raw, open(samp_path,"w",newline="",encoding="utf-8") as f_samp:
        wr=csv.writer(f_raw)
        ws=csv.writer(f_samp)
        hdr=["invoice_id","store_id","customer_id","invoice_date","payment_method","line_item","product_id","quantity","unit_price","discount"]
        wr.writerow(hdr)
        ws.writerow(hdr)

        total_rows=0
        t0=time.time()
        for inv in range(1, num_inv+1):
            is_err = random.random() < ERROR_RATE
            err = random.choice(['null_date','null_payment','no_customer','no_items','bad_product']) if is_err else None

            store_id = random.randint(1, NUM_STORES)

            # customer: 15% guest null, plus error handling
            if err == 'no_customer':
                cust = "" if random.random()<0.5 else NUM_CUSTOMERS + random.randint(1,10000)
            else:
                if random.random() < 0.15:
                    cust = ""  # guest
                else:
                    # keep sample customers for sample invoices
                    if inv <= SAMPLE_INVOICES and random.random()<0.85:
                        cust = random.randint(1, SAMPLE_CUSTOMERS)
                    else:
                        cust = random.randint(1, NUM_CUSTOMERS)

            year = random.choice(YEAR_WEIGHTS)
            if err == 'null_date':
                inv_date = ""
            else:
                inv_date = rand_date(year).strftime("%Y-%m-%d %H:%M:%S")

            if err == 'null_payment':
                pay = ""
            else:
                pay = random.choice(PAYMENT)

            if err == 'no_items':
                continue  # invoice with 0 items -> will be caught in Silver

            num_items = random.choices([1,2,3,4,5,6,7,8], weights=[18,24,20,15,9,6,4,4])[0]
            if random.random()<0.03:
                num_items = random.randint(9,14)

            for line in range(1, num_items+1):
                if err == 'bad_product' and line==1:
                    pid = NUM_PRODUCTS + random.randint(1,5000)
                    price = round(random.uniform(5,500),2)
                else:
                    pid = random.randint(1, NUM_PRODUCTS)
                    price = price_map[pid]
                qty = random.randint(1,6)
                disc = random.choice([0,0,0,0,0.05,0.1,0.15,0.2])

                row=[inv, store_id, cust, inv_date, pay, line, pid, qty, price, disc]
                wr.writerow(row)
                total_rows+=1
                if inv <= SAMPLE_INVOICES:
                    ws.writerow(row)

            if inv % 500000 == 0:
                elapsed=time.time()-t0
                print(f"  {inv}/{num_inv} {inv/num_inv*100:.1f}% rows:{total_rows} {inv/elapsed:.0f} inv/sec {elapsed/60:.1f}min")

    (RAW_DIR/".gitkeep").touch(exist_ok=True)
    (SAMPLE_DIR/".gitkeep").touch(exist_ok=True)
    print(f"DONE: {num_inv} invoices, {total_rows} rows in retail_raw.csv")
    print(f"  Sample: {SAMPLE_INVOICES} invoices in invoices_sample.csv")

if __name__ == "__main__":
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"RAW_DIR: {RAW_DIR}")
    print(f"SAMPLE_DIR: {SAMPLE_DIR}")
    gen_stores()
    pmap = gen_products()
    gen_customers()
    gen_retail_raw(pmap)
