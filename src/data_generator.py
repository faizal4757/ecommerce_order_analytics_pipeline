# Standard library for unique IDs, randomization, and date math
import uuid
import random
from datetime import datetime, timedelta
import os

# Environment variable loading
from dotenv import load_dotenv

# Data processing and S3 upload libraries
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from faker import Faker

# Fixed seeds ensure reproducible data across runs
fake = Faker()
Faker.seed(42)
random.seed(42)

# Generates 500 products across 8 categories with random pricing
def generate_products(n_products=500):
    categories = [
        "Electronics", "Clothing", "Home & Garden", "Sports",
        "Books", "Toys", "Food & Beverage", "Health & Beauty"
    ]
    brands = [
        "TechPro", "StyleCraft", "HomeBase", "ActiveGear",
        "ReadMore", "FunZone", "FreshChoice", "GlowWell",
        "ValueMax", "PrimeLine"
    ]
    products = []
    for i in range(n_products):
        products.append({
            "product_id": f"PROD-{i+1:05d}",
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "brand": random.choice(brands),
            "list_price": round(random.uniform(5.99, 599.99), 2),
        })
    return pd.DataFrame(products)

# Generates 10,000 customers with weighted tier distribution
def generate_customers(n_customers=10000):
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    customers = []
    for i in range(n_customers):
        signup = fake.date_between(start_date="-3y", end_date="-30d")
        customers.append({
            "customer_id": f"CUST-{i+1:06d}",
            "customer_name": fake.name(),
            "email": fake.email(),
            "signup_date": signup.isoformat(),
            # 40% Bronze, 30% Silver, 20% Gold, 10% Platinum
            "tier": random.choices(tiers, weights=[40, 30, 20, 10])[0],
            "city": fake.city(),
            "country": fake.country_code(),
        })
    return pd.DataFrame(customers)

# Generates orders for one day with intentional duplicates and late arrivals
def generate_orders_for_date(
    processing_date, customer_ids, product_ids,
    orders_per_day=170000, duplicate_rate=0.03, late_arrival_rate=0.05,
):
    payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]
    orders = []

    for _ in range(orders_per_day):
        order_id = str(uuid.uuid4())
        event_date = processing_date

        # 5% of orders are late arrivals from 2-5 days ago
        if random.random() < late_arrival_rate:
            days_late = random.randint(2, 5)
            event_date = processing_date - timedelta(days=days_late)

        order_ts = datetime.combine(event_date, datetime.min.time()) + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        orders.append({
        "order_id": order_id,
        "customer_id": random.choice(customer_ids),
        "product_id": random.choice(product_ids),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(9.99, 499.99), 2),
        "order_timestamp": order_ts.isoformat(),
        "shipping_address": fake.address().replace("\n", ", "),
        "payment_method": random.choice(payment_methods),
        "processing_date": processing_date.isoformat(),
    })

    # Inject 3% duplicates with slightly offset timestamps to simulate replay
    n_duplicates = int(len(orders) * duplicate_rate)
    for i in range(n_duplicates):
        dup = orders[random.randint(0, len(orders) - 1)].copy()
        dup["order_timestamp"] = (
            datetime.fromisoformat(dup["order_timestamp"])
            + timedelta(seconds=random.randint(1, 300))
        ).isoformat()
        orders.append(dup)

    return pd.DataFrame(orders)

# Serializes a DataFrame to Parquet in memory and uploads to S3
def upload_parquet_to_s3(df, s3_bucket, s3_key):
    table = pa.Table.from_pandas(df)
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=buffer.getvalue().to_pybytes())


def get_latest_available_date(s3_bucket):
    """Scans S3 under raw/orders/ to find the latest available processing date."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    
    latest_date = None
    
    print("Checking S3 for existing order partitions...")
    for page in paginator.paginate(Bucket=s3_bucket, Prefix="raw/orders/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Look for keys matching pattern: .../year=YYYY/month=MM/day=DD/...
            if "year=" in key and "month=" in key and "day=" in key:
                try:
                    parts = key.split("/")
                    year = int([p for p in parts if p.startswith("year=")][0].split("=")[1])
                    month = int([p for p in parts if p.startswith("month=")][0].split("=")[1])
                    day = int([p for p in parts if p.startswith("day=")][0].split("=")[1])
                    
                    file_date = datetime(year, month, day).date()
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                except (IndexError, ValueError):
                    continue
                    
    return latest_date


def run(s3_bucket, target_date=None, default_backfill_days=30):
    if target_date:
        dates = [datetime.strptime(target_date, "%Y-%m-%d").date()]
    else:
        latest_s3_date = get_latest_available_date(s3_bucket)
        target_end_date = datetime.now().date() - timedelta(days=1)

        if latest_s3_date:
            print(f"Latest data found in S3 up to: {latest_s3_date}")
            start_date = latest_s3_date + timedelta(days=1)
        else:
            print("No existing order data found in S3. Performing initial load.")
            start_date = target_end_date - timedelta(days=default_backfill_days - 1)

        if start_date > target_end_date:
            print("Data is already up to date with yesterday. No new data to generate.")
            return 0

        # Build inclusive date range from start_date to target_end_date
        total_days = (target_end_date - start_date).days + 1
        dates = [start_date + timedelta(days=i) for i in range(total_days)]

    print("Generating products and customers...")
    products_df = generate_products()
    customers_df = generate_customers()

    product_ids = products_df["product_id"].tolist()
    customer_ids = customers_df["customer_id"].tolist()

    # Upload dimension seeds as single Parquet files
    upload_parquet_to_s3(products_df, s3_bucket, "raw/products/products.parquet")
    upload_parquet_to_s3(customers_df, s3_bucket, "raw/customers/customers.parquet")
    print(f"Uploaded {len(products_df)} products, {len(customers_df)} customers")

    # Generate and upload orders day by day, partitioned by processing date
    total_orders = 0
    for date in sorted(dates):
        orders_df = generate_orders_for_date(
            processing_date=date, customer_ids=customer_ids, product_ids=product_ids,
        )
        s3_key = (
            f"raw/orders/year={date.year}/month={date.month:02d}/"
            f"day={date.day:02d}/orders_{date.isoformat()}.parquet"
        )
        upload_parquet_to_s3(orders_df, s3_bucket, s3_key)
        total_orders += len(orders_df)
        print(f"  {date}: {len(orders_df)} orders uploaded")

    print(f"Total orders generated: {total_orders:,}")
    return total_orders


if __name__ == "__main__":
    load_dotenv()
    
    bucket = os.environ.get("S3_BUCKET", "ecommerce-pipeline-dev")
    print(f"Using S3 Bucket: {bucket}")
    run(s3_bucket=bucket)