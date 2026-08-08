# Standard library for unique IDs, randomization, and date math
import uuid
import random
from datetime import datetime, timedelta

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