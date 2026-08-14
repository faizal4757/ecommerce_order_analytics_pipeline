import os
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EcommerceOrderTransform")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )


def deduplicate_orders(df):
    # Keep only the earliest record per order_id
    window = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").asc())
    deduped = (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    return deduped


def validate_and_clean(df):
    # Drop rows missing required identifiers
    valid = df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    dropped_count = df.count() - valid.count()
    if dropped_count > 0:
        print(f"WARNING: Dropped {dropped_count} rows with null order_id or customer_id")
    return valid


def enrich_orders(orders_df, products_df):
    # Join product dimension and compute derived columns
    enriched = (
        orders_df
        .join(products_df, on="product_id", how="left")
        .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("is_late_arrival",
                    F.col("order_date") != F.to_date(F.col("processing_date")))
    )
    return enriched


def add_metadata(df, pipeline_run_id):
    # Tag every row with lineage tracking columns
    return (
        df
        .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("_processed_at", F.lit(datetime.utcnow().isoformat()))
    )


def run(s3_bucket, processing_date):
    spark = create_spark_session()
    pipeline_run_id = str(uuid.uuid4())

    print(f"Processing date: {processing_date}")
    print(f"Pipeline run ID: {pipeline_run_id}")

    date_obj = datetime.strptime(processing_date, "%Y-%m-%d").date()
    raw_path = (
        f"s3a://{s3_bucket}/raw/orders/"
        f"year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}/"
    )
    products_path = f"s3a://{s3_bucket}/raw/products/products.parquet"
    output_path = f"s3a://{s3_bucket}/curated/orders/"

    print(f"Reading raw orders from: {raw_path}")
    raw_orders = spark.read.parquet(raw_path)
    raw_count = raw_orders.count()
    print(f"Raw order count: {raw_count:,}")

    products = spark.read.parquet(products_path)

    deduped = deduplicate_orders(raw_orders)
    deduped_count = deduped.count()
    print(f"After deduplication: {deduped_count:,} (removed {raw_count - deduped_count:,})")

    cleaned = validate_and_clean(deduped)
    enriched = enrich_orders(cleaned, products)
    final = add_metadata(enriched, pipeline_run_id)

    
    output_cols = [
        "order_id", "customer_id", "product_id", "product_name",
        "category", "brand", "quantity", "unit_price", "list_price",
        "total_amount", "order_timestamp", "order_date",
        "shipping_address", "payment_method",
        "is_late_arrival", "processing_date",
        "_pipeline_run_id", "_processed_at",
    ]
    final_output = final.select(*output_cols)

    print("Writing curated data partitioned by order_date...")
    final_output.write.mode("overwrite").partitionBy("order_date").parquet(output_path)

    final_count = final_output.count()
    late_count = final_output.filter(F.col("is_late_arrival") == True).count()
    print(f"Curated output: {final_count:,} rows, {late_count:,} late arrivals")

    spark.stop()
    return {"rows_processed": final_count, "duplicates_removed": raw_count - deduped_count}


if __name__ == "__main__":
    bucket = os.environ.get("S3_BUCKET", "ecommerce-pipeline-dev")
    date = os.environ.get("PROCESSING_DATE", "2026-07-10")
    run(s3_bucket=bucket, processing_date=date)