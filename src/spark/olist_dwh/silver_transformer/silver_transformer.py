from logging import getLogger
from typing import Tuple, List

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F

from dim_customers import create_dim_customers_df, get_customers_schema
from dim_sellers import create_dim_sellers_df, get_sellers_schema
from dim_product_category import create_dim_products_df, get_products_schema, get_product_category_name_translation_schema
from dim_order_status import create_dim_order_status_df
from dim_date import create_dim_date_df
from fact_orders import get_orders_schema, get_order_items_schema, create_fact_orders_df
from fact_payments import get_order_payments_schema, create_fact_payments_df
from fact_reviews import get_order_reviews_schema, create_fact_reviews_df


logger = getLogger(__file__)


def get_geolocations_schema() -> StructType:
    return StructType(
        [
            StructField("geolocation_zip_code_prefix", dataType=IntegerType()),
            StructField("geolocation_lat", dataType=DoubleType()),
            StructField("geolocation_lng", dataType=IntegerType()),
            StructField("geolocation_city", dataType=StringType()),
            StructField("geolocation_state", dataType=StringType()),
        ]
    )


def get_min_and_max_date(df_list: List[DataFrame]) -> Tuple[List, List]:
    min_list, max_list = list(), list()
    for df in df_list:
        timestamp_fields = [c for c in df.columns if dict(df.dtypes).get(c) == "timestamp"]
        min_max_dates = [tuple(dates) for f in timestamp_fields for dates in list(df.select(F.min(f), F.max(f)).collect())]
        min_list.extend([n for n, _ in min_max_dates])
        max_list.extend([m for _, m in min_max_dates])
    return min_list, max_list


def get_timestamp_fields(df):
    return [c for c in df.columns if dict(df.dtypes).get(c) == "timestamp"]


def get_date_df(df_list):
    fields = get_timestamp_fields(df_list[0])
    base_df = df_list[0].select(F.explode(F.array(*fields)).alias("timestamp"))

    for df in df_list[1:]:
        fields = [c for c in df.columns if dict(df.dtypes).get(c) == "timestamp"]
        new_df = df.select(F.explode(F.array(*fields)).alias("timestamp"))
        base_df = base_df.union(new_df)
    return base_df.distinct()


def overwrite_to_table(df: DataFrame, schema_name: str, table_name: str):
    df.write.format("delta") \
        .mode("overwrite") \
        .option("path", f"s3a://{schema_name}/{table_name}") \
        .saveAsTable(f"{schema_name}.{table_name}")


def main(spark: SparkSession):
    datalake_bronze_path = "s3a://bronze/olist"
    logger.info("Reading Files from Bronze Layer")

    customers_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_customers_dataset.csv") \
        .schema(get_customers_schema()) \
        .load()

    geolocations_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_geolocation_dataset.csv") \
        .schema(get_geolocations_schema()) \
        .load()

    sellers_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_sellers_dataset.csv") \
        .schema(get_sellers_schema()) \
        .load()

    products_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_products_dataset.csv") \
        .schema(get_products_schema()) \
        .load()

    product_category_name_translation_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/product_category_name_translation.csv") \
        .schema(get_product_category_name_translation_schema()) \
        .load()

    orders_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_orders_dataset.csv") \
        .schema(get_orders_schema()) \
        .load()

    order_items_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_order_items_dataset.csv") \
        .schema(get_order_items_schema()) \
        .load()

    order_payments_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_order_payments_dataset.csv") \
        .schema(get_order_payments_schema()) \
        .load()

    order_reviews_df = spark.read \
        .format("csv") \
        .option("path", f"{datalake_bronze_path}/olist_order_reviews_dataset.csv") \
        .schema(get_order_reviews_schema()) \
        .load()

    date_df = get_date_df(
        df_list=[orders_df, order_items_df, order_reviews_df]
    )

    logger.info("Processing Dim Customers")
    dim_customers_df = create_dim_customers_df(customers_df=customers_df, geolocations_df=geolocations_df)
    overwrite_to_table(df=dim_customers_df, schema_name="silver", table_name="dim_customers")

    logger.info("Processing Dim Sellers")
    dim_sellers_df = create_dim_sellers_df(sellers_df=sellers_df, geolocations_df=geolocations_df)
    overwrite_to_table(df=dim_sellers_df, schema_name="silver", table_name="dim_sellers")

    logger.info("Processing Dim Product Category")
    dim_product_category_df = create_dim_products_df(
        products_df=products_df, product_category_name_translation_df=product_category_name_translation_df
    )
    overwrite_to_table(df=dim_product_category_df, schema_name="silver", table_name="dim_product_category")

    logger.info("Processing Dim Order Status")
    dim_order_status_df = create_dim_order_status_df(orders_df=orders_df)
    overwrite_to_table(df=dim_order_status_df, schema_name="silver", table_name="dim_order_status")

    logger.info("Processing Dim Date")
    dim_date_df = create_dim_date_df(date_df=date_df)
    overwrite_to_table(df=dim_date_df, schema_name="silver", table_name="dim_date")

    logger.info("Processing Fact Payments")
    fact_payments_df = create_fact_payments_df(
        spark=spark, orders_df=orders_df, payments_df=order_payments_df, dim_order_status_df=dim_order_status_df, dim_date_df=dim_date_df
    )
    overwrite_to_table(df=fact_payments_df, schema_name="silver", table_name="fact_payments")

    logger.info("Processing Fact Reviews")
    fact_reviews_df = create_fact_reviews_df(
        reviews_df=order_reviews_df, orders_df=orders_df, dim_date_df=dim_date_df, dim_order_status_df=dim_order_status_df
    )
    overwrite_to_table(df=fact_reviews_df, schema_name="silver", table_name="fact_reviews")

    logger.info("Processing Fact Orders")
    fact_orders_df = create_fact_orders_df(
        spark=spark, orders_df=orders_df, order_items_df=order_items_df, dim_order_status_df=dim_order_status_df, dim_date_df=dim_date_df
    )
    overwrite_to_table(df=fact_orders_df, schema_name="silver", table_name="fact_orders")


if __name__ == '__main__':
    SPARK_URI = "spark://spark:7077"
    HIVE_URI = "thrift://hive-metastore:9083"
    MINIO_URI = "http://minio:9000"

    builder = SparkSession.builder.appName("olist_silver_transformer").master(SPARK_URI) \
        .config("spark.hadoop.hive.metastore.uris", HIVE_URI) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", "datalake") \
        .config("spark.hadoop.fs.s3a.secret.key", "datalake") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URI) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    main(spark=spark)
