import sys
from logging import getLogger

sys.path.append("/sources/spark_app/")  # noqa

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from dim_customers import create_dim_customers_df, get_customers_schema
from dim_sellers import create_dim_sellers_df, get_sellers_schema
from dim_product_category import create_dim_products_df, get_products_schema, get_product_category_name_translation_schema
from dim_order_status import create_dim_order_status_df
from dim_date import create_dim_date_df
from fact_orders import get_orders_schema, get_order_items_schema, create_fact_orders_df
from fact_payments import get_order_payments_schema, create_fact_payments_df
from fact_reviews import get_order_reviews_schema, create_fact_reviews_df
from helper import get_geolocations_schema, overwrite_to_table, get_date_df


logger = getLogger(__file__)


def main(spark: SparkSession):
    datalake_bronze_path = "s3a://bronze/olist"
    logger.info("Reading Files from Bronze Layer")

    customers_df = (
        spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_customers_dataset.csv").schema(get_customers_schema()).load()
    )

    geolocations_df = (
        spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_geolocation_dataset.csv").schema(get_geolocations_schema()).load()
    )

    sellers_df = spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_sellers_dataset.csv").schema(get_sellers_schema()).load()

    products_df = spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_products_dataset.csv").schema(get_products_schema()).load()

    product_category_name_translation_df = (
        spark.read.format("csv")
        .option("path", f"{datalake_bronze_path}/product_category_name_translation.csv")
        .schema(get_product_category_name_translation_schema())
        .load()
    )

    orders_df = spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_orders_dataset.csv").schema(get_orders_schema()).load()

    order_items_df = (
        spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_order_items_dataset.csv").schema(get_order_items_schema()).load()
    )

    order_payments_df = (
        spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_order_payments_dataset.csv").schema(get_order_payments_schema()).load()
    )

    order_reviews_df = (
        spark.read.format("csv").option("path", f"{datalake_bronze_path}/olist_order_reviews_dataset.csv").schema(get_order_reviews_schema()).load()
    )

    date_df = get_date_df(df_list=[orders_df, order_items_df, order_reviews_df])

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


if __name__ == "__main__":
    SPARK_URI = "spark://spark:7077"
    HIVE_URI = "thrift://hive-metastore:9083"
    MINIO_URI = "http://minio:9000"

    builder = (
        SparkSession.builder.appName("olist_silver_transformer")
        .master(SPARK_URI)
        .config("spark.hadoop.hive.metastore.uris", HIVE_URI)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", "datalake")
        .config("spark.hadoop.fs.s3a.secret.key", "datalake")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URI)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    spark_session = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    main(spark=spark_session)
