import sys

sys.path.append("/sources/spark_app/")  # noqa

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame

from helper import overwrite_to_table


def create_order_performance_report_df(spark: SparkSession):
    order_performance_df = spark.sql(
        """
        SELECT d_date1.year
            ,d_date1.year_month_start
            ,d_date1.month
            ,d_date1.year_week
            ,d_date1.week_start
            ,d_date1.time_of_day
            ,dos.order_status
            ,dc.customer_state
            ,dc.customer_city
            ,COUNT(DISTINCT fact_orders.order_id) AS total_orders
            ,ROUND(SUM(price), 2) AS total_amount
            ,ROUND(AVG(freight_value), 2) AS avg_freight
            ,ROUND(IF(AVG(DATEDIFF(d_date1.timestamp, d_date3.timestamp)) < 0, 0, AVG(DATEDIFF(d_date1.timestamp, d_date3.timestamp))), 2) AS avg_days_delivery_delay
            ,ROUND(AVG(DATEDIFF(d_date1.timestamp, d_date2.timestamp)), 2) AS avg_days_to_deliver
        FROM silver.fact_orders
        JOIN silver.dim_order_status dos ON dos.order_status_id = fact_orders.order_status_id
        LEFT JOIN silver.dim_date d_date1 ON d_date1.date_id = fact_orders.order_delivered_customer_date_id
        LEFT JOIN silver.dim_date d_date2 ON d_date2.date_id = fact_orders.order_purchase_timestamp_id
        LEFT JOIN silver.dim_date d_date3 ON d_date3.date_id = fact_orders.order_estimated_delivery_date_id
        JOIN silver.dim_customers dc ON dc.customer_id = fact_orders.customer_id
        GROUP BY dos.order_status
            ,dc.customer_state
            ,dc.customer_city
            ,d_date1.year
            ,d_date1.year_month_start
            ,d_date1.month
            ,d_date1.year_week
            ,d_date1.week_start
            ,d_date1.time_of_day
    """
    )
    overwrite_to_table(df=order_performance_df, schema_name="gold", table_name="order_performance_report")


def create_order_payment_channel_report_df(spark: SparkSession):
    order_payment_channel_report_df = spark.sql(
        """
        SELECT d_date1.year
            ,d_date1.year_month_start
            ,d_date1.month
            ,dos.order_status
            ,dc.customer_state
            ,dc.customer_city
            ,COUNT(fact_payments.order_id) AS total_transactions
            ,ROUND(SUM(fact_payments.payment_value), 2) AS total_payment
        FROM silver.fact_payments
        JOIN silver.dim_order_status dos ON dos.order_status_id = fact_payments.order_status_id
        LEFT JOIN silver.dim_date d_date1 ON d_date1.date_id = fact_payments.order_delivered_customer_date_id
        JOIN silver.dim_customers dc ON dc.customer_id = fact_payments.customer_id
        GROUP BY dos.order_status
            ,dc.customer_state
            ,dc.customer_city
            ,d_date1.year
            ,d_date1.year_month_start
            ,d_date1.month
    """
    )
    overwrite_to_table(df=order_payment_channel_report_df, schema_name="gold", table_name="order_payment_channel_report")


def main(spark: SparkSession, table_name: str):
    create_func_map = {
        "order_performance_report": create_order_performance_report_df,
        "order_payment_channel_report": create_order_payment_channel_report_df
    }
    function = create_func_map[table_name]
    function(spark=spark)


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
    main(spark=spark_session, table_name=sys.argv[1])
