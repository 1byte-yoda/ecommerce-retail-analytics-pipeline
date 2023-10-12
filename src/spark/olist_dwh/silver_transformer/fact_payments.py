import sys

sys.path.append("/sources/spark_app/olist_dwh/silver_transformer")  # noqa
sys.path.append("../../../src/spark/olist_dwh/silver_transformer")  # noqa

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from fact_orders import process_dim_date_df


def create_fact_payments_df(
    spark: SparkSession, payments_df: DataFrame, orders_df: DataFrame, dim_order_status_df: DataFrame, dim_date_df: DataFrame
) -> DataFrame:
    payments_df = payments_df.withColumnRenamed("order_id", "p_order_id")
    orders_df = orders_df.withColumnRenamed("order_status", "o_order_status")
    processed_dim_date_df = process_dim_date_df(spark=spark, dim_date_df=dim_date_df, orders_df=orders_df)

    fact_payments_df = payments_df.join(orders_df, on=F.col("order_id") == F.col("p_order_id"), how="inner") \
        .join(dim_order_status_df, on=F.col("order_status") == F.col("o_order_status"), how="inner") \
        .join(processed_dim_date_df, on=F.col("d_order_id") == F.col("order_id"), how="inner") \
        .select(
            "order_id",
            "customer_id",
            "order_status_id",
            "order_purchase_timestamp_id",
            "order_approved_at_id",
            "order_delivered_carrier_date_id",
            "order_delivered_customer_date_id",
            "order_estimated_delivery_date_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        )

    return fact_payments_df
