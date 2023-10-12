from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


def process_dim_date_df(spark: SparkSession, dim_date_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    dim_date_df.createOrReplaceTempView("dim_date")
    orders_df.createOrReplaceTempView("orders")

    return spark.sql(
        """
            SELECT  o.order_id AS d_order_id
                    ,d2.date_id AS order_purchase_timestamp_id
                    ,d3.date_id AS order_approved_at_id
                    ,d4.date_id AS order_delivered_carrier_date_id
                    ,d5.date_id AS order_delivered_customer_date_id
                    ,d6.date_id AS order_estimated_delivery_date_id
            FROM orders AS o
            LEFT JOIN dim_date AS d2 ON d2.unix_timestamp = UNIX_TIMESTAMP(o.order_purchase_timestamp)
            LEFT JOIN dim_date AS d3 ON d3.unix_timestamp = UNIX_TIMESTAMP(o.order_approved_at)
            LEFT JOIN dim_date AS d4 ON d4.unix_timestamp = UNIX_TIMESTAMP(o.order_delivered_carrier_date)
            LEFT JOIN dim_date AS d5 ON d5.unix_timestamp = UNIX_TIMESTAMP(o.order_delivered_customer_date)
            LEFT JOIN dim_date AS d6 ON d6.unix_timestamp = UNIX_TIMESTAMP(o.order_estimated_delivery_date)
        """
    )


def create_fact_orders_df(
    spark: SparkSession, orders_df: DataFrame, order_items_df: DataFrame, dim_order_status_df: DataFrame, dim_date_df: DataFrame
) -> DataFrame:
    order_items_df = order_items_df.withColumnRenamed("order_id", "order_item_order_id")
    orders_df = orders_df.withColumnRenamed("order_status", "o_order_status")
    processed_dim_date_df = process_dim_date_df(spark=spark, dim_date_df=dim_date_df, orders_df=orders_df)

    fact_orders_df = orders_df.join(order_items_df, on=F.col("order_id") == F.col("order_item_order_id"), how="inner") \
        .join(dim_order_status_df, on=F.col("o_order_status") == F.col("order_status"), how="inner") \
        .join(dim_date_df.alias("shipping_limit"), on=dim_date_df["unix_timestamp"] == F.unix_timestamp("shipping_limit_date")) \
        .join(processed_dim_date_df, on=F.col("d_order_id") == F.col("order_id"), how="inner") \
        .selectExpr(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_id",
            "order_status_id",
            "shipping_limit.date_id AS shipping_limit_date_id",
            "order_purchase_timestamp_id",
            "order_approved_at_id",
            "order_delivered_carrier_date_id",
            "order_delivered_customer_date_id",
            "order_estimated_delivery_date_id",
            "price",
            "freight_value"
        )

    return fact_orders_df
