from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


def get_order_reviews_schema() -> StructType:
    return StructType(
        [
            StructField("review_id", StringType()),
            StructField("order_id", StringType()),
            StructField("review_score", FloatType()),
            StructField("review_comment_title", StringType()),
            StructField("review_comment_message", StringType()),
            StructField("review_creation_date", TimestampType()),
            StructField("review_answer_timestamp", TimestampType())
        ]
    )


def create_fact_reviews_df(reviews_df: DataFrame, orders_df: DataFrame, dim_date_df: DataFrame, dim_order_status_df: DataFrame) -> DataFrame:
    orders_df = orders_df.withColumnRenamed("order_status", "o_order_status")
    reviews_df = reviews_df.withColumnRenamed("order_id", "v_order_id")

    fact_reviews_df = reviews_df.join(orders_df, on=F.col("order_id") == F.col("v_order_id"), how="inner") \
        .join(dim_order_status_df, on=F.col("order_status") == F.col("o_order_status")) \
        .join(dim_date_df.alias("d1"), on=F.col("d1.unix_timestamp") == F.unix_timestamp("review_creation_date"), how="inner") \
        .join(dim_date_df.alias("d2"), on=F.col("d2.unix_timestamp") == F.unix_timestamp("review_answer_timestamp"), how="inner") \
        .selectExpr(
            "review_id",
            "order_id",
            "customer_id",
            "order_status_id",
            "d1.date_id AS review_creation_date_id",
            "d2.date_id AS review_answer_timestamp_id",
            "review_score",
            "review_comment_title",
            "review_comment_message"
        )

    return fact_reviews_df
