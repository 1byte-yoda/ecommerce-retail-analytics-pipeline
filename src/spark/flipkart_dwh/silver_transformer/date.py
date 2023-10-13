import logging

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import IntegerType


logger = logging.getLogger(__file__)


def transform_date(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("*"),
        F.dense_rank().over(Window.orderBy("crawl_timestamp")).alias("crawl_date_id"),
        F.year("crawl_timestamp").alias("year"),
        F.month("crawl_timestamp").alias("month"),
        F.dayofmonth("crawl_timestamp").alias("day"),
        F.hour("crawl_timestamp").alias("hour"),
        F.minute("crawl_timestamp").alias("minute"),
        F.date_format("crawl_timestamp", "z").alias("timezone"),
        F.date_format("crawl_timestamp", "E").alias("day_name"),
        F.date_format("crawl_timestamp", "MMM").alias("month_name"),
        F.weekofyear("crawl_timestamp").alias("year_week"),
        F.quarter("crawl_timestamp").alias("quarter")
    )


def create_dim_date_df(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_date_df ...")

    dim_date_df = df.select(
        F.col("crawl_date_id").alias("id"),
        F.col("year"),
        F.col("month"),
        F.col("day"),
        F.col("hour"),
        F.col("minute"),
        F.col("timezone"),
        F.col("day_name"),
        F.col("month_name"),
        F.col("year_week"),
        F.col("quarter"),
    ).distinct()

    dim_date_df.printSchema()

    return dim_date_df
