import logging

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import IntegerType


logger = logging.getLogger(__file__)


def transform_brand(df: DataFrame) -> DataFrame:
    brand_window_spec = Window.orderBy("brand")
    return df.withColumn("brand_id", F.dense_rank().over(brand_window_spec))


def create_dim_brand_df(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_brand_df ...")

    dim_brand_df = (
        df.select(F.col("brand_id"), F.col("brand")).distinct().select(F.monotonically_increasing_id().cast(IntegerType()).alias("id"), F.col("*"))
    )

    dim_brand_df.printSchema()

    return dim_brand_df
