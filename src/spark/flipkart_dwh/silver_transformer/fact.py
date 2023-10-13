import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, BooleanType


logger = logging.getLogger(__file__)


def create_fact_product_posting(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_product_df ...")

    fact_product_posting_df = df.select(
        F.monotonically_increasing_id().cast(IntegerType()).alias("id"),
        F.col("unique_id"),
        F.col("brand_id"),
        F.col("main_category_id"),
        F.col("category_id"),
        F.col("sub_category_id"),
        F.col("product_id"),
        F.col("specification_id"),
        F.col("crawl_date_id"),
        F.col("retail_price"),
        F.col("discounted_price"),
        F.col("is_fk_advantage_product").cast(BooleanType()),
        F.col("product_rating"),
        F.col("overall_rating"),
    )
    fact_product_posting_df.printSchema()

    return fact_product_posting_df
