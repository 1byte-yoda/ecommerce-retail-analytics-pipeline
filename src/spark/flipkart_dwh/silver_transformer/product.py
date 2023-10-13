import logging

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType

logger = logging.getLogger(__file__)


def create_dim_product_df(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_product_df ...")

    dim_product_df = (
        df.select(
            F.col("product_id"),
            F.col("product_url"),
            F.col("product_name"),
            F.col("description"),
            F.from_json(F.col("image"), ArrayType(StringType())).alias("image"),
        )
        .distinct()
        .select(F.monotonically_increasing_id().cast(IntegerType()).alias("id"), F.col("*"))
    )

    dim_product_df.printSchema()

    return dim_product_df
