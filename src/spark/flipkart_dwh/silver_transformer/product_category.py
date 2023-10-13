from pyspark.sql import functions as F, DataFrame, Window
from pyspark.sql.types import ArrayType, StringType, IntegerType
import logging


logger = logging.getLogger(__file__)


def _convert_category_tree_to_list(category_df: DataFrame) -> DataFrame:
    category_tree_column = F.from_json(F.col("product_category_tree"), ArrayType(elementType=StringType())).getItem(0)
    return category_df.withColumn("product_category_tree", F.split(category_tree_column, ">>"))


def transform_product_category(df: DataFrame) -> DataFrame:
    modified_category_df = _convert_category_tree_to_list(category_df=df)

    main_category_window_spec = Window.orderBy("main_category")
    category_window_spec = Window.orderBy("category")
    sub_category_window_spec = Window.orderBy("sub_category")

    modified_category_df = modified_category_df.select(
        F.col("*"),
        F.col("product_category_tree").getItem(0).alias("main_category"),
        F.col("product_category_tree").getItem(1).alias("category"),
        F.col("product_category_tree").getItem(2).alias("sub_category"),
    ).select(
        F.col("*"),
        F.dense_rank().over(main_category_window_spec).alias("main_category_id"),
        F.dense_rank().over(category_window_spec).alias("category_id"),
        F.dense_rank().over(sub_category_window_spec).alias("sub_category_id"),
    )

    return modified_category_df


def create_dim_main_category_df(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_main_category_df ...")

    dim_main_category_df = (
        df.select(F.col("main_category_id"), F.col("main_category").alias("name"))
        .distinct()
        .select(F.monotonically_increasing_id().cast(IntegerType()).alias("id"), F.col("*"))
    )
    dim_main_category_df.printSchema()

    return dim_main_category_df


def create_dim_category_df(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_category_df ...")

    dim_category_df = (
        df.select(F.col("category_id"), F.col("category").alias("name"))
        .distinct()
        .select(F.monotonically_increasing_id().cast(IntegerType()).alias("id"), F.col("*"))
    )
    dim_category_df.printSchema()

    return dim_category_df


def create_dim_sub_category_df(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_sub_category_df ...")

    dim_sub_category_df = (
        df.select(F.col("sub_category_id"), F.col("sub_category").alias("name"))
        .distinct()
        .select(F.monotonically_increasing_id().cast(IntegerType()).alias("id"), F.col("*"))
    )
    dim_sub_category_df.printSchema()

    return dim_sub_category_df
