from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def get_product_category_name_translation_schema() -> StructType:
    return StructType(
        [
            StructField("product_category_name", dataType=StringType()),
            StructField("product_category_name_english", dataType=StringType())
        ]
    )


def get_products_schema() -> StructType:
    return StructType(
        [
            StructField("product_id", dataType=StringType()),
            StructField("product_category_name", dataType=StringType()),
            StructField("product_name_lenght", dataType=IntegerType()),
            StructField("product_description_lenght", dataType=IntegerType()),
            StructField("product_photos_qty", dataType=IntegerType()),
            StructField("product_weight_g", dataType=IntegerType()),
            StructField("product_length_cm", dataType=IntegerType()),
            StructField("product_height_cm", dataType=IntegerType()),
            StructField("product_width_cm", dataType=IntegerType()),
        ]
    )


def create_dim_products_df(products_df: DataFrame, product_category_name_translation_df: DataFrame) -> DataFrame:
    product_category_name_translation_df = product_category_name_translation_df.withColumnRenamed("product_category_name", "trans_category_name")
    return products_df.join(
        product_category_name_translation_df,
        on=F.col("product_category_name") == F.col("trans_category_name"),
        how="left"
    ).selectExpr("product_id", "product_category_name", "product_category_name_english")
