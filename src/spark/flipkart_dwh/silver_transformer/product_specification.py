import logging

from pyspark.sql.types import ArrayType, StructType, StringType, StructField, IntegerType

from pyspark.sql import functions as F, Window, DataFrame


logger = logging.getLogger(__file__)


def get_product_specs_item(index, key):
    dict_specs_schema = ArrayType(
        StructType(
            [
                StructField("key", StringType()),
                StructField("value", StringType()),
            ]
        )
    )

    product_specs_json_column = F.regexp_replace(F.col("product_specifications"), pattern="=>", replacement=":")
    product_specs_arr_column = F.from_json(col=F.get_json_object(product_specs_json_column, "$.product_specification"), schema=dict_specs_schema)
    return product_specs_arr_column.getItem(index).getItem(key)


def get_product_specs_field(name):
    return F.coalesce(
        F.when(get_product_specs_item(0, "key") == name, get_product_specs_item(0, "value")),
        F.when(get_product_specs_item(1, "key") == name, get_product_specs_item(1, "value")),
        F.when(get_product_specs_item(2, "key") == name, get_product_specs_item(2, "value")),
        F.when(get_product_specs_item(3, "key") == name, get_product_specs_item(3, "value")),
        F.when(get_product_specs_item(4, "key") == name, get_product_specs_item(4, "value")),
    )


def transform_product_specification(df: DataFrame) -> DataFrame:
    specs_window_spec = Window.orderBy("product_specifications")

    modified_specs_df = df.select(
        F.col("*"),
        F.dense_rank().over(specs_window_spec).alias("specification_id"),
        get_product_specs_field(name="Type").alias("type"),
        get_product_specs_field(name="Ideal For").alias("ideal_for"),
        get_product_specs_field(name="Occasion").alias("occasion"),
        get_product_specs_field(name="Color").alias("color"),
        get_product_specs_field(name="Number of Contents in Sales Package").alias("quantity"),
    )

    return modified_specs_df


def create_dim_specification_df(df: DataFrame) -> DataFrame:
    logger.info("Creating dim_specification_df ...")
    dim_specification_df = (
        df.select(
            F.col("specification_id"),
            F.col("type"),
            F.col("ideal_for"),
            F.col("occasion"),
            F.col("color"),
            F.col("quantity"),
        )
        .distinct()
        .select(F.monotonically_increasing_id().cast(IntegerType()).alias("id"), F.col("*"))
    )
    dim_specification_df.printSchema()

    return dim_specification_df
