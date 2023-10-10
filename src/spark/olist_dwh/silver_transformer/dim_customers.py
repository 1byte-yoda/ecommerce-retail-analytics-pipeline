from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def get_customers_schema() -> StructType:
    return StructType(
        [
            StructField("customer_id", dataType=StringType()),
            StructField("customer_unique_id", dataType=StringType()),
            StructField("customer_zip_code_prefix", dataType=IntegerType()),
            StructField("customer_city", dataType=StringType()),
            StructField("customer_state", dataType=StringType()),
        ]
    )


def create_dim_customers(customers_df: DataFrame, geolocations_df: DataFrame) -> DataFrame:
    return customers_df.join(geolocations_df, on=F.col("customers_zip_code_prefix") == F.col("geolocation_zip_code_prefix"), how="inner") \
        .selectExpr(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "geolocation_city AS customer_city",
        "geolocation_state AS customer_state",
        "geolocation_lat AS seller_location_latitude",
        "geolocation_lng AS seller_location_longitude"
    )
