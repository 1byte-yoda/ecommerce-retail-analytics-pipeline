from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType


def get_sellers_schema() -> StructType:
    return StructType(
        [
            StructField(name="seller_id", dataType=StringType()),
            StructField(name="seller_zip_code_prefix", dataType=StringType()),
            StructField(name="seller_city", dataType=StringType()),
            StructField(name="seller_state", dataType=StringType()),
        ]
    )


def create_dim_sellers_df(sellers_df: DataFrame, geolocations_df: DataFrame) -> DataFrame:
    return (
        sellers_df.join(geolocations_df, on=F.col("seller_zip_code_prefix") == F.col("geolocation_zip_code_prefix"), how="left")
        .selectExpr(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "FIRST(geolocation_lat) OVER(PARTITION BY seller_zip_code_prefix) AS seller_location_latitude",
            "FIRST(geolocation_lng) OVER(PARTITION BY seller_zip_code_prefix) AS seller_location_longitude",
        )
        .distinct()
    )
