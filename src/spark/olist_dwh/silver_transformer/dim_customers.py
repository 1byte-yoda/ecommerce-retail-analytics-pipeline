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


def create_dim_customers_df(customers_df: DataFrame, geolocations_df: DataFrame) -> DataFrame:
    return (
        customers_df.join(geolocations_df, on=F.col("customer_zip_code_prefix") == F.col("geolocation_zip_code_prefix"), how="left")
        .selectExpr(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "FIRST(geolocation_lat) OVER(PARTITION BY customer_zip_code_prefix) AS customer_location_latitude",
            "FIRST(geolocation_lng) OVER(PARTITION BY customer_zip_code_prefix) AS customer_location_longitude",
        )
        .distinct()
    )
