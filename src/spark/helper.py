from dataclasses import dataclass
from pathlib import Path
from typing import List

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession, DataFrame


@dataclass
class MinioCredential:
    access_key: str
    secret_key: str


def get_jar_packages() -> str:
    return ",".join([str(x) for x in Path("../jars").glob("*.jar")])


def create_spark_session(app_name: str, spark_uri: str, hive_uri: str, minio_uri: str, minio_credential: MinioCredential) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_uri)
        .config("hive.metastore.uris", hive_uri)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", minio_credential.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_credential.secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", minio_uri)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    return spark


def get_products_csv_schema() -> StructType:
    return StructType(
        [
            StructField("unique_id", StringType(), False),
            StructField("crawl_timestamp", StringType()),
            StructField("product_url", StringType()),
            StructField("product_name", StringType()),
            StructField("product_category_tree", StringType()),
            StructField("product_id", StringType()),
            StructField("retail_price", FloatType()),
            StructField("discounted_price", FloatType()),
            StructField("image", StringType()),
            StructField("is_fk_advantage_product", StringType()),
            StructField("description", StringType()),
            StructField("product_rating", StringType()),
            StructField("overall_rating", StringType()),
            StructField("brand", StringType()),
            StructField("product_specifications", StringType()),
        ]
    )


def overwrite_to_table(df: DataFrame, schema_name: str, table_name: str):
    df.write.format("delta").mode("overwrite").option("path", f"s3a://{schema_name}/{table_name}").saveAsTable(f"{schema_name}.{table_name}")


def get_geolocations_schema() -> StructType:
    return StructType(
        [
            StructField("geolocation_zip_code_prefix", dataType=IntegerType()),
            StructField("geolocation_lat", dataType=DoubleType()),
            StructField("geolocation_lng", dataType=IntegerType()),
            StructField("geolocation_city", dataType=StringType()),
            StructField("geolocation_state", dataType=StringType()),
        ]
    )


def get_timestamp_fields(df: DataFrame) -> List[str]:
    return [c for c in df.columns if dict(df.dtypes).get(c) == "timestamp"]


def get_date_df(df_list: List[DataFrame]) -> DataFrame:
    fields = get_timestamp_fields(df_list[0])
    base_df = df_list[0].select(F.explode(F.array(*fields)).alias("timestamp"))

    for df in df_list[1:]:
        fields = [c for c in df.columns if dict(df.dtypes).get(c) == "timestamp"]
        new_df = df.select(F.explode(F.array(*fields)).alias("timestamp"))
        base_df = base_df.union(new_df)
    return base_df.distinct()


def merge_to_table(spark: SparkSession, input_df: DataFrame, schema_name: str, table_name: str, partition_column: str, join_condition: str):
    if spark.catalog.tableExists(tableName=f"{schema_name}.{table_name}"):
        delta_table = DeltaTable.forPath(path=f"s3a://{schema_name}/{table_name}", sparkSession=spark)
        delta_table.alias("target").merge(
            source=input_df.alias("src"), condition=join_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:
        input_df.write.format("delta").mode("overwrite").partitionBy(partition_column).saveAsTable(f"{schema_name}.{table_name}")
