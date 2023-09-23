import pyspark
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import uuid

# .config("spark.hadoop.metastore.catalog.default", "hive") \
builder = SparkSession.builder.appName("delta") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key","datalake") \
    .config("spark.hadoop.fs.s3a.secret.key","datalake") \
    .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
delta_lake_container_path = "s3a://tmp"
df = spark.read.csv(f"{delta_lake_container_path}/results.csv",header=True)
df = df.withColumnRenamed('date','dt')

table_name = "results_delta"
path = f"{delta_lake_container_path}/{table_name}"
print(path)
df.write.format("delta").partitionBy("countryCountry").mode('overwrite').option("path", path).saveAsTable(f"delta_lake.{table_name}")
