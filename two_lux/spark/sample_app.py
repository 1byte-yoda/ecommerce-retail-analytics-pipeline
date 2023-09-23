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
# spark.catalog.setCurrentCatalog("spark_catalog")
df = spark.read.csv("s3a://delta-lake/results.csv",header=True)
df = df.withColumnRenamed('date','dt')
myuuid = uuid.uuid4()
table_name = "results_delta"
path = (f"s3a://delta-lake/{table_name}").format(myuuid)
print(path)
df.write.format("delta").partitionBy("countryCountry").mode('overwrite').option("path", path).saveAsTable(f"delta_lake.{table_name}")
