from datetime import datetime, timedelta
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType, DoubleType


def generate_series(spark: SparkSession, start: str, stop: str, interval: int):
    start, stop = spark.createDataFrame([(start, stop)], ("start", "stop")).select(
        [F.col(c).cast("timestamp").cast("long") for c in ("start", "stop")]
    ).first()

    return spark.range(start, stop + 1, interval).select(
        F.row_number().over(Window.orderBy("id")).alias("date_id"),
        F.unix_timestamp(F.col("id").cast("timestamp")).alias("unix_timestamp"),
        F.col("id").cast("timestamp").alias("timestamp")
    )


def get_date_generated_schema() -> StructType:
    return StructType(
        [
            StructField("date_id", IntegerType(), nullable=False),
            StructField("unix_timestamp", DoubleType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ]
    )


def _create_initial_dim_date_df(spark: SparkSession, min_dates: List[datetime], max_dates: List[datetime]):
    start_date, end_date = min(min_dates), max(max_dates)
    return generate_series(spark=spark, start=start_date.isoformat(), stop=end_date.isoformat(), interval=1)


def create_dim_date_df(spark: SparkSession, min_dates: List[datetime], max_dates: List[datetime]) -> DataFrame:
    dim_date_df = _create_initial_dim_date_df(spark=spark, min_dates=min_dates, max_dates=max_dates)
    udf_get_week_day_type = F.udf(lambda x: "weekend" if x in ("Sat", "Sun") else "weekday", StringType())
    dim_date_df = dim_date_df.withColumn("year", F.date_format(F.col("timestamp"), "yyyy")) \
        .withColumn("quarter", F.date_format(F.col("timestamp"), "Q")) \
        .withColumn("quarter_name", F.date_format(F.col("timestamp"), "qq")) \
        .withColumn("month", F.date_format(F.col("timestamp"), "M")) \
        .withColumn("month_name", F.date_format(F.col("timestamp"), "MMM")) \
        .withColumn("year_month", F.date_format(F.col("timestamp"), "yyyyMM")) \
        .withColumn("year_week", F.concat(F.date_format(F.col("timestamp"), "yyyy"), F.weekofyear(F.col("timestamp")))) \
        .withColumn("week_day_name", F.date_format(F.col("timestamp"), "EEE")) \
        .withColumn("week_day_type", udf_get_week_day_type(F.date_format(F.col("timestamp"), "EEE"))) \
        .withColumn("timezone", F.date_format(F.col("timestamp"), "zzz")) \
        .withColumn(
            "time_of_day",
            F.when(
                F.date_format(F.col("timestamp"), "HH:mm").between("00:01", "05:00"), "Dawn"
            ).when(
                F.date_format(F.col("timestamp"), "HH:mm").between("05:01", "11:59"), "Morning"
            ).when(
                F.date_format(F.col("timestamp"), "HH:mm").between("12:00", "18:00"), "Afternoon"
            ).when(
                F.date_format(F.col("timestamp"), "HH:mm").between("18:01", "24:00"), "Night"
            )
        )
    return dim_date_df
