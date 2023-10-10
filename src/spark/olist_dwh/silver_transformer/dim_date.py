from datetime import datetime, timedelta
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType, LongType


def generate_date_list(min_dates: List[datetime], max_dates: List[datetime]) -> List[Tuple]:
    start_date, end_date = max(max_dates), min(min_dates)
    n_days = (end_date - start_date).days
    return [(x + 1, (start_date + timedelta(days=x)).timestamp(), start_date + timedelta(days=x)) for x in range(0, n_days)]


def get_date_generated_schema() -> StructType:
    return StructType(
        [
            StructField("date_id", IntegerType(), nullable=False),
            StructField("unix_timestamp", LongType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ]
    )


def _create_initial_dim_date_df(spark: SparkSession, min_dates: List[datetime], max_dates: List[datetime]):
    date_generated = generate_date_list(min_dates=min_dates, max_dates=max_dates)
    schema = get_date_generated_schema()
    return spark.createDataFrame(data=date_generated, schema=schema)


def create_dim_date_df(spark: SparkSession, min_dates: List[datetime], max_dates: List[datetime]) -> DataFrame:
    dim_date_df = _create_initial_dim_date_df(spark=spark, min_dates=min_dates, max_dates=max_dates)
    udf_get_week_day_type = F.udf(lambda x: "weekend" if x in ("Sat", "Sun") else "weekday", StringType())

    dim_date_df = dim_date_df.withColumn("year", F.date_format(F.col("timestamp"), "yyyy")) \
        .withColumn("quarter", F.date_format(F.col("timestamp"), "Q")) \
        .withColumn("quarter_name", F.date_format(F.col("timestamp"), "qq")) \
        .withColumn("month", F.date_format(F.col("timestamp"), "M")) \
        .withColumn("month_name", F.date_format(F.col("timestamp"), "MMM")) \
        .withColumn("year_month", F.date_format(F.col("timestamp"), "yyyy-MM")) \
        .withColumn("year_week", F.concat(F.date_format(F.col("timestamp"), "yyyy"), "-", F.weekofyear(F.col("timestamp")))) \
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
                F.date_format(F.col("order_purchase_timestamp"), "HH:mm").between("18:01", "24:00"), "Night"
            )
        )
    return dim_date_df
