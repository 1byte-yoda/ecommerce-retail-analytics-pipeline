from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType


def create_dim_date_df(date_df: DataFrame) -> DataFrame:
    udf_get_week_day_type = F.udf(lambda x: "weekend" if x in ("Sat", "Sun") else "weekday", StringType())
    dim_date_df = date_df.withColumn("date_id", F.row_number().over(Window.orderBy("timestamp"))) \
        .withColumn("year", F.date_format(F.col("timestamp"), "yyyy").cast(IntegerType())) \
        .withColumn("unix_timestamp", F.unix_timestamp(F.col("timestamp").cast("timestamp"))) \
        .withColumn("quarter", F.date_format(F.col("timestamp"), "Q").cast(IntegerType())) \
        .withColumn("quarter_name", F.date_format(F.col("timestamp"), "qqq")) \
        .withColumn("month", F.date_format(F.col("timestamp"), "M").cast(IntegerType())) \
        .withColumn("month_name", F.date_format(F.col("timestamp"), "MMM")) \
        .withColumn("year_month", F.date_format(F.col("timestamp"), "yyyyMM").cast(IntegerType())) \
        .withColumn("year_month_start", F.date_format(F.col("timestamp"), "yyyy-MM-01").cast(TimestampType())) \
        .withColumn("year_week", F.concat(F.date_format(F.col("timestamp"), "yyyy"), F.weekofyear(F.col("timestamp"))).cast(IntegerType())) \
        .withColumn("week_start", F.date_sub(F.next_day(F.col("timestamp"), "SUN"), 6)) \
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
