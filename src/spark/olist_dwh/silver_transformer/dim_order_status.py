from pyspark.sql import DataFrame, Column
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def _get_order_status_id() -> Column:
    id_window_spec = Window.orderBy(F.desc(F.col("order_status")))
    return F.row_number().over(id_window_spec)


def create_dim_order_status_df(orders_df: DataFrame) -> DataFrame:
    order_status_id = _get_order_status_id()
    dim_order_status_df = orders_df.select("order_status").distinct().withColumn("order_status_id", order_status_id)
    return dim_order_status_df
