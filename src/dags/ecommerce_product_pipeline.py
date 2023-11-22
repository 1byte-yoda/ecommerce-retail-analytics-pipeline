from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_app_name = "olist_ecommerce_data_pipeline"


def get_jars() -> str:
    path = Path("/sources/jars")
    jar_list = [str(x) for x in list(path.glob("*.jar"))]
    return ",".join(jar_list)


now = datetime.now()

default_args = {
    "owner": "Mark",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["dmc.markr@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id=spark_app_name,
    description="This is the data pipeline for the Olist Ecommerce Data Platform.",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

bronze_layer_stage = DummyOperator(task_id="bronze_layer_stage", dag=dag)

create_dim_customers_job = SparkSubmitOperator(
    task_id="create_dim_customers_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_dim_customers_job",
    conn_id="spark",
    verbose=False,
    num_executors=1,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="500m",
    jars=get_jars(),
    dag=dag,
    application_args=["dim_customers"]
)

create_dim_sellers_job = SparkSubmitOperator(
    task_id="create_dim_sellers_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_dim_sellers_job",
    conn_id="spark",
    verbose=False,
    num_executors=1,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="500m",
    jars=get_jars(),
    dag=dag,
    application_args=["dim_sellers"]
)

create_dim_product_category_job = SparkSubmitOperator(
    task_id="create_dim_product_category_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_dim_product_category_job",
    conn_id="spark",
    verbose=False,
    num_executors=1,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="500m",
    jars=get_jars(),
    dag=dag,
    application_args=["dim_product_category"]
)

create_dim_order_status_job = SparkSubmitOperator(
    task_id="create_dim_order_status_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_dim_order_status_job",
    conn_id="spark",
    verbose=False,
    num_executors=1,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="500m",
    jars=get_jars(),
    dag=dag,
    application_args=["dim_order_status"]
)

create_dim_date_job = SparkSubmitOperator(
    task_id="create_dim_date_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_dim_date_job",
    conn_id="spark",
    verbose=False,
    num_executors=2,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="1G",
    jars=get_jars(),
    dag=dag,
    application_args=["dim_date"]
)

create_fact_payments_job = SparkSubmitOperator(
    task_id="create_fact_payments_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_fact_payments_job",
    conn_id="spark",
    verbose=False,
    num_executors=2,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="1G",
    jars=get_jars(),
    dag=dag,
    application_args=["fact_payments"]
)

create_fact_reviews_job = SparkSubmitOperator(
    task_id="create_fact_reviews_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_fact_reviews_job",
    conn_id="spark",
    verbose=False,
    num_executors=2,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="1G",
    jars=get_jars(),
    dag=dag,
    application_args=["fact_reviews"]
)

create_fact_orders_job = SparkSubmitOperator(
    task_id="create_fact_orders_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=f"{spark_app_name}_create_fact_orders_job",
    conn_id="spark",
    verbose=False,
    num_executors=2,
    executor_cores=2,
    driver_memory="500m",
    executor_memory="1G",
    jars=get_jars(),
    dag=dag,
    application_args=["fact_orders"]
)

create_order_performance_report_job = SparkSubmitOperator(
    task_id="create_order_performance_report_job",
    application="/sources/spark_app/olist_dwh/gold_transformer/gold_transformer.py",
    name=f"{spark_app_name}_create_order_performance_report_job",
    conn_id="spark",
    verbose=False,
    num_executors=1,
    executor_cores=1,
    driver_memory="500m",
    executor_memory="500m",
    jars=get_jars(),
    dag=dag,
)

create_order_payment_channel_report_job = SparkSubmitOperator(
    task_id="create_order_payment_channel_report_job",
    application="/sources/spark_app/olist_dwh/gold_transformer/gold_transformer.py",
    name=f"{spark_app_name}_create_order_payment_channel_report_job",
    conn_id="spark",
    verbose=False,
    num_executors=1,
    executor_cores=1,
    driver_memory="500m",
    executor_memory="500m",
    jars=get_jars(),
    dag=dag,
)

silver_layer_dim_tables = [
    create_dim_customers_job,
    create_dim_sellers_job,
    create_dim_product_category_job,
    create_dim_order_status_job,
    create_dim_date_job
]

silver_layer_fact_tables = [
    create_fact_payments_job,
    create_fact_reviews_job,
    create_fact_orders_job
]

silver_bridge = DummyOperator(task_id="silver_bridge", dag=dag)

gold_layer_tables = [
    create_order_performance_report_job,
    create_order_payment_channel_report_job
]

gold_bridge = DummyOperator(task_id="gold_bridge", dag=dag)

bronze_layer_stage >> silver_layer_dim_tables >> silver_bridge >> silver_layer_fact_tables >> gold_bridge >> gold_layer_tables  # noqa
