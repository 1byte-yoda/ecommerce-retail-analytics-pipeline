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
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="2lux_ecommerce_data_pipeline",
    description="This is the data pipeline for the Olist Ecommerce Data Platform.",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

bronze_ecommerce_product = DummyOperator(task_id="bronze_ecommerce_job", dag=dag)

silver_ecommerce_transformer_job = SparkSubmitOperator(
    task_id="silver_ecommerce_transformer_job",
    application="/sources/spark_app/olist_dwh/silver_transformer/silver_transformer.py",
    name=spark_app_name,
    conn_id="spark",
    verbose=False,
    num_executors=2,
    executor_cores=1,
    driver_memory="500m",
    executor_memory="1700m",
    jars=get_jars(),
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

bronze_ecommerce_product >> silver_ecommerce_transformer_job >> end
