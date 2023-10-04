from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from pathlib import Path


def get_jars() -> str:
    path = Path("/sources/jars")
    jar_list = [str(x) for x in list(path.glob("*.jar"))]
    return ",".join(jar_list)

###############################################
# Parameters
###############################################
spark_master = "spark://172.22.0.3:7077"
spark_app_name = "Spark Hello World"
#file_path = "/usr/local/spark/resources/data/airflow.cfg"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-test", 
        description="This DAG runs a simple Pyspark app.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/sources/spark_app/sample_app.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark",
    verbose=False,
    jars=get_jars(),
    # packages="io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.392",
    # conf={"spark.master": spark_master},
    num_executors=2,
    executor_cores=1,
    driver_memory="500m",
    executor_memory="1G",
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

# io.delta#delta-core_2.12;2.4.0 in central
# io.delta#delta-storage;2.4.0 in central
# org.antlr#antlr4-runtime;4.9.3 in central
# org.apache.hadoop#hadoop-aws;3.3.1 in central
# org.wildfly.openssl#wildfly-openssl;1.0.7.Final in central
# com.amazonaws#aws-java-sdk-bundle;1.12.392 in central

start >> spark_job >> end