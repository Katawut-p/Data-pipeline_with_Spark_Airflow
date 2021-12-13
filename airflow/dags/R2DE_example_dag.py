from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
# Spark
spark_master = "spark://spark:7077"

# Source
source_path = '/usr/local/spark/resources/R2DE_example/ws2_data.csv'

# Destination
destination_path = '/usr/local/spark/resources/R2DE_example/csv_output/Cleaned_data'

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "katawut.p",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["katawut.p@outlook.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
        dag_id="E2DE_example", 
        description="",
        default_args=default_args, 
        schedule_interval='0 8 * * *'
    )

read_data = SparkSubmitOperator(
    task_id = "cleaning_data_example",
    application = "/usr/local/spark/app/R2DE_example/cleaning_data.py",
    name = "cleaning_data_example",
    conn_id = "spark_default",
    verbose = 1,
    conf = {"spark.master" : spark_master},
    application_args = [source_path,
                        destination_path],
    dag = dag)