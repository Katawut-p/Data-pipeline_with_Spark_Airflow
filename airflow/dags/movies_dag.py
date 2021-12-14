from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
class Config:
    # Spark
    spark_master = "spark://spark:7077"
    postgres_driver_jar = "/usr/postgres_jar/postgresql-42.3.1.jar"
    # CSV output
    movies_file = "/usr/local/spark/resources/movies/movies.csv"
    ratings_file = "/usr/local/spark/resources/movies/ratings.csv"
    # postgres 
    postgres_db = "jdbc:postgresql://postgres/test"
    postgres_user = "test"
    postgres_pwd = "postgres"

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
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id = "movies", 
        description = "",
        default_args = default_args, 
        schedule_interval = timedelta(1)
    )

start = DummyOperator(task_id = "start", dag = dag)

spark_job_load_postgres = SparkSubmitOperator(
    task_id = "spark_job_load_postgres",
    application = "/usr/local/spark/app/movies/extract_data.py",
    name = "load-postgres",
    conn_id = "spark_default",
    verbose = 1,
    conf = {"spark.master" : Config.spark_master},
    application_args = [Config.movies_file,
                        Config.ratings_file,
                        Config.postgres_db,
                        Config.postgres_user,
                        Config.postgres_pwd],
    jars = Config.postgres_driver_jar,
    driver_class_path = Config.postgres_driver_jar,
    dag = dag)

spark_job_read_postgres = SparkSubmitOperator(
    task_id = "spark_job_read_postgres",
    application = "/usr/local/spark/app/movies/read_data.py",
    name = "read-postgres",
    conn_id = "spark_default",
    verbose = 1,
    conf = {"spark.master": Config.spark_master},
    application_args = [Config.postgres_db,
                        Config.postgres_user,
                        Config.postgres_pwd],
    jars = Config.postgres_driver_jar,
    driver_class_path = Config.postgres_driver_jar,
    dag = dag)

end = DummyOperator(task_id = "end", dag = dag)

start >> spark_job_load_postgres >> spark_job_read_postgres >> end