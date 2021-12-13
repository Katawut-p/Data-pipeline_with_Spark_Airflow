from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

###############################################
# Parameters
###############################################
# Spark
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/postgres_jar/postgresql-42.3.1.jar"

# CSV output
final_df_path = '/usr/local/spark/resources/covid19_global_time_series/csv_output/covid19_global_time_series'
thai_df_path = '/usr/local/spark/resources/covid19_global_time_series/csv_output/thailand_covid19'

# postgres 
postgres_db = "test"
postgres_spark_url_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

###############################################
# use pandas import and melt data
###############################################
def get_melted_data(url, case_type):
        df = pd.read_csv(url)
        df['Province/State']= df['Province/State'].fillna('-')
        melted_df = df.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'])
        melted_df.rename(columns={'Province/State':'Province_and_State', 'Country/Region':'Country', 'variable':'Date', 'value': case_type}, inplace=True)
    
        return melted_df

def get_data():
    # Source path
    confirmed_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    death_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    # CSSE stopped to maintain the recovered cases
    # recovered_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

    # Applying the Spark function
    # df_csv_sample = moduleExample.pysparkFunctions.sample_df(df_csv, 0.1)
    confirm_df = get_melted_data(confirmed_url, 'Confirmed')
    death_df = get_melted_data(death_url, 'Death')
    # recovered_dt = meltData.pandasFunctions.get_melted_data(recovered_url, 'Recovered')

    # Join data Confirm, Death and Recovery
    # final_dt = confirm_dt.join(recovered_dt['Recovered']).join(death_dt['Death'])
    final_df = confirm_df.join(death_df['Death'])

    # Save to Postgres DB
    engine_url = "postgresql://{}:{}@postgres:5432/{}".format(postgres_user, postgres_pwd, postgres_db)
    print(engine_url)
    engine = create_engine(engine_url)
    final_df.to_sql('covid19_global_data', engine, if_exists='replace')

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
        dag_id="covid19_global_time_series", 
        description="",
        default_args=default_args, 
        schedule_interval='0 12 * * *'
    )

start = DummyOperator(task_id = "start", dag = dag)

get_data_from_source = PythonOperator(
        task_id='get_data_from_source', 
        python_callable=get_data,
        dag=dag
    )

read_data = SparkSubmitOperator(
    task_id = "read_data",
    application = "/usr/local/spark/app/covid19_global_time_series/read_data.py",
    name = "read_data",
    conn_id = "spark_default",
    verbose = 1,
    conf = {"spark.master" : spark_master},
    application_args = [postgres_spark_url_db,
                        postgres_user,
                        postgres_pwd,
                        final_df_path],
    jars = postgres_driver_jar,
    driver_class_path = postgres_driver_jar,
    dag = dag)

read_data_thai = SparkSubmitOperator(
    task_id = "read_data_thai",
    application = "/usr/local/spark/app/covid19_global_time_series/read_data_thai.py",
    name = "read_data_thai",
    conn_id = "spark_default",
    verbose = 1,
    conf ={"spark.master" : spark_master},
    application_args = [final_df_path, thai_df_path],
    jars = postgres_driver_jar,
    driver_class_path = postgres_driver_jar,
    dag = dag)

end = DummyOperator(task_id="end", dag=dag)

start >> get_data_from_source >> read_data >> read_data_thai >> end