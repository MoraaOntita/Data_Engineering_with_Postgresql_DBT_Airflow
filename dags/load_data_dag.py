from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import sys

sys.path.append('/media/moraa/New Volume/Ontita/10Academy/Cohort B/Projects/Week2/Data_Engineering_with_Postgresql_DBT_Airflow')

from scripts.config import (
    csv_file_path,
    prod_postgres_conn,
    dev_postgres_conn,
    staging_postgres_conn
)
from utils import load_data_to_postgres


default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2024, 4, 30),
    'catchup': False
}

@dag(
    dag_id='data_load_dag',
    schedule_interval='*/10 * * * *',  
    default_args=default_args,
    description='Data loading DAG with PythonOperator'
)


def data_load_dag_with_python_operator():
    @task(task_id='load_data_to_prod', dag=dag)
    def load_data_to_prod():
        load_data_to_postgres(csv_file_path, prod_postgres_conn, 'prod_track_table', 'prod_trajectory_table')

    @task(task_id='load_data_to_dev', dag=dag)
    def load_data_to_dev():
        load_data_to_postgres(csv_file_path, dev_postgres_conn, 'dev_track_table', 'dev_trajectory_table')

    @task(task_id='load_data_to_staging', dag=dag)
    def load_data_to_staging():
        load_data_to_postgres(csv_file_path, staging_postgres_conn, 'staging_track_table', 'staging_trajectory_table')

    load_data_to_prod_task = load_data_to_prod()
    load_data_to_dev_task = load_data_to_dev()
    load_data_to_staging_task = load_data_to_staging()

    load_data_to_prod_task >> [load_data_to_dev_task, load_data_to_staging_task]

dag = data_load_dag_with_python_operator()


