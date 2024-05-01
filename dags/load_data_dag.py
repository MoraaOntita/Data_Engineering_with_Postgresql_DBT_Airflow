from airflow.decorators import dag, task
from datetime import datetime
from scripts.config import (
    csv_file_path,
    prod_postgres_conn,
    dev_postgres_conn,
    staging_postgres_conn
)
from utils import load_data_to_postgres

@dag(
    'data_load_dag',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    start_date=datetime(2024, 4, 30),  # A day before the desired start date
    catchup=False,
    default_args={
        'owner': 'airflow',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    }
)
def data_load_dag():
    @task()
    def load_data_to_prod():
        load_data_to_postgres(csv_file_path, prod_postgres_conn, 'prod_track_table', 'prod_trajectory_table')

    @task()
    def load_data_to_dev():
        load_data_to_postgres(csv_file_path, dev_postgres_conn, 'dev_track_table', 'dev_trajectory_table')

    @task()
    def load_data_to_staging():
        load_data_to_postgres(csv_file_path, staging_postgres_conn, 'staging_track_table', 'staging_trajectory_table')

    load_data_to_prod_task = load_data_to_prod()
    load_data_to_dev_task = load_data_to_dev()
    load_data_to_staging_task = load_data_to_staging()

    load_data_to_prod_task >> [load_data_to_dev_task, load_data_to_staging_task]

dag = data_load_dag()
