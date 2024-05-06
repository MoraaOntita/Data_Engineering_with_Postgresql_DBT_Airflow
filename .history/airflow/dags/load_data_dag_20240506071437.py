from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils import load_data_to_postgres 
from config import csv_file_path, postgres_conn 

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2024, 4, 30),
    'catchup': False
}

with DAG(
    'data_load_dag',
    schedule_interval='*/10 * * * *',
    default_args=default_args,
    description='Data loading DAG with PythonOperator'
) as dag:
    create_log_table = PostgresOperator(
        task_id="create_log_table",
        postgres_conn_id=postgres_conn,
        sql="sql/log_table.sql"
    )

    log_query = PostgresOperator(
        task_id="log_query",
        postgres_conn_id=postgres_conn,
        sql="sql/log_format.sql"
    )

    def load_data_to_tables():
        track_table = 'track_table'
        trajectory_table = 'trajectory_table'
        load_data_to_postgres(csv_file_path, postgres_conn, track_table, trajectory_table)

    load_data_to_tables_task = PythonOperator(
        task_id='load_data_to_tables',
        python_callable=load_data_to_tables
    )

    email_username = os.getenv("EMAIL_USERNAME")

    email_on_failure = EmailOperator(
        task_id='email_on_failure',
        to=email_username,
        subject='{{ ti.task_id }} Failed',
        html_content='Hi, <br> The task {{ ti.task_id }} in DAG {{ dag.dag_id }} failed.',
        trigger_rule='all_failed',
        dag=dag
    )

    create_log_table >> log_query >> load_data_to_tables_task >> email_on_failure

