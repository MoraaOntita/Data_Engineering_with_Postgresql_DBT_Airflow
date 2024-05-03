from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from config import csv_file_path, postgres_conn
from utils import load_data_to_postgres

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2024, 4, 30),
    'catchup': False
}

def load_data_to_tables():
    track_table = 'track_table'
    trajectory_table = 'trajectory_table'
    load_data_to_postgres(csv_file_path, postgres_conn, track_table, trajectory_table)

def send_email_on_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    subject = f"DAG {dag_id} - Task {task_id} Failed"
    body = f"The task {task_id} in DAG {dag_id} has failed."
    return subject, body

with DAG(
    'data_load_dag',
    schedule_interval='*/10 * * * *',  
    default_args=default_args,
    description='Data loading DAG with PythonOperator'
) as dag:
    load_data_to_tables_task = PythonOperator(
        task_id='load_data_to_tables',
        python_callable=load_data_to_tables
    )

    email_on_failure = EmailOperator(
        task_id='email_on_failure',
        to='your@email.com',  # Add your email here
        subject='{{ ti.task_id }} Failed',
        html_content='Hi, <br> The task {{ ti.task_id }} in DAG {{ dag.dag_id }} failed.',
        trigger_rule='all_failed',
    )

    load_data_to_tables_task >> email_on_failure
