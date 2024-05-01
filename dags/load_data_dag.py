from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_load_dag',
    default_args=default_args,
    description='A DAG to load data files into PostgreSQL',
    schedule_interval=timedelta(days=1),  # Adjust the schedule interval as needed
)

# Task to load data files into PostgreSQL in Prod environment
load_data_prod = BashOperator(
    task_id='load_data_prod',
    bash_command='python /path/to/load_data_prod.py',  # Command to load data for Prod
    dag=dag,
)

# Task to load data files into PostgreSQL in Dev environment
load_data_dev = BashOperator(
    task_id='load_data_dev',
    bash_command='python /path/to/load_data_dev.py',  # Command to load data for Dev
    dag=dag,
)

# Task to load data files into PostgreSQL in Staging environment
load_data_staging = BashOperator(
    task_id='load_data_staging',
    bash_command='python /path/to/load_data_staging.py',  # Command to load data for Staging
    dag=dag,
)

# Define task dependencies
load_data_dev >> load_data_prod
load_data_staging >> load_data_prod
