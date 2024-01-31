from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Instantiate a DAG
dag = DAG(
    'creation_des_sources',
    default_args=default_args,
    description='creation des sources',
    schedule_interval=timedelta(days=1),
)

# Define the function to run your Python scripts
def run_python_scripts(script_path):
    # Example: Use subprocess to run the Python script
    subprocess.run(['python3', script_path], check=True)

# Create tasks for each Python script
creation_des_sources = PythonOperator(
    task_id='creation_of_source',
    python_callable=run_python_scripts,
    op_args=['/opt/airflow/creation_of_source.py'],
    dag=dag,
)



# Set the task dependencies
creation_des_sources