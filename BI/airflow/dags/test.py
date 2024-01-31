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
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'BI_workflow',
    default_args=default_args,
    description='piplien BI',
    schedule_interval=timedelta(days=1),
)

# Define the function to run your Python scripts
def run_python_scripts(script_path):
    # Example: Use subprocess to run the Python script
    subprocess.run(['python3', script_path], check=True)

# Create tasks for each Python script
data_ingestion = PythonOperator(
    task_id='data_ingestion',
    python_callable=run_python_scripts,
    op_args=['/opt/airflow/docker-hadoop/workspace/import_csv_to_hdfs.py'],
    dag=dag,
)

data_retreival = PythonOperator(
    task_id='data_retreival',
    python_callable=run_python_scripts,
    op_args=['/opt/airflow/docker-hadoop/workspace/import_db_to_hdfs.py'],
    dag=dag,
)

data_warehouse = PythonOperator(
    task_id='data_warehouse',
    python_callable=run_python_scripts,
    op_args=['/opt/airflow/docker-hadoop/workspace/export_db_hdfs.py'],
    dag=dag,
)

# Set the task dependencies
data_ingestion >> data_retreival >> data_warehouse
