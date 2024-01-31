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
    'BI',
    default_args=default_args,
    description='BI DAG',
    schedule_interval=timedelta(days=1),
)

# Define the function to run your Python scripts
def run_python_scripts(script_path):
    # Example: Use subprocess to run the Python script
    subprocess.run(['python3', script_path], check=True)

# Create tasks for each Python script
task_import_csv_to_hdfs = PythonOperator(
    task_id='import_csv_to_hdfs',
    python_callable=run_python_scripts,
    op_args=['/opt/airflow/docker-hadoop/workspace/import_csv_to_hdfs.py'],
    dag=dag,
)

task_import_db_to_hdfs = PythonOperator(
    task_id='import_db_to_hdfs',
    python_callable=run_python_scripts,
    op_args=['/opt/airflow/docker-hadoop/workspace/import_db_to_hdfs.py'],
    dag=dag,
)

task_export_db_hdfs = PythonOperator(
    task_id='export_db_hdfs',
    python_callable=run_python_scripts,
    op_args=['/opt/airflow/docker-hadoop/workspace/export_db_hdfs.py'],
    dag=dag,
)

# Set the task dependencies
task_import_csv_to_hdfs >> task_import_db_to_hdfs >> task_export_db_hdfs
