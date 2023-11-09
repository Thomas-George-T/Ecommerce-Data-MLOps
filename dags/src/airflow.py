"""
The Airflow Dag for the preprocessing datapipeline
"""

# Import necessary libraries and modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import configuration as conf
from src.download_data import ingest_data
from src.unzip_data import unzip_file


# Enable pickle support for XCom, allowing data to be passed between tasks
conf.set('core', 'enable_xcom_pickling', 'True')

# Define default arguments for your DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 11, 9),
    'retries': 0, # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5), # Delay before retries
}

# Create a DAG instance named 'datapipeline' with the defined default arguments
dag = DAG(
    'datapipeline',
    default_args=default_args,
    description='Airflow DAG for the datapipeline',
    schedule_interval=None,  # Set the schedule interval or use None for manual triggering
    catchup=False,
)

# Define PythonOperators for each function

# Task to download data from source, calls the 'ingest_data' Python function
ingest_data_task = PythonOperator(
    task_id='ingest_data_task',
    python_callable=ingest_data,
    op_args=["https://archive.ics.uci.edu/static/public/352/online+retail.zip"],
    dag=dag,
)
# Task to unzip the downloaded data, depends on 'ingest_data'
unzip_file_task = PythonOperator(
    task_id='unzip_file_task',
    python_callable=unzip_file,
    op_args=[ingest_data_task.output],
    dag=dag,
)

# Set task dependencies
ingest_data_task >> unzip_file_task

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.cli()
