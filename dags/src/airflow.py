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
from src.data_loader import load_data
from src.missing_values_handler import handle_missing
from src.duplicates_handler import remove_duplicates
from src.transaction_status_handler import handle_transaction_status
from src.anomaly_code_handler import handle_anomalous_codes


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

# Task to load data, depends on unzip_file_task
load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    op_kwargs={
        'excel_path': '{{ ti.xcom_pull(task_ids="unzip_file_task") }}',
    },
    dag=dag,
)

# Task to handle missing values, depends on load_data_task
handle_missing_task = PythonOperator(
    task_id='missing_values_task',
    python_callable=handle_missing,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="load_data_task") }}',
    },
    dag=dag,
)

# Task to handle duplicates, depends on missing_values_task
remove_duplicates_task = PythonOperator(
    task_id='remove_duplicates_task',
    python_callable=remove_duplicates,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="handle_missing_task") }}',
    },
    dag=dag,
)

# Task to handle transaction status, depends on remove_duplicates_task
transaction_status_task = PythonOperator(
    task_id='transaction_status_task',
    python_callable=handle_transaction_status,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="remove_duplicates_task") }}',
    },
    dag=dag,
)

# Task to handle anomaly codes, depends on transaction_status_task
anomaly_codes_task = PythonOperator(
    task_id='anomaly_codes_task',
    python_callable=handle_anomalous_codes,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="transaction_status_task") }}',
    },
    dag=dag,
)

# Set task dependencies
ingest_data_task >> unzip_file_task >> load_data_task >> handle_missing_task >> remove_duplicates_task >> transaction_status_task >> anomaly_codes_task

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.cli()
