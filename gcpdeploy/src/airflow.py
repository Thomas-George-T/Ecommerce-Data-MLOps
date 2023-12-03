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
from src.cleaning_description import cleaning_description
from src.removing_zero_unitprice import removing_zero
from src.rfm import rfm
from src.unique_products import unique_products
from src.customers_behavior import customers_behavior
from src.geographic_features import geographic_features
from src.cancellation_details import cancellation_details
from src.seasonality import seasonality_impacts
from src.outlier_treatment import removing_outlier
from src.scaler import scaler
from src.pca import pc_analyzer
from src.correlation import correlation_check

# Enable pickle support for XCom, allowing data to be passed between tasks
conf.set('core', 'enable_xcom_pickling', 'True')
conf.set('core', 'enable_parquet_xcom', 'True')

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

# Task to handle cleaning description, depends on anomaly codes
cleaning_description_task = PythonOperator(
    task_id='cleaning_description_task',
    python_callable=cleaning_description,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="anomaly_codes_task") }}',
    },
    dag=dag,
)

# Task to handle removing zero unitprices, depends on cleaning description
removing_zero_unitprice_task = PythonOperator(
    task_id='removing_zero_unitprice_task',
    python_callable=removing_zero,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="cleaning_description_task") }}',
    },
    dag=dag,
)

# Task to handle RFM analysis, depends on removing zero unitprices
rfm_task = PythonOperator(
    task_id='rfm_task',
    python_callable=rfm,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="removing_zero_unitprice_task") }}',
    },
    dag=dag,
)

# Task to handle grouping based on unique products, depends on RFM analysis
unique_products_task = PythonOperator(
    task_id='unique_products_task',
    python_callable=unique_products,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="rfm_task") }}',
    },
    dag=dag,
)

# Task to handle behavorial patterns, depends on grouping based on unique products
customers_behavior_task = PythonOperator(
    task_id='customers_behavior_task',
    python_callable=customers_behavior,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="unique_products_task") }}',
    },
    dag=dag,
)

# Task to handle geographic features, depends on behavorial patterns
geographic_features_task = PythonOperator(
    task_id='geographic_features_task',
    python_callable=geographic_features,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="customers_behavior_task") }}',
    },
    dag=dag,
)

# Task to handle cancellation frequency and rate, depends on geographic features
cancellation_details_task = PythonOperator(
    task_id='cancellation_details_task',
    python_callable=cancellation_details,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="geographic_features_task") }}',
    },
    dag=dag,
)

# Task to handle seasonality trends, depends on cancellation frequency and rate
seasonality_task = PythonOperator(
    task_id='seasonality_task',
    python_callable=seasonality_impacts,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="cancellation_details_task") }}',
    },
    dag=dag,
)

# Task to handle outlier treatment, depends on seasonality trends
outlier_treatment_task = PythonOperator(
    task_id='outlier_treatment_task',
    python_callable=removing_outlier,
    op_kwargs={
        'input_picle_path': '{{ ti.xcom_pull(task_ids="seasonality_task") }}',
    },
    dag=dag,
)

# Task to standardize the columns
column_values_scaler_task = PythonOperator(
    task_id='column_values_scaler_task',
    python_callable=scaler,
    op_kwargs={
        'in_path': '{{ ti.xcom_pull(task_ids="outlier_treatment_task") }}',
    },
    dag=dag,
)

# Task for dimensionality reduction
pca_task = PythonOperator(
    task_id='pca_task',
    python_callable=pc_analyzer,
    op_kwargs={
        'in_path': '{{ ti.xcom_pull(task_ids="column_values_scaler_task") }}',
    },
    dag=dag,
)

# Task to check correlation amongst columns at this stage
correlation_check_task = PythonOperator(
    task_id='correlation_check_task',
    python_callable=correlation_check,
    op_kwargs={
        'in_path': '{{ ti.xcom_pull(task_ids="column_values_scaler_task") }}',
    },
    dag=dag,
)

# Set task dependencies
ingest_data_task >> unzip_file_task >> load_data_task >> handle_missing_task \
>> remove_duplicates_task >> transaction_status_task >> anomaly_codes_task >> cleaning_description_task \
>> removing_zero_unitprice_task >> rfm_task >> unique_products_task >> customers_behavior_task \
>> geographic_features_task >> cancellation_details_task >> seasonality_task >> outlier_treatment_task \
>> column_values_scaler_task >> pca_task >> correlation_check_task

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.cli()
