from flask import Flask, jsonify, request
# from google.cloud import storage
import joblib
import os
import json
from dotenv import load_dotenv
import pandas as pd
# Experimental Start
import time
from datetime import datetime
from google.cloud import storage, logging, bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from google.logging.type import log_severity_pb2 as severity
# Experimental End

load_dotenv()

# Global initialization of the storage client
storage_client = storage.Client()

app = Flask(__name__)

## Experimental start
# Set up Google Cloud logging
service_account_file = 'ecommerce-mlops-406821-40598235283c.json'
credentials = service_account.Credentials.from_service_account_file(service_account_file)
client = logging.Client(credentials=credentials)
logger = client.logger('training_pipeline')
# Initialize BigQuery client
bq_client = bigquery.Client(credentials=credentials)
table_id = os.environ['BIGQUERY_TABLE_ID']


def get_table_schema():
    """Build the table schema for the output table
    
    Returns:
        List: List of `SchemaField` objects"""
    return [

        SchemaField("PC1", "FLOAT", mode="NULLABLE"),
        SchemaField("PC2", "FLOAT", mode="NULLABLE"),
        SchemaField("PC3", "FLOAT", mode="NULLABLE"),
        SchemaField("PC4", "FLOAT", mode="NULLABLE"),
        SchemaField("PC5", "FLOAT", mode="NULLABLE"),
        SchemaField("PC6", "FLOAT", mode="NULLABLE"),
        SchemaField("prediction", "FLOAT", mode="NULLABLE"),
        SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("latency", "FLOAT", mode="NULLABLE"),
    ]


def create_table_if_not_exists(client, table_id, schema):
    """Create a BigQuery table if it doesn't exist
    
    Args:
        client (bigquery.client.Client): A BigQuery Client
        table_id (str): The ID of the table to create
        schema (List): List of `SchemaField` objects
        
    Returns:
        None"""
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
    except NotFound:
        print("Table {} is not found. Creating table...".format(table_id))
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)  # Make an API request.
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

## Experimental End

def initialize_variables():
  """
  Initialize environment variables.
  Returns:
      tuple: The project id and bucket name.
  """
  project_id = os.getenv("PROJECT_ID")
  bucket_name = os.getenv("BUCKET_NAME")
  return project_id, bucket_name

def initialize_client_and_bucket(bucket_name):
  """
  Initialize a storage client and get a bucket object.
  Args:
      bucket_name (str): The name of the bucket.
  Returns:
      tuple: The storage client and bucket object.
  """
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  return storage_client, bucket
  
def load_model(bucket, bucket_name):
  """
  Fetch and load the latest model from the bucket.
  Args:
      bucket (Bucket): The bucket object.
      bucket_name (str): The name of the bucket.
  Returns:
      _BaseEstimator: The loaded model.
  """
  try:
    latest_model_blob_name = fetch_latest_model(bucket_name)
    local_model_file_name = os.path.basename(latest_model_blob_name)
    model_blob = bucket.blob(latest_model_blob_name)

    print("latest_model_blob_name",latest_model_blob_name)

    # Download the model file
    model_blob.download_to_filename(local_model_file_name)

    # Load the model
    model = joblib.load(local_model_file_name)
    return model
  except Exception as e:
    print(f"Error occurred while loading the model: {e}")


def fetch_latest_model(bucket_name, prefix="model/model_"):
  """Fetches the latest model file from the specified GCS bucket.
  Args:
      bucket_name (str): The name of the GCS bucket.
      prefix (str): The prefix of the model files in the bucket.
  Returns:
      str: The name of the latest model file.
  """
  # List all blobs in the bucket with the given prefix
  blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

  # Extract the timestamps from the blob names and identify the blob with the latest timestamp
  blob_names = [blob.name for blob in blobs]
  if not blob_names:
      raise ValueError("No model files found in the GCS bucket.")

  latest_blob_name = sorted(blob_names, key=lambda x: x.split('_')[-1], reverse=True)[1]

  return latest_blob_name
  
@app.route(os.environ['AIP_HEALTH_ROUTE'], methods=['GET'])
def health_check():
  """Health check endpoint that returns the status of the server.
  Returns:
      Response: A Flask response with status 200 and "healthy" as the body.
  """
  return {"status": "healthy"}
  
@app.route(os.environ['AIP_PREDICT_ROUTE'], methods=['POST'])
def predict():
  """
  Endpoint for making predictions with the KMeans model.
  Expects a JSON payload with a 'data' key containing a list of 6 PCA values.
  Returns:
      Response: A Flask response containing JSON-formatted predictions.
  """
  request_json = request.get_json()

  request_instances = request_json['instances']

  ## Experimental start
  logger.log_text("Received prediction request.", severity='INFO')

  prediction_start_time = time.time()
  current_timestamp = datetime.now().isoformat()
  ## Experimental end

  prediction = model.predict(pd.DataFrame(list(request_instances)))
  
  ## Experimental start
  prediction_end_time = time.time()
  prediction_latency = prediction_end_time - prediction_start_time
  ## Experimental end

  prediction = prediction.tolist()

  ## Experimental start
  
  logger.log_text(f"Prediction results: {prediction}", severity='INFO')

  rows_to_insert = [
      {   
          "PC1": instance['PC1'],
          "PC2": instance['PC2'],
          "PC3": instance['PC3'],
          "PC4": instance['PC4'],
          "PC5": instance['PC5'],
          "PC6": instance['PC6'],
          "prediction": pred,
          "timestamp": current_timestamp,
          "latency": prediction_latency
      }
      for instance, pred in zip(request_instances, prediction)
  ]

  errors = bq_client.insert_rows_json(table_id, rows_to_insert)
  if errors == []:
      logger.log_text("New predictions inserted into BigQuery.", severity='INFO')
  else:
      logger.log_text(f"Encountered errors inserting predictions into BigQuery: {errors}", severity='ERROR')


## Experiment end
  # print("prediction",prediction)
  output = {'predictions': [{'cluster': pred} for pred in prediction]}
  return jsonify(output)


project_id, bucket_name = initialize_variables()
storage_client, bucket = initialize_client_and_bucket(bucket_name)

model = load_model(bucket, bucket_name)

## Experiment start
schema = get_table_schema()
create_table_if_not_exists(bq_client, table_id, schema)
## Experiment end


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8080)