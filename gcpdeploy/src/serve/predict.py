from flask import Flask, jsonify, request
from google.cloud import storage
import joblib
import os
import json
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Global initialization of the storage client
storage_client = storage.Client()

app = Flask(__name__)

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

  latest_blob_name = sorted(blob_names, key=lambda x: x.split('_')[-1], reverse=True)[0]

  return latest_blob_name
  
@app.route(os.environ['AIP_HEALTH_ROUTE'], methods=['GET'])
def health_check():
  """Health check endpoint that returns the status of the server.
  Returns:
      Response: A Flask response with status 200 and "healthy" as the body.
  """
  return {"status": "healthy"}
  
@app.route('/predict', methods=['POST'])
def predict():
  """
  Endpoint for making predictions with the KMeans model.
  Expects a JSON payload with a 'data' key containing a list of 6 PCA values.
  Returns:
      Response: A Flask response containing JSON-formatted predictions.
  """
  request_json = request.get_json()

  # Validate the input data
  if 'data' not in request_json or len(request_json['data']) != 6:
      return jsonify({'error': 'Please provide exactly 6 PCA values'}), 400

  # Convert the input data to a DataFrame
  data = request_json['data']
  column_names = ['PCA1', 'PCA2', 'PCA3', 'PCA4', 'PCA5', 'PCA6']
  input_df = pd.DataFrame([data], columns=column_names)

  # Make predictions with the model
  prediction = model.predict(input_df)
  return jsonify({'prediction': int(prediction[0])})

project_id, bucket_name = initialize_variables()
storage_client, bucket = initialize_client_and_bucket(bucket_name)
stats = load_stats(bucket)
model = load_model(bucket, bucket_name)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=8080)