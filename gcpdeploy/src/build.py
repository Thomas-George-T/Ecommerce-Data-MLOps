from google.cloud import aiplatform
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration parameters
REGION = os.getenv("REGION")
PROJECT_ID = os.getenv("PROJECT_ID")
BASE_OUTPUT_DIR =  os.getenv("BASE_OUTPUT_DIR")
BUCKET = os.getenv("AIP_MODEL_DIR")  # Should be same as AIP_STORAGE_URI specified in the docker file
CONTAINER_URI = os.getenv("CONTAINER_URI")
MODEL_SERVING_CONTAINER_IMAGE_URI = os.getenv("MODEL_SERVING_CONTAINER_IMAGE_URI")
DISPLAY_NAME = 'customer-segmentation:training'
SERVICE_ACCOUNT_EMAIL = os.getenv("SERVICE_ACCOUNT_EMAIL")

def initialize_aiplatform(project_id, region, bucket):
    """Initializes the AI platform with the given parameters.
    :param project_id: GCP project ID
    :param region: GCP region
    :param bucket: GCS bucket
    """

    aiplatform.init(project=project_id, location=region, staging_bucket=bucket)

def create_training_job(display_name, container_uri, model_serving_container_image_uri, bucket):
    """Creates a custom container training job.
    :param display_name: Display name of the training job
    :param container_uri: URI of the training container
    :param model_serving_container_image_uri: URI of the model serving container
    :param bucket: GCS bucket
    
    :return: Custom container training job
    """
    job = aiplatform.CustomContainerTrainingJob(
        display_name=display_name,
        container_uri=container_uri,
        model_serving_container_image_uri=model_serving_container_image_uri,
        staging_bucket=bucket,
    )
    return job

def run_training_job(job, display_name, base_output_dir, service_account_email):
    """Runs the custom container training job.
    :param job: Custom container training job
    :param display_name: Display name of the training job
    :param base_output_dir: Base output directory
    :param service_account_email: Service account email

    :return: Trained model
    """
    model = job.run(
        model_display_name=display_name,
        base_output_dir=base_output_dir,
        service_account=service_account_email
    )
    return model

def deploy_model_to_endpoint(model, display_name, service_account_email):
    """Deploys the trained model to an endpoint.
    :param model: Trained model
    :param display_name: Display name of the endpoint

    :return: Endpoint
    """
    endpoint = model.deploy(
        deployed_model_display_name=display_name,
        sync=True,
        service_account=service_account_email
    )
    return endpoint

def main():
    # Initialize AI platform
    initialize_aiplatform(PROJECT_ID, REGION, BUCKET)

    # Create and run the training job
    training_job = create_training_job(DISPLAY_NAME, CONTAINER_URI, MODEL_SERVING_CONTAINER_IMAGE_URI, BUCKET)
    model = run_training_job(training_job, DISPLAY_NAME, BASE_OUTPUT_DIR, SERVICE_ACCOUNT_EMAIL)

    # Deploy the model to the endpoint
    endpoint = deploy_model_to_endpoint(model, DISPLAY_NAME,SERVICE_ACCOUNT_EMAIL)
    return endpoint

if __name__ == '__main__':
    endpoint = main()
