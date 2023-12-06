FROM python:3.9-slim

# WORKDIR /
WORKDIR /app

ENV AIP_STORAGE_URI=gs://ecommerce_retail_online_mlops/model
ENV BUCKET_NAME=ecommerce_retail_online_mlops

# Copy current directory contents

COPY trainer/train.py /app/
COPY trainer/ClusterBasedRecommender.py /app/

# Install required dependencies
RUN pip install --no-cache-dir pandas google-cloud-storage scikit-learn joblib grpcio gcsfs python-dotenv pyarrow

# Run the training function
ENTRYPOINT ["python", "-m", "train"]
