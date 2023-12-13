from google.cloud import storage
from datetime import datetime
import pytz
import pandas as pd
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor
from sklearn.cluster import KMeans
import joblib
import json
import gcsfs
import os
# import pickle
from dotenv import load_dotenv
# import plotly.graph_objects as go
# import seaborn as sns
# import matplotlib.pyplot as plt
import numpy as np
# from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
# from tabulate import tabulate
from collections import Counter

from ClusterBasedRecommender import generate_recommendations

# Load environment variables
load_dotenv()

# Initialize variables
fs = gcsfs.GCSFileSystem()
storage_client = storage.Client()
bucket_name = os.getenv("BUCKET_NAME")
MODEL_DIR = os.environ['AIP_STORAGE_URI']

def kmeans_clustering(df_cleaned, df_pca, number_of_clusters=3):
    """
    Clustering our data with kmeans
    """

    kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', n_init=10, max_iter=100, random_state=0)
    kmeans.fit(df_pca)

    cluster_frequencies = Counter(kmeans.labels_)


    label_mapping = {label: new_label for new_label, (label, _) in
                 enumerate(cluster_frequencies.most_common())}

    label_mapping = {v: k for k, v in {2: 1, 1: 0, 0: 2}.items()}


    new_labels = np.array([label_mapping[label] for label in kmeans.labels_])


    df_cleaned['cluster'] = new_labels
    df_pca['cluster'] = new_labels              
    
    return df_cleaned, kmeans, df_pca

def save_and_upload_model(model, local_model_path, gcs_model_path):
    """
    Saves the model locally and uploads it to GCS.
    
    Parameters:
    model (kmeans): The trained model to be saved and uploaded.
    local_model_path (str): The local path to save the model.
    gcs_model_path (str): The GCS path to upload the model.
    """
    # Save the model locally
    joblib.dump(model, local_model_path)

    # Upload the model to GCS
    with fs.open(gcs_model_path, 'wb') as f:
        joblib.dump(model, f)


def main():
    """
    Main function to orchestrate the loading of data, training of the model,
    and uploading the model to Google Cloud Storage.
    """
    # Load and transform data
    # gcs_train_data_path = "gs://mlops___fall2023/data/train/train_data.csv"
    # df = load_data(gcs_train_data_path)
    # X_train, X_test, y_train, y_test = data_transform(df)

    # data_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = "gs://ecommerce_retail_online_mlops/data"

    pca_output = pd.read_parquet(data_dir_path + "/pca_output.parquet")
    after_outlier = pd.read_pickle(data_dir_path + "/after_outlier_treatment.pkl")
    outliers_data = pd.read_parquet(data_dir_path + "/df_outlier.parquet")
    df_transactions = pd.read_parquet(data_dir_path + "/transaction_dataframe.parquet")
    
    # Training the model
    customer_data_cleaned, model, customer_data_pca = kmeans_clustering(after_outlier, pca_output)
    print("clustering ran successfully!")
    recommendations_df = generate_recommendations(df_transactions, outliers_data, customer_data_cleaned)
    print("recommendations_df generated successfully!")
    print(recommendations_df.shape)

    # Save the model locally and upload to GCS
    edt = pytz.timezone('US/Eastern')
    current_time_edt = datetime.now(edt)
    version = current_time_edt.strftime('%Y%m%d_%H%M%S')
    local_model_path = "model.pkl"
    gcs_model_path = f"{MODEL_DIR}/model_{version}.pkl"
    print(gcs_model_path)
    save_and_upload_model(model, local_model_path, gcs_model_path)

if __name__ == "__main__":
    main()



