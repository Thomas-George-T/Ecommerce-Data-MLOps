from google.cloud import storage
from datetime import datetime
import pytz
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
import json
import gcsfs
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize variables
fs = gcsfs.GCSFileSystem()
storage_client = storage.Client()
bucket_name = os.getenv("BUCKET_NAME")
MODEL_DIR = os.environ['AIP_STORAGE_URI']

def load_data(gcs_train_data_path):
    """
    Loads the training data from Google Cloud Storage (GCS).
    
    Parameters:
    gcs_train_data_path (str): GCS path where the training data CSV is stored.
    
    Returns:
    DataFrame: A pandas DataFrame containing the training data.
    """
    with fs.open(gcs_train_data_path) as f:
        df = pd.read_csv(f)

    # Columns are assumed to be in the correct order
    column_names = [
        'Date', 'Time', 'CO(GT)', 'PT08.S1(CO)', 'NMHC(GT)', 'C6H6(GT)',
        'PT08.S2(NMHC)', 'NOx(GT)', 'PT08.S3(NOx)', 'NO2(GT)', 'PT08.S4(NO2)',
        'PT08.S5(O3)', 'T', 'RH', 'AH'
    ]
    df.columns = column_names
    
    return df

def normalize_data(data, stats):
    """
    Normalizes the data using the provided statistics.

    Parameters:
    data (DataFrame): The data to be normalized.
    stats (dict): A dictionary containing the feature means and standard deviations.

    Returns:
    DataFrame: A pandas DataFrame containing the normalized data.
    """
    normalized_data = {}
    for column in data.columns:
        mean = stats["mean"][column]
        std = stats["std"][column]
        
        normalized_data[column] = [(value - mean) / std for value in data[column]]
    
    # Convert normalized_data dictionary back to a DataFrame
    normalized_df = pd.DataFrame(normalized_data, index=data.index)
    return normalized_df

def data_transform(df):
    """
    Transforms the data by setting a datetime index, and splitting it into 
    training and validation sets. It also normalizes the features.
    
    Parameters:
    df (DataFrame): The DataFrame to be transformed.
    
    Returns:
    tuple: A tuple containing normalized training features, test features,
           normalized training labels, and test labels.
    """
    
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('Datetime', inplace=True)
    df.drop(columns=['Date', 'Time'], inplace=True)

    # Splitting the data into training and validation sets (80% training, 20% validation)
    train, test = train_test_split(df, test_size=0.2, shuffle=False)

    # Separating features and target variable
    X_train = train.drop(columns=['CO(GT)'])
    y_train = train['CO(GT)']

    X_test = test.drop(columns=['CO(GT)'])
    y_test = test['CO(GT)']

     # Get the json from GCS
    client = storage.Client()
    bucket_name = os.getenv("BUCKET_NAME")
    blob_path = 'scaler/normalization_stats.json' # Change this to your blob path where the data is stored
    bucket = client.get_bucket("mlops___fall2023")
    blob = bucket.blob(blob_path)

    # Download the json as a string
    data = blob.download_as_string()
    stats = json.loads(data)

    # Normalize the data using the statistics from the training set
    X_train_scaled = normalize_data(X_train, stats)
    y_train_scaled = (y_train - stats["mean"]['CO(GT)']) / stats["std"]['CO(GT)']
    
    return X_train_scaled, X_test, y_train_scaled, y_test


def train_model(X_train, y_train):
    """
    Trains a Random Forest Regressor model on the provided data.
    
    Parameters:
    X_train (DataFrame): The training features.
    y_train (Series): The training labels.
    
    Returns:
    RandomForestRegressor: The trained Random Forest model.
    """
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    return model

def save_and_upload_model(model, local_model_path, gcs_model_path):
    """
    Saves the model locally and uploads it to GCS.
    
    Parameters:
    model (RandomForestRegressor): The trained model to be saved and uploaded.
    local_model_path (str): The local path to save the model.
    gcs_model_path (str): The GCS path to upload the model.
    """
    # Save the model locally
    joblib.dump(model, local_model_path)

    # Upload the model to GCS
    with fs.open(gcs_model_path, 'wb') as f:
        joblib.dump(model, f)

def silhouette_analysis(df, figsize=(25, 25)):

    start_k = 2
    stop_k = 15

    # Set the size of the figure
    plt.figure(figsize=figsize)

    # Create a grid with (stop_k - start_k + 1) rows and 2 columns
    grid = gridspec.GridSpec(stop_k - start_k + 1, 2)

    # Assign the first plot to the first row and both columns
    first_plot = plt.subplot(grid[0, :])

    # First plot: Silhouette scores for different k values
    sns.set_palette(['darkorange'])
    
    silhouette_scores = []

    for k in range(start_k, stop_k + 1):
        km = KMeans(n_clusters=k, init='k-means++', n_init=10, max_iter=100, random_state=0)
        km.fit(df)
        labels = km.predict(df)
        score = silhouette_score(df, labels)
        silhouette_scores.append(score)

    best_k = start_k + silhouette_scores.index(max(silhouette_scores))

    plt.plot(range(start_k, stop_k + 1), silhouette_scores, marker='o')
    plt.xticks(range(start_k, stop_k + 1))
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Silhouette score')
    plt.title('Average Silhouette Score for Different k Values', fontsize=15)

    # Add the optimal k value text to the plot
    optimal_k_text = f'The k value with the highest Silhouette score is: {best_k}'
    plt.text(10, 0.23, optimal_k_text, fontsize=12, verticalalignment='bottom', 
             horizontalalignment='left', bbox=dict(facecolor='#fcc36d', edgecolor='#ff6200', boxstyle='round, pad=0.5'))
             

    # Second plot (subplot): Silhouette plots for each k value
    colors = sns.color_palette("bright")

    for i in range(start_k, stop_k + 1):    
        km = KMeans(n_clusters=i, init='k-means++', n_init=10, max_iter=100, random_state=0)
        row_idx, col_idx = divmod(i - start_k, 2)

        # Assign the plots to the second, third, and fourth rows
        ax = plt.subplot(grid[row_idx + 1, col_idx])

        visualizer = SilhouetteVisualizer(km, colors=colors, ax=ax)
        visualizer.fit(df)

        # Add the Silhouette score text to the plot
        score = silhouette_score(df, km.labels_)
        ax.text(0.97, 0.02, f'Silhouette Score: {score:.2f}', fontsize=12, \
                ha='right', transform=ax.transAxes, color='red')

        ax.set_title(f'Silhouette Plot for {i} Clusters', fontsize=15)

    plt.tight_layout()
    plt.show()

    return best_k


def kmeans_clustring(df_cleaned,df_pca, number_of_clusters=3):

    kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', n_init=10, max_iter=100, random_state=0)
    kmeans.fit(df_pca)

    cluster_frequencies = Counter(kmeans.labels_)


    label_mapping = {label: new_label for new_label, (label, _) in
                 enumerate(cluster_frequencies.most_common())}

    label_mapping = {v: k for k, v in {2: 1, 1: 0, 0: 2}.items()}


    new_labels = np.array([label_mapping[label] for label in kmeans.labels_])


    df_cleaned['cluster'] = new_labels
    df_pca['cluster'] = new_labels              
    
    return df_cleaned, df_pca


def main():
    """
    Main function to orchestrate the loading of data, training of the model,
    and uploading the model to Google Cloud Storage.
    """
    # Load and transform data
    gcs_train_data_path = "gs://mlops___fall2023/data/train/train_data.csv"
    df = load_data(gcs_train_data_path)
    X_train, X_test, y_train, y_test = data_transform(df)

    # Train the model
    model = train_model(X_train, y_train)

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



