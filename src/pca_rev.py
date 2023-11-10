"""
Code for dimensionality reduction through PCA on data features.
  
"""
import os
import json
import pandas as pd
from sklearn.decomposition import PCA
import numpy as np
from pathlib import Path
import pyarrow.parquet as pq
from prompt_toolkit.shortcuts import yes_no_dialog

#Defining the Defaults 
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(PROJECT_DIR,"config","config_feature_processing.json")
with open(config_path, "r") as json_file:
    pca_config = json.load(json_file).get("pca")

#Selected columns for PCA are taken from config file for 'feature_processing'
COLS = pca_config.get("columns")
#threshold cumulative variance ratio
CVR_THRESHOLD = pca_config.get("cvr_threshold")
DEFAULT_PATH = os.path.join(PROJECT_DIR, pca_config.get("ingest_file_path"))
OUT_PATH = os.path.join(PROJECT_DIR, pca_config.get("out_file_path"))
INDEXER=pca_config.get("indexed_on")[0]

def PC_Analyzer(columns, data_path=DEFAULT_PATH, save_path=OUT_PATH, cvr=CVR_THRESHOLD, indexed_on=INDEXER):
    # Placeholder
    data = None

    # Try to Load data from pickle
    try:
        data = pd.read_pickle(data_path)
    except FileNotFoundError as ffe:
        print(f"Pickle File does not exist at path: {data_path}. \n{ffe}")
        try:
            # Try to Load data from parquet
            data = pd.read_parquet(data_path)
        except FileNotFoundError as parquet_ffe:
            print(f"Parquet File also does not exist at path: {data_path}. \n{parquet_ffe}")
            raise FileNotFoundError(f"No data file found at path: {data_path}") from None
    
    print(data)

    # Check if the length of columns is greater than 2
    if len(columns) <= 2:
        print("\nColumns are less than 2. Cannot proceed with PCA.")
        raise ValueError

    try:
        assert all(col in data.columns for col in columns)
    except AssertionError:
        print(f"Column not found in Dataset. Recheck Config File.")
        raise KeyError

    # Keeping the Specific Columns defined for PCA
    # (Defaults set in Config)
    data.set_index(indexed_on, inplace=True)  # Hardcoded CustomerID
    pca_df = data[columns]

    """
    Implements PCA with 3 Principal Component axes by default.
    If the Cumulative variance ratio is below threshold, function implements PCA with 3 Components.
    """    
    # PCA outputting values along 2 axes 
    n_components_range = np.arange(2, 9)
    cumulative_var_ratio = [0]

    for n_components in n_components_range:
        if cumulative_var_ratio[-1] > cvr:
            break
        else:
            pca_check = PCA(n_components=n_components).fit(pca_df)
            var_ratio = pca_check.explained_variance_ratio_
            cumulative_var_ratio.append(np.cumsum(var_ratio)[-1])

    print([(n_components_range(i),cumulative_var_ratio(i)) for i in range(len(n_components_range))])    
    
    pca = PCA(n_components=n_components).fit(pca_df)

    # Getting post-PCA data
    reduced_data = pca.transform(pca_df)
    pca_transformed_data = pd.DataFrame(reduced_data, columns=[f'PC{i+1}' for i in range(pca.n_components_)])
    pca_transformed_data.index = data.index

    # Saving the processed data as a parquet file and returning path
    try:
        pca_transformed_data.to_parquet(save_path)
        print(f"File saved successfully at Path: {save_path}.")
        return save_path
    except AttributeError as e:
        print(f"Could not save File at Path: {save_path}. Error: {e}")
        return None
    except FileExistsError:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result == True:
            pca_transformed_data.to_parquet(save_path)
        else:
            print(f"Could not save File at Path: {save_path}.")

# Example usage:
PC_Analyzer(columns=COLS)
