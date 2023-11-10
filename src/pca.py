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
PROJECT_DIR = Path("Ecommerce-Data-MLOps").resolve()
config_path = os.path.join(PROJECT_DIR, 'config', "config_feature_processing.json")
with open(config_path, "r") as json_file:
    pca_config = json.load(json_file)

#Selected columns for PCA are taken from config file for 'feature_processing'
COLS = pca_config.get("pca").get("columns")
#threshold cumulative variance ratio
CVR_THRESHOLD = pca_config.get("pca").get("cvr_threshold")
DEFAULT_PATH = os.path.join(PROJECT_DIR, pca_config.get("pca").get("ingest_file_path"))
OUT_PATH = os.path.join(PROJECT_DIR, pca_config.get("pca").get("out_file_path"))

def PC_Analyzer(columns, data_path=DEFAULT_PATH, save_path=OUT_PATH):
    #Try to Load data from pickle
    if str(data_path).endswith(".pkl"):
        try:
            data = pd.read_pickle(data_path)
        except FileNotFoundError as ffe:
            print(f"Pickle File does not exist at path: {data_path}. \n{ffe}")

    #Try to Load data from parquet
    elif str(data_path).endswith(".parquet"):
        try:
            data = pd.read_parquet(data_path)
        except FileNotFoundError as ffe:
            print(f"Parquet File does not exist at path: {data_path}. \n{ffe}")
    
    if len(columns) < 5:
        raise ValueError("\nColumns are less than 3. Cannot proceed with PCA.")
    
    #Keeping the Specific Columns defined for PCA
    # (Defaults set in Config)
    data = data[columns]

    data.set_index('CustomerID', inplace=True) #hardcoded CustomerID
    
    """
    Implements PCA with 3 Principal Component axes by default.
    If the Cumulative variance ratio is below threshold, dunction implements PCA with 3 Components.
    """    
    #PCA outputting values along 2 axes 
    reducer = np.arange(2,8)
    cvr_= []

    for i in range(len(reducer)):
        if cvr_[-1] > CVR_THRESHOLD:
            n=reducer[i]
            break
        else:
            check = PCA(n_components=reducer[i]).fit(data)
            vr_ = np.cumsum(check.explained_variance_ratio_[reducer[i]])
            cvr_.append(vr_)

    pca = PCA(n_components=n).fit(data)
         
    #Getting post-PCA data
    reduced_data=pca.transform(data)

    #Saving the processed data as a parquet file and returning path
        
    try:
        pq.write_table(reduced_data, save_path)
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result == True:
            pq.write_table(reduced_data, save_path)
        else:
            print(f"Could not save File at Path: {save_path}.")
    else:
        print(f"File saved successfully at Path: {save_path}.")
        return save_path
