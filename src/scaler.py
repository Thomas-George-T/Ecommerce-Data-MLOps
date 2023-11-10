"""
Code for scaling of column values.
Scaler works on Standardization of data across the columns selected.

[Json file data]
file_path:
columns:  
"""
import os
import json
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import Normalizer
from pathlib import Path
import pyarrow.parquet as pq
from prompt_toolkit.shortcuts import yes_no_dialog

#Defining the Defaults 
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(PROJECT_DIR,"config","config_feature_processing.json")
with open(config_path, "r") as json_file:
    scaler_config = json.load(json_file).get("scaler")

#Selected columns for Standard are taken from config file for 'feature_processing'
N_COLS = scaler_config.get("normalize_columns")
S_COLS = scaler_config.get("standardize_columns")
DEFAULT_PATH = os.path.join(PROJECT_DIR,scaler_config.get("ingest_file_path"))
OUT_PATH = os.path.join(PROJECT_DIR,scaler_config.get("out_file_path"))

def data_scaler(standardize_columns, normalize_columns, data_path=DEFAULT_PATH, save_path=OUT_PATH):
    #Placeholder for data variable
    data = None

    #Try to Load data from pickle
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

    scaled_data=data        

    #Standardizing the data 
    for i in range(len(standardize_columns)):
        std_scaler = StandardScaler()
        scaled_data[[standardize_columns[i]]]=std_scaler.fit_transform(data[[standardize_columns[i]]])

    #Normalizing the data 
    for i in range(len(normalize_columns)):
        norm_scaler = Normalizer()
        scaled_data[[normalize_columns[i]]]=norm_scaler.fit_transform(data[[normalize_columns[i]]])

    print(scaled_data)#checking
    #Check is all columns in Config are in Data
    try:
        assert all(col in data.columns for col in standardize_columns)
    except AssertionError:
        print(f"Column not found in Dataset. Recheck Config File.")
        raise KeyError
    else:
        try:
            assert all(col in data.columns for col in normalize_columns)
        except AssertionError:
            print(f"Column not found in Dataset. Recheck Config File.")
            raise KeyError
    try:
        assert [scaled_data[i].between(-1,1).all() for i in standardize_columns]
    except AssertionError:
        raise AssertionError(f"Standard Scaling incomplete. Values out of bounds:(-1,1)") from None

    try:
        assert [scaled_data[i].between(0,1).all() for i in normalize_columns]
    except AssertionError:
        raise AssertionError(f"Normalize Scaling incomplete. Values out of bounds:(-1,1)") from None

    #Saving the processed data as a parquet file and returning path
    try:
        scaled_data.to_parquet(save_path)
    except AttributeError:
        print(f"Could not save File at Path: {save_path}.")
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result == True:
            scaled_data.to_parquet(save_path)
        else:
            print(f"Could not save File at Path: {save_path}.")
    else:
        print(f"File saved successfully at Path: {save_path}.")
        return save_path
    
data_scaler( N_COLS, S_COLS, DEFAULT_PATH, OUT_PATH,)