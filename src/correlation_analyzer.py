"""
Checks correlation amonst columns to identify 
"""
import os
import json
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path
from prompt_toolkit.shortcuts import yes_no_dialog

# Reset background style
sns.set_style('whitegrid')

#Defining the Defaults 
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(PROJECT_DIR,"config","config_feature_processing.json")
with open(config_path, "r") as json_file:
    corr_config = json.load(json_file).get("correlation_analyzer")

#Selected columns for Standard are taken from config file for 'feature_processing'
f = corr_config.get("ingest_file_path")
drop_cols = corr_config.get("drop_columns")
corr_threshold = corr_config.get("corr_threshold")
DEFAULT_PATH =  os.path.join(PROJECT_DIR, f)
IMG_PATH= corr_config.get("img_file_path")
LIST_PATH= corr_config.get("txt_file_path")
CORR_PATH=corr_config.get("corr_data_path")

def correlation_check(columns=[],data_path=DEFAULT_PATH, img_save_path=IMG_PATH, txt_save_path=LIST_PATH, corr_data_path=CORR_PATH,correlation_threshold=corr_threshold):
    #Placeholder for data variable
    data= None
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
    
    try:
        assert type(img_save_path) == str
    except AssertionError:
        a= type(img_save_path)
        raise TypeError(f"Path cannot be a {a} object.") from None
    try:
        assert type(txt_save_path) == str
    except AssertionError:
        a= type(img_save_path)
        raise TypeError(f"Path cannot be a {a} object.") from None
    try:
        assert type(corr_data_path) == str
    except AssertionError:
        a= type(img_save_path)
        raise TypeError(f"Path cannot be a {a} object.") from None
    
    # Calculate the correlation matrix excluding the 'CustomerID' column
    corr_data = data.drop("CustomerID", axis=1)
    corr = corr_data.corr()

    # Define a custom colormap
    colors = ['#ff6200', '#ffcaa8', 'white', '#ffcaa8', '#ff6200']
    my_cmap = LinearSegmentedColormap.from_list('custom_map', colors, N=256)

    # Create a mask to only show the lower triangle of the matrix (since it's mirrored around its 
    # top-left to bottom-right diagonal)
    mask = np.zeros_like(corr)
    mask[np.triu_indices_from(mask, k=1)] = True

    # Plot the heatmap
    fig=plt.figure(figsize=(10, 10))
    plt.title('Correlation Matrix', fontsize=14)
    hmap = sns.heatmap(corr, mask=mask, cmap=my_cmap, annot=True, center=0, fmt='.2f', linewidths=2)

    #Save heatmap as image for reference
    try:
        fig.savefig(img_save_path)
                
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result == True:
            fig.savefig(img_save_path)
        else:
            print(f"Could not save File at Path: {img_save_path}.")

    else:
        print(f"File saved successfully at Path: {img_save_path}.")
        
    # Find pairs with correlation value above threshold
    high_corr = []
    for i in range(len(corr.columns)): 
        query = corr.loc[corr[corr.columns[i]] > correlation_threshold]
        paired_column = query.index
        [high_corr.append((corr.columns[i], query[corr.columns[i]])) for col in paired_column]

    print(high_corr)

    #saving high correlation columns list as text file
    try:
        with open(txt_save_path, 'w') as fp:
            [fp.write(f"{item}\n") for item in high_corr]
        print(f"File saved successfully at {txt_save_path}.")
    except FileNotFoundError:
        with open(txt_save_path, 'w') as fp:
            [fp.write(f"{item}\n") for item in high_corr]
        print(f"File created successfully at {txt_save_path}.")

    #saving corr matrix
    try:
        corr.to_parquet(corr_data_path)
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result == True:
            corr.to_parquet(corr_data_path)
        else:
            print(f"Could not save File at Path: {img_save_path}.")

    else:
        print(f"File saved successfully at Path: {img_save_path}.")

    return [img_save_path, txt_save_path, corr_data_path]