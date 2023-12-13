"""
Inputs: {
    path: file saving location, 
    correlation threshold: threshold for determining high correlations}
Outputs: { 
    Image: png file with masked heatmap
    Matrix: Correlation Matrix as parquet}
Returns: None
"""
import os
from prompt_toolkit.shortcuts import yes_no_dialog
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# Reset background style
sns.set_style('whitegrid')

#Loading Config File
PAR_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#Global variables
__INGESTPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed','scaler_output.parquet')
__IMGPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed',"images",\
    "correlation_heatmap.png")
__PARQUETPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed', "correlation_matrix.parquet")
CORR_THRESH = 0.5
__OUTPUTPATH__=(__IMGPATH__,__PARQUETPATH__)

def correlation_check(in_path=__INGESTPATH__,out_path=__OUTPUTPATH__,\
    correlation_threshold=CORR_THRESH):
    """
    Global variables(can only be changed through Config file)
    Args:
    in_path(str): ingest_path, 
    out_path(str1,str2):
    cvr_threshold[float]: cumulative explained variance threshold for variance
    drop_cols: column to be ommitted for pca
    """
    #Placeholder for data
    data = None

    #Try to Load data from pickle
    try:
        data = pd.read_parquet(in_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found at {in_path}.") from None

    #Check datatype
    if not isinstance(data,pd.DataFrame):
        raise TypeError("File did not load DataFrame correctly.") from None

    print(data)

    #check save paths are strings
    try:
        assert isinstance(out_path[0],str)
    except AssertionError as ae:
        raise TypeError("Image Save Path should be a String!") from ae

    try:
        assert isinstance(out_path[0],str)
    except AssertionError as ae:
        raise TypeError("Parquet Save Path should be a String!") from ae

    #Value check for correlation_thresh
    if not 0<correlation_threshold<1:
        raise ValueError("cvr_thresh should lie between 0 and 1.")

    # Calculate the correlation matrix, **Future deprecation warning**
    corr = data.corr()

    # Create a mask to only show the lower triangle of the matrix (since it's mirrored around its
    # top-left to bottom-right diagonal)
    mask = np.zeros_like(corr)
    mask[np.triu_indices_from(mask, k=1)] = True

    # Define a custom colormap
    colors = ['#ff6200', '#ffcaa8', 'white', '#ffcaa8', '#ff6200']
    my_cmap = LinearSegmentedColormap.from_list('custom_map', colors, N=256)

    # Plot the heatmap
    fig=plt.figure(figsize=(10, 10))
    plt.title(f'Correlation Matrix, Correlation Threshold:{correlation_threshold}', fontsize=14)
    sns.heatmap(corr, mask=mask, cmap=my_cmap, annot=True, center=0, fmt='.2f', linewidths=2)

    #Save heatmap as image for reference
    save_heatmap(fig,out_path[0])

    #saving corr matrix
    save_correlations_as_parquet(corr,out_path[1])

def save_heatmap(fig,path):
    """
    Saves the heatmap as png file.
    Inputs: data: data to be saved, path: file saving location
    Returns: None
    Executes: saving of png, except prompts the user to proceed if file open.
    Error Checks: FileNotFoundError
    """
    try:
        p=os.path.dirname(path)
        if not os.path.exists(p):
            os.makedirs(p)
        fig.savefig(path)
        print(f"File saved successfully at Path: {path}.")
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result:
            fig.savefig(path)
        else:
            print(f"Could not save File at Path: {path}.")

def save_correlations_as_parquet(data,path):
    """
    Saves the correlation matrix as parquet.
    Inputs: data: data to be saved, path: file saving location
    Returns: None
    Executes: raises Attribute error if data is not df as /
    to_parquet would not be executed.
    Error Checks: FileNotFoundError
    """
    try:
        p=os.path.dirname(path)
        if not os.path.exists(p):
            os.makedirs(p)
        data.to_parquet(path)
        print(f"File saved successfully at Path: {path}.")
    except AttributeError as ae:
        raise AttributeError("to_parquet could not be executed as object not DataFrame.") from ae
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result:
            data.to_parquet(path)
        else:
            print(f"Could not save File at Path: {path}.")
