"""
Modular script to perform scaling on incoming data.
Script taken in columns to be standardized or normalized.
Input: Path string to load pickle/parquet file.
Output: Path string for parquet file.
"""
import os
import json
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import Normalizer
from prompt_toolkit.shortcuts import yes_no_dialog

#Loading Config File
PAR_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(PAR_DIRECTORY,"config","feature_processing.json")
with open(config_path, "rb") as f:
    config = json.load(f).get("scaler")

#Global variables
__INGESTPATH__ = config.get("ingest_path") #default path in config
__OUTPUTPATH__ = config.get("output_path") #default path in config
__PATH__ = (__INGESTPATH__,__OUTPUTPATH__)
S_COLS = config.get("standardize_columns")#columns wrt to defaults in config
N_COLS = config.get("normalize_columns")#columns wrt to defaults in config
COLS=(S_COLS,N_COLS)

def scaler(paths=__PATH__,columns=COLS):
    """
    Global variables(can only be changed through Config file)
    Args:
    paths(str1,str2): str1:ingest_path, str2:output_path
    cvr_threshold[float]: cumulative explained variance threshold for variance
    drop_cols: column to be ommitted for pca
    """
    #Placeholder for data
    data = None

    #File Loading
    try:
        if str(paths[0]).endswith(".pkl"):
            data = pd.read_pickle(paths[0])
        else:
            data = pd.read_parquet(paths[0])
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found at {paths[0]}.") from None

    try:
        assert isinstance(paths[1],str)
    except AssertionError as ae:
        raise TypeError("Save Path should be a String!") from ae

    #Check datatype
    if not isinstance(data,pd.DataFrame):
        raise TypeError("File did not load DataFrame correctly.") from None

    #Check all required columns exist in DF
    try:
        assert all(col in data.columns for col in columns[0])
    except AssertionError as exc:
        raise KeyError("Column listed in to be Standardized \
        Columns not found in Dataframe.") from exc

    try:
        assert all(col in data.columns for col in columns[1])
    except AssertionError as exc2:
        raise KeyError("Column listed in to be Normalized \
            Columns not found in Dataframe.") from exc2

    print(data)

    #Standardization (-1,1)
    std_scaler = StandardScaler()
    data_std = std_scaler.fit_transform(data[columns[0]])
    df_std = pd.DataFrame(data_std,columns=[columns[0]])

    #Normalization (0,1)
    norm_scaler = Normalizer()
    data_norm = norm_scaler.fit_transform(data[columns[1]])
    df_norm = pd.DataFrame(data_norm,columns=[columns[1]])

    data[columns[0]] = df_std[columns[0]]
    data[columns[1]]=df_norm[columns[1]]

    #saving data as parquet
    try:
        p=os.path.dirname(paths[1])
        if not os.path.exists(p):
            raise AssertionError(f"Path: {paths[1]} does not exist.")
        data.to_parquet(paths[1])
        print(f"File saved successfully at Path: {paths[1]}.")
    except FileExistsError:
        result = yes_no_dialog(
            title='File Exists Error',
            text="Existing file in use. Please close to overwrite the file. Error:.").run()
        if result:
            data.to_parquet(paths[1])
            print(paths[1])
        else:
            print(f"Could not save File at Path: {paths[1]}.")
    return paths[1]

scaler()
