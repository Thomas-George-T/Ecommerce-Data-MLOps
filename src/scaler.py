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
__INGESTPATH__ = os.path.join(PAR_DIRECTORY,config.get("ingest_path")) #default path in config
__OUTPUTPATH__ = os.path.join(PAR_DIRECTORY,config.get("output_path")) #default path in config
S_COLS = config.get("standardize_columns")#columns wrt to defaults in config
N_COLS = config.get("normalize_columns")#columns wrt to defaults in config
COLS=(S_COLS,N_COLS)

def scaler(in_path=__INGESTPATH__,out_path=__OUTPUTPATH__, cols=COLS):
    """
    Global variables(can only be changed through Config file)
    cvr_threshold[float]: cumulative explained variance threshold for variance
    cols(standardize_columns, normalize_columns): tuple of columns to be scaled. 
    """
    #Placeholder for data
    data = None

    #File Loading
    try:
        data = pd.read_pickle(in_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found at {in_path}.") from None

    try:
        assert isinstance(out_path,str)
    except AssertionError as ae:
        raise TypeError("Save Path should be a String!") from ae

    #Check datatype
    if not isinstance(data,pd.DataFrame):
        raise TypeError("File did not load DataFrame correctly.") from None

    #Check all required columns exist in DF
    try:
        assert all(col in data.columns for col in cols[0])
    except AssertionError as exc:
        raise KeyError("Column listed in to be Standardized \
        Columns not found in Dataframe.") from exc

    try:
        assert all(col in data.columns for col in cols[1])
    except AssertionError as exc2:
        raise KeyError("Column listed in to be Normalized \
            Columns not found in Dataframe.") from exc2

    print(data)

    #Standardization (-1,1)
    std_scaler = StandardScaler()
    data_std = std_scaler.fit_transform(data[cols[0]])
    df_std = pd.DataFrame(data_std,columns=[cols[0]])

    #Normalization (0,1)
    norm_scaler = Normalizer()
    data_norm = norm_scaler.fit_transform(data[cols[1]])
    df_norm = pd.DataFrame(data_norm,columns=[cols[1]])

    data[COLS[0]] = df_std[cols[0]]
    data[COLS[1]]=df_norm[cols[1]]

    #saving data as parquet
    try:
        p=os.path.dirname(out_path)
        if not os.path.exists(p):
            os.makedirs(p)
        data.to_parquet(out_path)
        print(f"File saved successfully at Path: {out_path}.")
    except FileExistsError:
        result = yes_no_dialog(
            title='File Exists Error',
            text="Existing file in use. Please close to overwrite the file. Error:.").run()
        if result:
            data.to_parquet(out_path)
            print(f"File saved successfully at Path: {out_path}.")
        else:
            print(f"Could not save File at Path: {out_path}.")
    return out_path
