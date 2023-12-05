"""
Modular script to perform scaling on incoming data.
Script taken in columns to be standardized or normalized.
Input: Path string to load pickle/parquet file.
Output: Path string for parquet file.
"""
import os
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import Normalizer
from prompt_toolkit.shortcuts import yes_no_dialog

#Loading Config File
PAR_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#Global variables
__INGESTPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed', 'after_outlier_treatment.pickle')
__OUTPUTPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed', 'scaler_output.parquet')
S_COLS = ["Days_Since_Last_Purchase", "Total_Transactions",  \
                "Total_Spend", "Average_Transaction_Value","Unique_Products_Purchased", \
                "Cancellation_Rate","Monthly_Spending_Mean","Monthly_Spending_Std","Spending_Trend"],
N_COLS = ["Total_Products_Purchased","Average_Days_Between_Purchases","Cancellation_Frequency"]
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
