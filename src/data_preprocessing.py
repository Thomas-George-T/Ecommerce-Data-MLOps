import pandas as pd
import numpy as np

# ======================== Missing Values ========================
def remove_and_check_missing(df):
    """
    Remove rows with missing values in 'CustomerID' and 'Description' columns.
    Then, check if there are any missing values left in the dataframe.
    If there are, raise a MissingValueError.
    """
    
    # Remove rows with missing values in 'CustomerID' and 'Description'
    df = df.dropna(subset=['CustomerID', 'Description'])
    
    # Check if there are any missing values left
    if df.isna().sum().sum() != 0:
        missing_count = df.isna().sum().sum()
        message = f"There are {missing_count} missing values left in the dataframe."
        print(message)
        raise ValueError(message)
    
    return df

# ======================== Drop Duplicates ========================
def drop_duplicates(df):
    """
    Drop duplicates from the dataframe based on the columns: 
    'InvoiceNo', 'StockCode', 'Description', 'CustomerID', 'Quantity'.
    
    Parameters:
    - df: Input dataframe.
    
    Return:
    - Dataframe with duplicates removed.
    """
    
    columns_to_check = ['InvoiceNo', 'StockCode', 'Description', 'CustomerID', 'Quantity']
    df = df.drop_duplicates(subset=columns_to_check)
    
    return df

# ======================== Adding Transaction Status column ========================
def add_transaction_status(df):
    """
    Add a new column 'transaction_status' to the dataframe. 
    The column indicates whether the transaction was 'Cancelled' or 'Completed' 
    based on the 'InvoiceNo' column.
    
    Parameters:
    - Input dataframe.
    
    Return:
    - Dataframe with the new 'Transaction_Status' column added.
    
    :raises KeyError: If the 'InvoiceNo' column doesn't exist in the dataframe.
    """
    
    # Check if 'InvoiceNo' column exists
    if 'InvoiceNo' not in df.columns:
        raise KeyError("The input dataframe does not contain an 'InvoiceNo' column.")
    
    # Add the 'Transaction_Status' column
    df['transaction_status'] = np.where(df['InvoiceNo'].astype(str).str.startswith('C'), 
                                        'Cancelled', 'Completed')
    
    return df

# ======================== Remove Anomaly codes ========================
def remove_anomalous_stock_codes(df):
    """
    Removes rows with stock codes that have 0 or 1 numeric characters.

    :param df: Input dataframe.
    :return: Dataframe with anomalous stock codes removed.
    """
    
    # Finding the stock codes with 0 and 1 numeric characters
    unique_stock_codes = df['StockCode'].unique()
    anomalous_stock_codes = [code for code in unique_stock_codes if sum(c.isdigit() for c in str(code)) in (0, 1)]
    
    # Removing rows with these anomalous stock codes
    df = df[~df['StockCode'].isin(anomalous_stock_codes)]
    
    return df
