import pandas as pd

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


