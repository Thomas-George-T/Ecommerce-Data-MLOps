import pickle
import pandas as pd

# ======================== Missing Values ========================
import pandas as pd
import pickle
import os

def handle_missing(input_pickle_path="../data/raw_data.pkl", output_pickle_path="../data/after_missing_values.pkl"):
    """
    Load the DataFrame from the input pickle, remove rows with missing values in 'CustomerID' and 'Description' columns.
    Then, check if there are any missing values left in the dataframe.
    If there are, raise a ValueError. Finally, save the DataFrame back to a pickle and return its path.
    
    :param input_pickle_path: Path to the input pickle file.
    :param output_pickle_path: Path to the output pickle file.
    :return: Path to the saved pickle file.
    """
    
    # Load DataFrame from input pickle
    if os.path.exists(input_pickle_path):
        with open(input_pickle_path, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {input_pickle_path}")
    
    # Remove rows with missing values in 'CustomerID' and 'Description'
    df = df.dropna(subset=['CustomerID', 'Description'])
    
    # Check if there are any missing values left
    if df.isna().sum().sum() != 0:
        missing_count = df.isna().sum().sum()
        message = f"There are {missing_count} missing values left in the dataframe."
        print(message)
        raise ValueError(message)
    
    # Save the data to output pickle
    with open(output_pickle_path, "wb") as file:
        pickle.dump(df, file)
    
    print(f"Data saved to {output_pickle_path}.")
    return output_pickle_path

