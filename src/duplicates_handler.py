import pandas as pd
import pickle
import os

def remove_duplicates(input_pickle_path="../data/after_missing_values.pkl", output_pickle_path="../data/after_duplicates.pkl"):
    """
    Load the DataFrame from the input pickle, drop duplicates based on certain columns.
    Save the DataFrame back to a pickle and return its path.
    
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
    
    # Columns to check for duplicates
    columns_to_check = ['InvoiceNo', 'StockCode', 'Description', 'CustomerID', 'Quantity']
    
    # Drop duplicates
    df = df.drop_duplicates(subset=columns_to_check)
    
    # Save the data to output pickle
    with open(output_pickle_path, "wb") as file:
        pickle.dump(df, file)
    
    print(f"Data saved to {output_pickle_path}.")
    return output_pickle_path
