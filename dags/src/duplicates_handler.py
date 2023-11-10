"""
A module for removing duplicates in dataset based on subset of 
following columns:
- InvoiceNo
- StockCode
- Description
- CustomerID
- Quantity
"""

import pickle
import os

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_missing_values.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_duplicates.pkl')

def remove_duplicates(input_pickle_path=INPUT_PICKLE_PATH, output_pickle_path=OUTPUT_PICKLE_PATH):
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
