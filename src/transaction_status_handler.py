"""
A module for adding a new column named transaction_status based on the 
starting character of InvoiceNo column. 
transaction_status values are Cancelled or Completed.
"""

import pickle
import os
import numpy as np

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_duplicates.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_transaction_status.pkl')

def handle_transaction_status(input_pickle_path=INPUT_PICKLE_PATH,
                              output_pickle_path=OUTPUT_PICKLE_PATH):
    """
    Load the DataFrame from the input pickle, add a 'transaction_status' column 
    to indicate whether the transaction was 'Cancelled' or 'Completed'. 
    Save the DataFrame back to a pickle and return its path.
    
    :param input_pickle_path: Path to the input pickle file.
    :param output_pickle_path: Path to the output pickle file.
    :return: Path to the saved pickle file.
    
    :raises KeyError: If the 'InvoiceNo' column doesn't exist in the dataframe.
    """
    # Load DataFrame from input pickle
    if os.path.exists(input_pickle_path):
        with open(input_pickle_path, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {input_pickle_path}")
    # Check if 'InvoiceNo' column exists
    if 'InvoiceNo' not in df.columns:
        raise KeyError("The input dataframe does not contain an 'InvoiceNo' column.")
    # Add the 'Transaction_Status' column
    df['transaction_status'] = np.where(df['InvoiceNo'].astype(str).str.startswith('C'),
                                        'Cancelled', 'Completed')
    # Save the data to output pickle
    with open(output_pickle_path, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {output_pickle_path}.")
    return output_pickle_path
