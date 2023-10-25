"""
A module for testing transaction_status_handler module.
"""

import os
import pickle
from src.transaction_status_handler import handle_transaction_status

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_duplicates.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_transaction_status.pkl')

def test_handle_transaction_status():
    """
    Test that handle_transaction_status correctly adds the 'transaction_status' column 
    based on the 'InvoiceNo' and ensures statuses are 'Cancelled' or 'Completed'.
    """
    result = handle_transaction_status(input_pickle_path=INPUT_PICKLE_PATH,
                                       output_pickle_path=OUTPUT_PICKLE_PATH)
    assert result == OUTPUT_PICKLE_PATH, f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."
    # Load the output pickle file and check the 'transaction_status' column
    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)
    # Assert that 'transaction_status' column exists
    assert 'transaction_status' in df.columns,\
        "'transaction_status' column not found in the dataframe."
    # Check if all values in 'transaction_status' are either 'Cancelled' or 'Completed'
    unique_statuses = df['transaction_status'].unique()
    assert set(unique_statuses) == {'Cancelled', 'Completed'},\
        "Unexpected values found in 'transaction_status' column."
