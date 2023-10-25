"""
A test for duplicates_handler module.
"""

import os
import pickle
import pytest
from src.duplicates_handler import remove_duplicates

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_missing_values.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_duplicates.pkl')

def test_remove_duplicates_no_input_file():
    """
    Test that remove_duplicates raises an error when the input pickle doesn't exist.
    """
    # Temporarily rename the input file
    if os.path.exists(INPUT_PICKLE_PATH):
        os.rename(INPUT_PICKLE_PATH, INPUT_PICKLE_PATH + ".bak")
    with pytest.raises(FileNotFoundError):
        remove_duplicates(input_pickle_path=INPUT_PICKLE_PATH,
                          output_pickle_path=OUTPUT_PICKLE_PATH)
    # Rename input file back to its original name
    if os.path.exists(INPUT_PICKLE_PATH + ".bak"):
        os.rename(INPUT_PICKLE_PATH + ".bak", INPUT_PICKLE_PATH)

def test_remove_duplicates():
    """
    Test that remove_duplicates correctly removes duplicates and
    saves to the output pickle.
    """
    result = remove_duplicates(input_pickle_path=INPUT_PICKLE_PATH,
                               output_pickle_path=OUTPUT_PICKLE_PATH)
    assert result == OUTPUT_PICKLE_PATH, f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."
    # Check if duplicates are truly removed
    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)
    columns_to_check = ['InvoiceNo', 'StockCode', 'Description', 'CustomerID', 'Quantity']
    assert not df.duplicated(subset=columns_to_check).any(),\
        "There are still duplicates in the dataframe."
