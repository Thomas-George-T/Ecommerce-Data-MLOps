"""
A test module for testing missing_values_handler module.
"""

import os
import pytest
from src.missing_values_handler import handle_missing

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed', 'raw_data.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed','after_missing_values.pkl')

def test_handle_missing_success():
    """
    Test successful removal of rows with missing values and saving of the dataframe.
    """
    result = handle_missing(input_pickle_path=INPUT_PICKLE_PATH,
                            output_pickle_path=OUTPUT_PICKLE_PATH)
    assert result == OUTPUT_PICKLE_PATH, f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."
def test_handle_missing_file_not_found():
    """
    Test that handle_missing raises an error when the input pickle doesn't exist.
    """
    # Rename the input pickle temporarily to simulate its absence
    if os.path.exists(INPUT_PICKLE_PATH):
        os.rename(INPUT_PICKLE_PATH, INPUT_PICKLE_PATH + ".bak")
    with pytest.raises(FileNotFoundError):
        handle_missing(input_pickle_path=INPUT_PICKLE_PATH,
                       output_pickle_path=OUTPUT_PICKLE_PATH)
    # Rename the input pickle back to its original name
    if os.path.exists(INPUT_PICKLE_PATH + ".bak"):
        os.rename(INPUT_PICKLE_PATH + ".bak", INPUT_PICKLE_PATH)
