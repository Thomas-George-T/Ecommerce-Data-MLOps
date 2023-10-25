import pytest
import os
from src.data_loader import load_data

# Setup constants
PICKLE_PATH = "../data/raw_data.pkl"
CSV_PATH = "../data/data.csv"

def test_load_data_from_pickle():
    """
    Test that load_data correctly loads data from pickle when it exists.
    """
    # Ensure the pickle file exists for this test
    assert os.path.exists(PICKLE_PATH), "Pickle file doesn't exist for testing."
    
    result = load_data(pickle_path=PICKLE_PATH, csv_path=CSV_PATH)
    assert result == PICKLE_PATH, f"Expected {PICKLE_PATH}, but got {result}."

def test_load_data_from_csv():
    """
    Test that load_data correctly loads data from CSV and saves as pickle when pickle doesn't exist.
    """
    # Temporarily rename the pickle to simulate its absence
    if os.path.exists(PICKLE_PATH):
        os.rename(PICKLE_PATH, PICKLE_PATH + ".bak")
    
    result = load_data(pickle_path=PICKLE_PATH, csv_path=CSV_PATH)
    assert result == PICKLE_PATH, f"Expected {PICKLE_PATH}, but got {result}."

    # Rename pickle back to its original name
    if os.path.exists(PICKLE_PATH + ".bak"):
        os.rename(PICKLE_PATH + ".bak", PICKLE_PATH)

def test_load_data_no_files():
    """
    Test that load_data raises an error when neither pickle nor CSV exists.
    """
    # Temporarily rename both files
    if os.path.exists(PICKLE_PATH):
        os.rename(PICKLE_PATH, PICKLE_PATH + ".bak")
    if os.path.exists(CSV_PATH):
        os.rename(CSV_PATH, CSV_PATH + ".bak")
    
    with pytest.raises(FileNotFoundError):
        load_data(pickle_path=PICKLE_PATH, csv_path=CSV_PATH)
    
    # Rename files back to their original names
    if os.path.exists(PICKLE_PATH + ".bak"):
        os.rename(PICKLE_PATH + ".bak", PICKLE_PATH)
    if os.path.exists(CSV_PATH + ".bak"):
        os.rename(CSV_PATH + ".bak", CSV_PATH)