"""
Tests for data_loader module.
"""

import os
import pytest
from src.data_loader import load_data

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Use the project directory to construct paths to other directories
PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed', 'raw_data.pkl')
EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'Online Retail.xlsx')

@pytest.mark.skip(reason="Skipping test_data_loader for now")
def test_load_data_from_excel():
    """
    Test that load_data correctly loads data from Excel and saves as pickle
    when pickle doesn't exist.
    """
    # Temporarily rename the pickle to simulate its absence
    if os.path.exists(PICKLE_PATH):
        os.rename(PICKLE_PATH, PICKLE_PATH + ".bak")
    result = load_data(pickle_path=PICKLE_PATH, excel_path=EXCEL_PATH)
    assert result == PICKLE_PATH, f"Expected {PICKLE_PATH}, but got {result}."
    # Rename pickle back to its original name
    if os.path.exists(PICKLE_PATH + ".bak"):
        os.rename(PICKLE_PATH + ".bak", PICKLE_PATH)
@pytest.mark.skip(reason="Skipping test_data_loader for now")
def test_load_data_no_files():
    """
    Test that load_data raises an error when neither pickle nor Excel exists.
    """
    # Temporarily rename both files
    if os.path.exists(PICKLE_PATH):
        os.rename(PICKLE_PATH, PICKLE_PATH + ".bak")
    if os.path.exists(EXCEL_PATH):
        os.rename(EXCEL_PATH, EXCEL_PATH + ".bak")
    with pytest.raises(FileNotFoundError):
        load_data(pickle_path=PICKLE_PATH, excel_path=EXCEL_PATH)
    # Rename files back to their original names
    if os.path.exists(PICKLE_PATH + ".bak"):
        os.rename(PICKLE_PATH + ".bak", PICKLE_PATH)
    if os.path.exists(EXCEL_PATH + ".bak"):
        os.rename(EXCEL_PATH + ".bak", EXCEL_PATH)
