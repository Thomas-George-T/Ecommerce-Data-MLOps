"""
A test module for testing scaler module.
"""

import os
import sys
import pytest
import pandas as pd

# Import sys to modify the sys.path dynamically
import sys
from pathlib import Path
from src.scaler import data_scaler

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE_PATH = os.path.join(PROJECT_DIR, "data", "processed","calculate_trends.pkl")
OUTPUT_PATH=os.path.join(PROJECT_DIR, "test", "test_data","test_scaler_output.parquet")
norm_cols = ["Total_Spend",	"Spending_Trend"]
std_cols = ["Monthly_Spending_Mean","Monthly_Spending_Std"]
attribute_error_path=os.path.join(PROJECT_DIR,"\\test\\test_data\\corr.png")

def test_scaler_result():
    """
    Test that raises an error on output files issues.
    """
    # Call the function with the sample data
    result = data_scaler(data_path=INPUT_FILE_PATH,save_path=OUTPUT_PATH,normalize_columns= norm_cols, standardize_columns = std_cols)

    # Add assertions based on expected outcomes
    assert isinstance(result, str)  # Check if the result is a string (the save path)
    assert "scaler_output.parquet" in result  # Check if the save path contains the expected file name

def test_scaler_FileNotFound():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # Call the function with a non-existent file path
        data_scaler(data_path="nonexistent_file.csv", save_path=OUTPUT_PATH,normalize_columns= [], standardize_columns = [])

def test_scaler_standardized():
    # Call the function with the sample data
    with pytest.raises(AssertionError):
        result = data_scaler(data_path=INPUT_FILE_PATH,save_path=OUTPUT_PATH, normalize_columns= [], standardize_columns =norm_cols)

def test_scaler_normalized():
    # Call the function with the sample data
    with pytest.raises(AssertionError):
        result = data_scaler(data_path=INPUT_FILE_PATH, save_path=OUTPUT_PATH,normalize_columns= norm_cols, standardize_columns = [])

def test_scaler_columnslessthan2():
       """
       Test that raises an error for less columns.
       """
       with pytest.raises(KeyError):
       # Call the function with the sample data
            result = data_scaler(data_path=INPUT_FILE_PATH, save_path=OUTPUT_PATH,normalize_columns= ['Days_Since_Last_Purchase', 'Total_Transactions'], standardize_columns = [])
            

