"""
A test module for testing scaler module.
"""

import os
import sys
import pytest
import pandas as pd
from pathlib import Path

# Import sys to modify the sys.path dynamically
import sys
from pathlib import Path

# Add the path to another_folder to the sys.path
#sys.path.append(os.path.join(os.path.dirname(Path(__file__).resolve()),"src"))
import sys
sys.path.append('S:\NEU\Courses_Academic\IE7374_MLOps\Ecommerce-Data-MLOps\src')
# from ab import your_function
from src.scaler import data_scaler

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE_PATH = os.path.join(PROJECT_DIR, "data", "processed","calculate_trends.pkl")
OUTPUT_PATH=os.path.join(PROJECT_DIR, "test", "test_data","test_scaler_output.parquet")
norm_cols = ["Total_Spend",	"Spending_Trend"]
std_cols = ["Monthly_Spending_Mean","Monthly_Spending_Std"]

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

def test_scaler_emptydata():
    """
    Test that datafile is empty.
    """
    with pytest.raises(TypeError):
        # Call the function with a type error
        data_scaler(data_path=INPUT_FILE_PATH,save_path=OUTPUT_PATH, normalize_columns= [], standardize_columns = [])

def test_scaler_File_exists():
    """
    Test that cols for pca are empty.
    """
    with pytest.raises(FileExistsError):
        # Call the file exists
        data_scaler(data_path=INPUT_FILE_PATH,save_path=OUTPUT_PATH, normalize_columns= [], standardize_columns = [])

def test_data_standardized():
    # Call the function with the sample data
    result = data_scaler(data_path=INPUT_FILE_PATH,save_path=OUTPUT_PATH, normalize_columns= [], standardize_columns = std_cols)

    # Load the saved file
    saved_data = pd.read_parquet(result)

    # Check if specific columns have correct values
    assert (saved_data[std_cols] in range(-1,1)).all()

def test_data_normalized():
    # Call the function with the sample data
    result = data_scaler(data_path=INPUT_FILE_PATH, save_path=OUTPUT_PATH,normalize_columns= norm_cols, standardize_columns = [])

    # Load the saved file
    saved_data = pd.read_parquet(result)

    # Check if specific columns have correct values
    assert (saved_data[norm_cols] in range(0,1)).all()
