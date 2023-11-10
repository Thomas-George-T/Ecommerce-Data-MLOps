"""
A test module for testing pca module.
"""

import os
import pytest
from pathlib import Path
from src.pca import PC_Analyzer

# Determine the absolute path of the project directory
PROJECT_DIR = Path("Ecommerce-Data-MLOps").resolve()
INPUT_FILE_PATH = os.path.join(PROJECT_DIR, "data", "processed","calculate_trends.pkl")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "test", "test_data","test_pca_output.parquet")
cols = []

def test_pca_result():
    """
    Test that raises an error on output files issues.
    """
    # Call the function with the sample data
    result = PC_Analyzer(data_path=INPUT_FILE_PATH, save_path= OUTPUT_PATH, columns=cols)

    # Add assertions based on expected outcomes
    assert isinstance(result, str)  # Check if the result is a string (the save path)
    assert "test_pca_output.parquet" in result  # Check if the save path contains the expected file name


def test_pca_FileNotFound():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # Call the function with a non-existent file path
        PC_Analyzer(data_path="nonexistent_file.csv", save_path= OUTPUT_PATH, columns=cols)

def test_pca_emptydata():
    """
    Test that datafile for pca are empty.
    """
    with pytest.raises(TypeError):
        # Call the function with a non-existent file path
        PC_Analyzer(data_path=INPUT_FILE_PATH, save_path= OUTPUT_PATH, columns=cols)

def test_pca_emptycols():
    """
    Test that cols for pca are empty.
    """
    with pytest.raises(ValueError):
        # Call the function with a non-existent file path
        PC_Analyzer(data_path=INPUT_FILE_PATH, save_path= OUTPUT_PATH, columns=[])


