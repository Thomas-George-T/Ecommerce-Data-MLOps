"""
A test module for testing correlation module.
"""
import pytest
import os
import pandas as pd
from pathlib import Path
from src.correlation_analyzer import correlation_check  # Replace 'your_module_name' with the actual name of your module

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE_PATH = os.path.join(PROJECT_DIR, "data", "processed","seasonality.pkl")
drop_cols=[]
test_img_path =  os.path.join(PROJECT_DIR, "test", "test_data","corr.png")
test_txt_path = os.path.join(PROJECT_DIR, "test", "test_data","high_corr.txt")
test_corr_path = os.path.join(PROJECT_DIR, "test", "test_data","corr.parquet")
corr_threshold = 0.5
attribute_error_path=os.path.join(PROJECT_DIR,"\\test\\test_data\\corr.png")

def test_correlation_typecheckimg():
    # Add assertions based on expected outcomes
    with pytest.raises(TypeError):
        correlation_check(data_path=INPUT_FILE_PATH, img_save_path=None, txt_save_path=test_txt_path,corr_data_path=test_corr_path, columns=drop_cols, correlation_threshold=corr_threshold)

def test_correlation_typechecktxt():
    # Add assertions based on expected outcomes
    with pytest.raises(TypeError):
        correlation_check(data_path=INPUT_FILE_PATH, img_save_path=test_img_path, txt_save_path=None,corr_data_path=test_corr_path, columns=drop_cols, correlation_threshold=corr_threshold)

def test_correlation_typecheckcorr():
    # Add assertions based on expected outcomes
    with pytest.raises(TypeError):
        correlation_check(data_path=INPUT_FILE_PATH, img_save_path=test_img_path, txt_save_path=test_txt_path,corr_data_path=None, columns=drop_cols, correlation_threshold=corr_threshold)

def test_correlation_imagesavecheck():
    # Call the function with the sample data
    result = correlation_check(data_path=INPUT_FILE_PATH, img_save_path=test_img_path, txt_save_path=test_txt_path,corr_data_path=test_corr_path, columns=drop_cols, correlation_threshold=corr_threshold)

    # Add assertions based on expected outcomes
    assert os.path.exists(result[0])  # Check if the imageresult is a string (the image save path)
    
def test_correlation_parquetsavecheck():
    # Call the function with the sample data
    result = correlation_check(data_path=INPUT_FILE_PATH, img_save_path=test_img_path, txt_save_path=test_txt_path,corr_data_path=test_corr_path, columns=drop_cols, correlation_threshold=corr_threshold)

    # Add assertions based on expected outcomes      
    assert os.path.exists(result[2])  # Check if the parquet result exists (the image save path)

def test_correlation_FileNotFound():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # Call the function with a non-existent file path
        correlation_check(data_path="nonexistent.csv", img_save_path=test_img_path, txt_save_path=test_txt_path,corr_data_path=test_corr_path, columns=drop_cols, correlation_threshold=corr_threshold)

