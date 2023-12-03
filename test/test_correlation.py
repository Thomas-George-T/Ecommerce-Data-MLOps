"""
A test module for testing correlation module.
"""
import os
import pytest
from src.correlation import correlation_check

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE_PATH = os.path.join(PROJECT_DIR, "data", "processed","scaler_output.parquet")
test_img_path =  os.path.join(PROJECT_DIR, "data", "test_data","corr.png")
test_corr_path = os.path.join(PROJECT_DIR, "data", "test_data","corr.parquet")
OUTPUT_PATH=(test_img_path,test_corr_path)

def test_correlation_filenotfound():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # Call the function with a non-existent file path
        correlation_check(in_path="nonexistent_file.csv")

def test_correlation_parquetpath_error():
    """
    Test that raises an error path is not a string.
    """
    # Add assertions based on expected outcomes
    with pytest.raises(TypeError):
        correlation_check(out_path=(1233,OUTPUT_PATH[1]))

def test_correlation_imagepath_error():
    """
    Test that raises an error path is not a string.
    """
    # Add assertions based on expected outcomes
    with pytest.raises(TypeError):
        correlation_check(out_path=(OUTPUT_PATH[0],1233))

def test_correlation_imagesavecheck():
    """
    Test that raises an error when image not saved correctly.
    """
    # Call the function with the sample data
    try:
        os.remove(OUTPUT_PATH[0])
    except FileNotFoundError:
        # Call the function with the sample data
        correlation_check(out_path=OUTPUT_PATH)
        assert os.path.exists(OUTPUT_PATH[0])

def test_correlation_parquetsavecheck():
    """
    Test that raises an error when parquet not saved correctly.
    """
    # Call the function with the sample data
    try:
        os.remove(OUTPUT_PATH[1])
    except FileNotFoundError:
        # Call the function with the sample data
        correlation_check(out_path=OUTPUT_PATH)
        assert os.path.exists(OUTPUT_PATH[1])

def test_corr_thresh_error():
    """
    Test that raises an error when not 0< thresh <1.
    """
    with pytest.raises(ValueError):
        #Call the function with a non-existent file path
        correlation_check(out_path=OUTPUT_PATH,correlation_threshold=80)
