"""
A test module for testing PCA module.
"""
import os
import pytest
from src.pca import pc_analyzer

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH = os.path.join(PROJECT_DIR, "data", "processed","seasonality.pkl")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "data", "test_data","test_pca_output.parquet")
path=(INPUT_PATH,OUTPUT_PATH)
drop = []
attribute_error_path=os.path.join(PROJECT_DIR,"/test/test_data/corr.png")

def test_pca_file_not_found():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # non-existent input file path
        pc_analyzer(paths=("nonexistent_file.csv",path[1]),drop_cols=drop)

def test_pca_savepath_error():
    """
    Test that raises an error when the output file directory path doesn't exist.
    """
    with pytest.raises(AssertionError):
        # non-existent output file path
        pc_analyzer(paths=(path[0],"src/test/test_pca_output.parquet"), drop_cols=drop)

def test_missing_columns():
    """
    Test that raises an error when the columns don't exist in df.
    """
    with pytest.raises(KeyError):
        #Call the function with a non-existent file path
        pc_analyzer(paths=path,drop_cols=['Days_Since_Purchase'])

def test_pca_outfile_exists():
    """
    Test that raises an error if output file does not exist.
    """
    try:
        os.remove(path[1])
    except FileNotFoundError:
        # Call the function with the sample data
        pc_analyzer(paths=path,drop_cols=drop)
        assert os.path.exists(OUTPUT_PATH)

def test_cvr_thresh_error():
    """
    Test that raises an error when threshold >1.
    """
    with pytest.raises(ValueError):
        #Call the function with a non-existent file path
        pc_analyzer(paths=path,drop_cols=drop, cvr_thresh=80)

def test_pca_columnslessthan4():
    """
    Test that raises an error if columns already <4.
    """
    with pytest.raises(ValueError):
        # Call the function with the sample data
        pc_analyzer(paths=path, drop_cols=drop,cvr_thresh=80)
