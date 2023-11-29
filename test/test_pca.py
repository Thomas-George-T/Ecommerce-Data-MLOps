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
drop = []
remain_4=['Days_Since_Last_Purchase', 'Total_Transactions','Total_Products_Purchased',\
    'Total_Spend', 'Average_Transaction_Value','Unique_Products_Purchased',\
    'Average_Days_Between_Purchases','Day_Of_Week','Hour', 'Is_UK', 'Cancellation_Frequency',\
    'Cancellation_Rate', 'Monthly_Spending_Mean', 'Monthly_Spending_Std','Spending_Trend']

def test_pca_file_not_found():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # non-existent input file path
        pc_analyzer(in_path="nonexistent_file.csv")

def test_missing_columns():
    """
    Test that raises an error when the columns don't exist in df.
    """
    with pytest.raises(KeyError):
        #Call the function with a non-existent file path
        pc_analyzer(out_path=OUTPUT_PATH,drop_cols=['Days_Since_Purchase'])

def test_pca_outfile_exists():
    """
    Test that raises an error if output file does not exist.
    """
    try:
        os.remove(OUTPUT_PATH)
    except FileNotFoundError:
        # Call the function with the sample data
        pc_analyzer(out_path=OUTPUT_PATH,drop_cols=drop)
        assert os.path.exists(OUTPUT_PATH)

def test_cvr_thresh_error():
    """
    Test that raises an error when threshold >1.
    """
    with pytest.raises(ValueError):
        #Call the function with a non-existent file path
        pc_analyzer(out_path=OUTPUT_PATH,drop_cols=drop, cvr_thresh=80)

def test_pca_columnslessthan4():
    """
    Test that raises an error if columns already <4.
    """
    with pytest.raises(ValueError):
        # Call the function with the sample data
        pc_analyzer(out_path=OUTPUT_PATH, drop_cols=remain_4)
