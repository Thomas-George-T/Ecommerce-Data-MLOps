"""
A test module for testing_scaler module.
"""
import os
import pytest
from src.scaler import scaler

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH = os.path.join(PROJECT_DIR, "data", "processed","seasonality.pkl")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "data", "test_data","test_scaler_output.parquet")
n_cols = ["Days_Since_Last_Purchase", "Total_Transactions", "Total_Spend",
"Average_Transaction_Value","Unique_Products_Purchased", "Cancellation_Rate",
"Monthly_Spending_Mean","Monthly_Spending_Std","Spending_Trend"]
s_cols = ["Total_Products_Purchased","Average_Days_Between_Purchases","Cancellation_Frequency"]
cols = (s_cols,n_cols)
attribute_error_path=os.path.join(PROJECT_DIR,"/test/test_data/corr.png")

def test_scaler_file_not_found():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        # non-existent input file path
        scaler(in_path="nonexistent_file.csv")

def test_missing_std_columns():
    """
    Test that raises an error when the columns don't exist in df.
    """
    with pytest.raises(KeyError):
        #Call the function with a non-existent file path
        scaler(out_path=OUTPUT_PATH,cols=(['Days_Since_Purchase'],n_cols))

def test_missing_norm_columns():
    """
    Test that raises an error when the columns don't exist in df.
    """
    with pytest.raises(KeyError):
        #Call the function with a non-existent file path
        scaler(out_path=OUTPUT_PATH,cols=(s_cols,['Days_Since_Purchase']))

def test_scaler_outfile_exists():
    """
    Test that raises an error if output file does not exist.
    """
    try:
        os.remove(OUTPUT_PATH)
    except FileNotFoundError:
        # Call the function with the sample data
        scaler(out_path=OUTPUT_PATH)
        assert os.path.exists(OUTPUT_PATH)
