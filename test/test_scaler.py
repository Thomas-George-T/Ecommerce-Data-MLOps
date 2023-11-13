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
path=(INPUT_PATH,OUTPUT_PATH)
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
        scaler(paths=("nonexistent_file.csv",path[1]),columns=cols)

def test_scaler_savepath_error():
    """
    Test that raises an error when the output file directory path doesn't exist.
    """
    with pytest.raises(AssertionError):
        # non-existent output file path
        scaler(paths=(path[0],"src/test/test_scaler_output.parquet"), columns=cols)

def test_missing_std_columns():
    """
    Test that raises an error when the columns don't exist in df.
    """
    with pytest.raises(KeyError):
        #Call the function with a non-existent file path
        scaler(paths=path,columns=(['Days_Since_Purchase'],n_cols))

def test_missing_norm_columns():
    """
    Test that raises an error when the columns don't exist in df.
    """
    with pytest.raises(KeyError):
        #Call the function with a non-existent file path
        scaler(paths=path,columns=(s_cols,['Days_Since_Purchase']))

def test_scaler_outfile_exists():
    """
    Test that raises an error if output file does not exist.
    """
    try:
        os.remove(path[1])
    except FileNotFoundError:
        # Call the function with the sample data
        scaler(paths=path,columns=cols)
        assert os.path.exists(OUTPUT_PATH)
