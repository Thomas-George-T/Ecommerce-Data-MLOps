"""
A test module for testing pca module.
"""

import os
import pytest
from pathlib import Path
from src.pca_rev import PC_Analyzer

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE_PATH = os.path.join(PROJECT_DIR, "data", "processed","seasonality.pkl")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "test", "test_data","test_pca_output.parquet")
cols = ['Days_Since_Last_Purchase', 'Total_Transactions',
       'Total_Products_Purchased', 'Total_Spend', 'Average_Transaction_Value',
       'Unique_Products_Purchased', 'Average_Days_Between_Purchases','Cancellation_Frequency',
       'Cancellation_Rate', 'Monthly_Spending_Mean', 'Monthly_Spending_Std',
       'Spending_Trend']
attribute_error_path=os.path.join(PROJECT_DIR,"\\test\\test_data\\corr.png")

def test_pca_ColumnsNotinFile():
       """
       Test that raises an error when the input files doesn't exist.
       """
       with pytest.raises(KeyError):
       #Call the function with a non-existent file path
              PC_Analyzer(data_path=INPUT_FILE_PATH, save_path= OUTPUT_PATH, columns=['Days_Since_Last_Purchase', 'Total_Transactions','Total_Products_Purchased', 'Total_Spend',"Number" ],cvr=0.75, indexed_on="CustomerID")

def test_pca_result():
       """
       Test that raises an error on output files issues.
       """
       # Call the function with the sample data
       result = PC_Analyzer(data_path=INPUT_FILE_PATH, save_path= OUTPUT_PATH, columns=cols,cvr=0.75, indexed_on="CustomerID")

       # Add assertions based on expected outcomes
       assert isinstance(result, str)  # Check if the result is a string (the save path)
       assert "test_pca_output.parquet" in result  # Check if the save path contains the expected file name

def test_pca_columnslessthan2():
       """
       Test that raises an error on output files issues.
       """
       with pytest.raises(ValueError):
       # Call the function with the sample data
              result = PC_Analyzer(data_path=INPUT_FILE_PATH, save_path= OUTPUT_PATH, columns=['Days_Since_Last_Purchase', 'Total_Transactions'],cvr=0.75, indexed_on="CustomerID")

def test_pca_FileNotFound():
    """
    Test that raises an error when the input files doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
       # Call the function with a non-existent file path
       PC_Analyzer(data_path="nonexistent_file.csv", save_path= OUTPUT_PATH, columns=cols,cvr=0.75, indexed_on="CustomerID")


