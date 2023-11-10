"""
This module tests if the RFM function is working.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_RFM.pkl')

if os.path.exists(input_pickle_path):
    with open(input_pickle_path, "rb") as file:
        result= pickle.load(file)

def test_rfm():
    """
      This function raises an AssertionError if the specified columns are not 
      present in the result DataFrame.
    """
    assert 'Days_Since_Last_Purchase' in result.columns
    assert 'Total_Transactions' in result.columns
