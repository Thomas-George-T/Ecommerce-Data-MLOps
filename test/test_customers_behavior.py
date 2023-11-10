"""
This module checks if the customers_behavior function works.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'customers_behavior.pkl')

if os.path.exists(input_pickle_path):
    with open(input_pickle_path, "rb") as file:
        result= pickle.load(file)

def test_customers_behavior():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'CustomerID' in result.columns
    assert 'Hour' in result.columns
