"""
This module tests if the unique_products script works.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'unique_products.pkl')

if os.path.exists(input_pickle_path):
    with open(input_pickle_path, "rb") as file:
        result= pickle.load(file)

def test_unique_products():
    """
     This function raises an Assertion error if the columns are not present in the result.
    """
    assert 'Unique_Products_Purchased' in result.columns
    assert 'Average_Transaction_Value' in result.columns
