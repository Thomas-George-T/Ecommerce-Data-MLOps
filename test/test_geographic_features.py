"""
This module tests if geographic_features script is working.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'geographic_features.pkl')

if os.path.exists(input_pickle_path):
    with open(input_pickle_path, "rb") as file:
        result= pickle.load(file)

def test_geographic_features():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'Unique_Products_Purchased' in result.columns
    assert 'Is_UK' in result.columns
