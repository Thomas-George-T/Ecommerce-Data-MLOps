"""
This module checks if the seasonality scripts works.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'seasonality.pkl')

if os.path.exists(input_pickle_path):
    with open(input_pickle_path, "rb") as file:
        result= pickle.load(file)

def test_seasonality_impacts():
    """
    This function raises an AssertionError if the specified 
    columns are not present in the result DataFrame.
    """
    assert 'Spending_Trend' in result.columns
    assert 'Monthly_Spending_Std' in result.columns
