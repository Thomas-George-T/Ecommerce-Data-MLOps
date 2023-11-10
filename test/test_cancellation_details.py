"""
This module checks if the cancellation_details script works.
"""
import os
import pickle

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'cancellation_details.pkl')

if os.path.exists(input_pickle_path):
    with open(input_pickle_path, "rb") as file:
        result= pickle.load(file)

def test_cancellation_details():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'Cancellation_Frequency' in result.columns
    assert 'Cancellation_Rate' in result.columns
