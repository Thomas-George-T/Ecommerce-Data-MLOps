"""
This module checks if the cancellation_details script works.
"""
import os
from src.cancellation_details import cancellation_details

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
geographic_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','geographic_features.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'cancellation_details.pkl')

result= cancellation_details(input_pickle_path, geographic_pickle_path, output_pickle_path)

def test_cancellation_details():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'Cancellation_Frequency' in result.columns
    assert 'Cancellation_Rate' in result.columns
