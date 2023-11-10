"""
This module checks if the seasonality scripts works.
"""
import os
from src.seasonality import seasonality_impacts

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
cancellation_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','cancellation_details.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'seasonality.pkl')

result = seasonality_impacts(input_pickle_path, cancellation_pickle_path, output_pickle_path)

def test_seasonality_impacts():
    """
    This function raises an AssertionError if the specified 
    columns are not present in the result DataFrame.
    """
    assert 'Spending_Trend' in result.columns
    assert 'Monthly_Spending_Std' in result.columns
