"""
This module tests if geographic_features script is working.
"""
import os
from src.geographic_features import geographic_features

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
behavorial_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','customers_behavior.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'geographic_features.pkl')

result= geographic_features(input_pickle_path, behavorial_pickle_path, output_pickle_path)

def test_geographic_features():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'Unique_Products_Purchased' in result.columns
    assert 'Is_UK' in result.columns
