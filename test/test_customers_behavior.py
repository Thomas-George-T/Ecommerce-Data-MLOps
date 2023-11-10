"""
This module checks if the customers_behavior function works.
"""
import os
from src.customers_behavior import customers_behavior

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
unique_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','unique_products.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'customers_behavior.pkl')

result= customers_behavior(input_pickle_path, unique_pickle_path, output_pickle_path)

def test_customers_behavior():
    """
    This function raises an AssertionError if the specified columns are not
    present in the result DataFrame.
    """
    assert 'CustomerID' in result.columns
    assert 'Hour' in result.columns
