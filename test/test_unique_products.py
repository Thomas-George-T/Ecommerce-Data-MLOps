"""
This module tests if the unique_products script works.
"""
import os
from src.unique_products import unique_products

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
rfm_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_rfm.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'unique_products.pkl')

result= unique_products(input_pickle_path, rfm_pickle_path, output_pickle_path)
def test_unique_products():
    """
     This function raises an Assertion error if the columns are not present in the result.
    """
    assert 'Unique_Products_Purchased' in result.columns
    assert 'Average_Transaction_Value' in result.columns
