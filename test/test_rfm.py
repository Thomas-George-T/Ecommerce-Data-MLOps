"""
This module tests if the RFM function is working.
"""
import os
from src.rfm import rfm

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'after_RFM.pkl')

result = rfm(input_pickle_path, output_pickle_path)

def test_rfm():
    """
      This function raises an AssertionError if the specified columns are not 
      present in the result DataFrame.
    """
    assert 'Days_Since_Last_Purchase' in result.columns
    assert 'Total_Transactions' in result.columns
