import  pickle
import os
import pandas as pd
import pytest
from src.unique_products import unique_Products

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
rfm_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_rfm.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'unique_products.pkl')

result= unique_Products(input_pickle_path, rfm_pickle_path, output_pickle_path)

def test_unique_Products():
    assert 'Unique_Products_Purchased' in result.columns
    assert 'Average_Transaction_Value' in result.columns