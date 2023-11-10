import  pickle
import os
import pandas as pd
import pytest
from src.customers_behavior import customers_Behavior

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
unique_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','unique_products.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'customers_behavior.pkl')

result= customers_Behavior(input_pickle_path, unique_pickle_path, output_pickle_path)

def test_customers_Behavior():
    assert 'CustomerID' in result.columns
    assert 'Hour' in result.columns