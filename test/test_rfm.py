import pickle
import os
import sys
import pandas as pd
import pytest
from src.rfm import rfm

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'after_RFM.pkl')

result = rfm(input_pickle_path, output_pickle_path)

def test_rfm():
   assert 'Days_Since_Last_Purchase' in result.columns
   assert 'Total_Transactions' in result.columns
