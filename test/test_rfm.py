import pickle
import os
import sys
import pandas as pd
import pytest
from src.rfm import rfm

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'after_RFM.pkl')

def test_rfm():
   assert rfm(input_pickle_path, output_pickle_path)['Total_Products_Purchased'][1]==2458
   assert rfm(input_pickle_path, output_pickle_path)['Average_Transaction_Value'][1]==615.7142857142857
