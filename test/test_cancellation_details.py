import  pickle
import os
import pandas as pd
import pytest
from src.cancellation_details import cancellation_Details

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
geographic_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','geographic_features.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'cancellation_details.pkl')

result= cancellation_Details(input_pickle_path, geographic_pickle_path, output_pickle_path)

def test_cancellation_Details():
    assert 'Cancellation_Frequency' in result.columns
    assert 'Cancellation_Rate' in result.columns