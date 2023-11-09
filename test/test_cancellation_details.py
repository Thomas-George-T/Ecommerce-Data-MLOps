import  pickle
import os
import pandas as pd
import pytest
from src.cancellation_details import cancellation_Details

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
geographic_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','geographic_features.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'cancellation_details.pkl')

def test_cancellation_Details():
    assert cancellation_Details(input_pickle_path, geographic_pickle_path, output_pickle_path)['Cancellation_Frequency'][1]==0.0   
    assert cancellation_Details(input_pickle_path, geographic_pickle_path, output_pickle_path)['Cancellation_Rate'][0]==0.5