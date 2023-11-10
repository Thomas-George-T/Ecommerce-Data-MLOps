import  pickle
import os
import pandas as pd
import pytest
from src.geographic_features import geographic_Features

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
behavorial_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','customers_behavior.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'geographic_features.pkl')

result= geographic_Features(input_pickle_path, behavorial_pickle_path, output_pickle_path)

def test_geographic_Features():
    assert 'Unique_Products_Purchased' in result.columns  
    assert 'Is_UK' in result.columns