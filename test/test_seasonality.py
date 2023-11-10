import  pickle
import os
import pandas as pd
import numpy as np
import scipy.stats as sc
from src.seasonality import seasonality_Impacts

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
cancellation_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','cancellation_details.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'seasonality.pkl')

result = seasonality_Impacts(input_pickle_path, cancellation_pickle_path, output_pickle_path)

def test_seasonality_Impacts():
    assert 'Spending_Trend' in result.columns
    assert 'Monthly_Spending_Std' in result.columns