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

def test_seasonality_Impacts():
    assert seasonality_Impacts(input_pickle_path, cancellation_pickle_path, output_pickle_path)['Spending_Trend'][0]==0.0
    assert seasonality_Impacts(input_pickle_path, cancellation_pickle_path, output_pickle_path)['Monthly_Spending_Std'][1]==341.07078946836157