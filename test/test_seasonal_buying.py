import pickle
import os
import sys
import pandas as pd

os.chdir('C:\DAE\MLOps\Project\Ecommerce-Data-MLOps\src')
sys.path.append('C:\DAE\MLOps\Project\Ecommerce-Data-MLOps\src')

from seasonal_buying import seasonal_Buying

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','monthly_spending.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'seasonal_buying.pkl')

def test_seasonal_Buying():
   assert seasonal_Buying(input_pickle_path, output_pickle_path)['CustomerID'][1]==12347.0
   assert seasonal_Buying(input_pickle_path, output_pickle_path)['Monthly_Spending_Mean'][2]==449.310000 
