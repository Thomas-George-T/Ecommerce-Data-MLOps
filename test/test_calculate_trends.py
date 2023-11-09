import pickle
import os
import sys
import pandas as pd

os.chdir('C:\DAE\MLOps\Project\Ecommerce-Data-MLOps\src')
sys.path.append('C:\DAE\MLOps\Project\Ecommerce-Data-MLOps\src')

from calculate_trends import customer_data

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
monthly_input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','monthly_spending.pkl')
seasonal_input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','seasonal_buying.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'calculate_trends.pkl')


def test_customer_data():
   assert customer_data(monthly_input_pickle_path, seasonal_input_pickle_path, output_pickle_path)['Year'][3]==2011
   assert customer_data(monthly_input_pickle_path, seasonal_input_pickle_path, output_pickle_path)['Month'][2]==1
