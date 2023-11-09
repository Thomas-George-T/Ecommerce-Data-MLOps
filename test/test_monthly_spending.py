import os
import sys
import pickle

os.chdir('C:\DAE\MLOps\Project\Ecommerce-Data-MLOps\src')
sys.path.append('C:\DAE\MLOps\Project\Ecommerce-Data-MLOps\src')

from monthly_spending import monthly_Spending

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'monthly_spending.pkl')



def test_monthly_Spending():
   assert monthly_Spending(input_pickle_path, output_pickle_path)['CustomerID'][1]==12347.0
   assert monthly_Spending(input_pickle_path, output_pickle_path)['Year'][1]==2010
