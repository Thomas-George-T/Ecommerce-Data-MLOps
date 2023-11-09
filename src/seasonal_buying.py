import pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','monthly_spending.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'seasonal_buying.pkl')

def seasonal_Buying(input_pickle_file, output_pickle_file):
    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)

    seasonal_Buying = df.groupby('CustomerID')['Total_Spend'].agg(['mean', 'std']).reset_index()
    seasonal_Buying.rename(columns={'mean': 'Monthly_Spending_Mean', 'std': 'Monthly_Spending_Std'}, inplace=True)
    seasonal_Buying['Monthly_Spending_Std'].fillna(0, inplace=True)


    with open(output_pickle_file, "wb") as file:
        pickle.dump(seasonal_Buying, file)

    return seasonal_Buying

