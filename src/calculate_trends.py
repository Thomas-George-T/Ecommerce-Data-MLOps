import pickle
import os
import pandas as pd
import numpy as np
import scipy.stats as sc

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
monthly_input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','monthly_spending.pkl')
seasonal_input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','seasonal_buying.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'calculate_trends.pkl')



def customer_data(monthly_input_pickle_file, seasonal_input_pickle_file, output_pickle_file):
    
    if os.path.exists(monthly_input_pickle_file) and os.path.exists(seasonal_input_pickle_file):
       
        with open(monthly_input_pickle_file, "rb") as file:
            monthly_df = pickle.load(file)
        
        with open(seasonal_input_pickle_file, "rb") as file:
            seasonal_df = pickle.load(file)
        
        def calculate_trend(spend_data):
            
            if len(spend_data) > 1:
                x = np.arange(len(spend_data))
                slope, _, _, _, _ = sc.linregress(x, spend_data)
                return slope
            
            else:
                return 0
        
        spending_trends = monthly_df.groupby('CustomerID')['Total_Spend'].apply(calculate_trend).reset_index()
        spending_trends.rename(columns={'Total_Spend': 'Spending_Trend'}, inplace=True)

        
        customer_data = pd.merge(monthly_df, seasonal_df, on='CustomerID')
        customer_data = pd.merge(customer_data, spending_trends, on='CustomerID')
        customer_data['CustomerID'] = customer_data['CustomerID'].astype(str)

       
        customer_data = customer_data.convert_dtypes()

    else:
        raise FileNotFoundError("Input pickle files not found")

    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)
    
    return customer_data

