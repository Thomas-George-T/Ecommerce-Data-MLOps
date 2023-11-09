import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
rfm_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_rfm.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'unique_products.pkl')

def unique_Products(input_pickle_file, rfm_pickle_file , output_pickle_file):
    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)
    
    if os.path.exists(rfm_pickle_file):
        with open(rfm_pickle_file, "rb") as file:
            input_customer_data = pickle.load(file)
    
    unique_products_purchased = df.groupby('CustomerID')['StockCode'].nunique().reset_index()
    unique_products_purchased.rename(columns={'StockCode': 'Unique_Products_Purchased'}, inplace=True)
    customer_data = pd.merge(input_customer_data, unique_products_purchased, on='CustomerID')
    
    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)

    return customer_data
    
