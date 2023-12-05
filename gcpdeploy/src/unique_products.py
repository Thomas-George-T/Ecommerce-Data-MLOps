"""
This module groups the values based on unique values of CustomerID and orders.
"""
import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
rfm_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_RFM.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'unique_products.pkl')

def unique_products(input_pickle_file=input_pickle_path, rfm_pickle_file=rfm_pickle_path ,
output_pickle_file=output_pickle_path):
    """
    Calculate the number of unique products purchased by each customer.

    :param input_pickle_file: Input pickle file path containing transaction data.
    :param rfm_pickle_file: Input pickle file path containing RFM data.
    :param output_pickle_file: Output pickle file path for storing processed customer data
                               with unique product information.
    :return: Processed customer data with added information about unique products purchased.
    """
    customer_data = pd.DataFrame()

    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)

    if os.path.exists(rfm_pickle_file):
        with open(rfm_pickle_file, "rb") as file:
            customer_data = pickle.load(file)

    if 'CustomerID' in df.columns and not df.empty:
        unique_products_purchased = (
            df.groupby('CustomerID')['StockCode']
            .nunique()
            .reset_index()
            .rename(columns={'StockCode': 'Unique_Products_Purchased'})
        )

        customer_data = pd.merge(
            customer_data,
            unique_products_purchased[unique_products_purchased['CustomerID']
            .isin(customer_data['CustomerID'])],
            on='CustomerID',
        )
    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)
    print(f"Data saved to {output_pickle_file}.")
    return output_pickle_file
