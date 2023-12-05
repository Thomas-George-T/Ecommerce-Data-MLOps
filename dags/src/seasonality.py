"""
This module analyzes the seasonal trends and how it affects customers and business
"""
import  pickle
import os
import pandas as pd
import numpy as np
import scipy.stats as sc

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
cancellation_pickle_path=os.path.join(PROJECT_DIR, 'data','processed',
                                    'cancellation_details.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data', 'processed', 'seasonality.pkl')
transaction_filepath = os.path.join(PROJECT_DIR, 'data','processed',
                                    'transaction_dataframe.parquet')

def seasonality_impacts(input_pickle_file=input_pickle_path,
cancellation_pickle_file=cancellation_pickle_path, output_pickle_file=output_pickle_path,
data_filepath=transaction_filepath):
    """
    Calculate seasonality impacts and trends for customer spending.

    :param input_pickle_file: Input pickle file path containing customer spending data.
    :param cancellation_pickle_file: Input pickle file path containing data from
    cancellation_details.
    :param output_pickle_file: Output pickle file path for storing processed customer data.
    :return: Processed customer data with seasonality impacts and trends.
    """

    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)

    if os.path.exists(cancellation_pickle_file):
        with open(cancellation_pickle_file, "rb") as file:
            customer_data = pickle.load(file)

    df['Total_Spend'] = df['UnitPrice'] * df['Quantity']
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month
    df['StockCode'] = df['StockCode'].astype(str).apply(lambda x: x.encode('utf-8'))
    df.to_parquet(data_filepath)
    monthly_spending = df.groupby(['CustomerID', 'Year',
    'Month'])['Total_Spend'].sum().reset_index()
    seasonal_buying_patterns =(
        monthly_spending.groupby('CustomerID')['Total_Spend'].agg(['mean', 'std']).reset_index()
    )
    seasonal_buying_patterns.rename(columns={'mean': 'Monthly_Spending_Mean',
    'std': 'Monthly_Spending_Std'}, inplace=True)
    seasonal_buying_patterns['Monthly_Spending_Std'].fillna(0, inplace=True)

    def calculate_trend(spend_data):

        if len(spend_data) > 1:
            x = np.arange(len(spend_data))
            slope, _, _, _, _ = sc.linregress(x, spend_data)
            result= slope

            return result

        return 0

    spending_trends =(
        monthly_spending.groupby('CustomerID')['Total_Spend'].apply(calculate_trend).reset_index()
    )
    spending_trends.rename(columns={'Total_Spend': 'Spending_Trend'}, inplace=True)
    customer_data = pd.merge(customer_data, seasonal_buying_patterns, on='CustomerID')
    customer_data = pd.merge(customer_data, spending_trends, on='CustomerID')
    customer_data['CustomerID'] = customer_data['CustomerID'].astype(str)
    customer_data = customer_data.convert_dtypes()

    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)
    print(f"Data saved to {output_pickle_file}.")
    return output_pickle_file
