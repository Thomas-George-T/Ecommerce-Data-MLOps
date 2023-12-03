"""
This module analyzes Recency, Frequency and Monetary methods to know about the value
of customers and dividing the base.
"""
import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed',
'after_removing_zero_unitprice.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'after_RFM.pkl')

def rfm(input_pickle_file=input_pickle_path, output_pickle_file=output_pickle_path):
    """
    Process customer RFM data based on input pickle file.

    :param input_pickle_file: Input pickle file path containing customer data.
    :param output_pickle_file: Output pickle file path for storing processed RFM data.
    :return: Processed RFM data.
    """
    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['InvoiceDay'] = df['InvoiceDate'].dt.date
    customer_data = df.groupby('CustomerID')['InvoiceDay'].max().reset_index()
    most_recent_date = df['InvoiceDay'].max()
    customer_data['InvoiceDay'] = pd.to_datetime(customer_data['InvoiceDay'])
    most_recent_date = pd.to_datetime(most_recent_date)
    customer_data['Days_Since_Last_Purchase'] =(
        (most_recent_date - customer_data['InvoiceDay']).dt.days
    )
    customer_data.drop(columns=['InvoiceDay'], inplace=True)

    total_transactions = df.groupby('CustomerID')['InvoiceNo'].nunique().reset_index()
    total_transactions.rename(columns={'InvoiceNo': 'Total_Transactions'}, inplace=True)
    total_products_purchased = df.groupby('CustomerID')['Quantity'].sum().reset_index()
    total_products_purchased.rename(columns={'Quantity': 'Total_Products_Purchased'}, inplace=True)
    customer_data = pd.merge(customer_data, total_transactions, on='CustomerID')
    customer_data = pd.merge(customer_data, total_products_purchased, on='CustomerID')

    df['Total_Spend'] = df['UnitPrice'] * df['Quantity']
    total_spend = df.groupby('CustomerID')['Total_Spend'].sum().reset_index()
    average_transaction_value = total_spend.merge(total_transactions, on='CustomerID')
    average_transaction_value['Average_Transaction_Value'] =(
        average_transaction_value['Total_Spend'] / average_transaction_value['Total_Transactions']
    )
    customer_data = pd.merge(customer_data, total_spend, on='CustomerID')
    customer_data =(
        pd.merge(customer_data, average_transaction_value[
            ['CustomerID', 'Average_Transaction_Value']], on='CustomerID')
    )
    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)
    print(f"Data saved to {output_pickle_file}.")
    return output_pickle_file
