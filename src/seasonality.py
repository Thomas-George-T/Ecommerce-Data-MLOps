import  pickle
import os
import pandas as pd
import numpy as np
import scipy.stats as sc

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
cancellation_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','cancellation_details.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'seasonality.pkl')

def seasonality_Impacts(input_pickle_file, cancellation_pickle_file, output_pickle_file):
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

    monthly_spending = df.groupby(['CustomerID', 'Year', 'Month'])['Total_Spend'].sum().reset_index()
    seasonal_buying_patterns = monthly_spending.groupby('CustomerID')['Total_Spend'].agg(['mean', 'std']).reset_index()
    seasonal_buying_patterns.rename(columns={'mean': 'Monthly_Spending_Mean', 'std': 'Monthly_Spending_Std'}, inplace=True)
    seasonal_buying_patterns['Monthly_Spending_Std'].fillna(0, inplace=True)

    def calculate_trend(spend_data):

        if len(spend_data) > 1:
            x = np.arange(len(spend_data))
            slope, _, _, _, _ = sc.linregress(x, spend_data)
            return slope

        else:
            return 0

    spending_trends = monthly_spending.groupby('CustomerID')['Total_Spend'].apply(calculate_trend).reset_index()
    spending_trends.rename(columns={'Total_Spend': 'Spending_Trend'}, inplace=True)
    customer_data = pd.merge(customer_data, seasonal_buying_patterns, on='CustomerID')
    customer_data = pd.merge(customer_data, spending_trends, on='CustomerID')
    customer_data['CustomerID'] = customer_data['CustomerID'].astype(str)
    customer_data = customer_data.convert_dtypes()

    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)

    return customer_data
