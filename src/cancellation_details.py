import  pickle
import os
import pandas as pd
import numpy as np

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
geographic_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','geographic_features.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'cancellation_details.pkl')

def cancellation_Details(input_pickle_file, geographic_pickle_file, output_pickle_file):
    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)
    
    if os.path.exists(geographic_pickle_file):
        with open(geographic_pickle_file, "rb") as file:
            customer_data = pickle.load(file)

    total_transactions = df.groupby('CustomerID')['InvoiceNo'].nunique().reset_index()
    df['Transaction_Status'] = np.where(df['InvoiceNo'].astype(str).str.startswith('C'), 'Cancelled', 'Completed')
    cancelled_transactions = df[df['Transaction_Status'] == 'Cancelled']
    cancellation_frequency = cancelled_transactions.groupby('CustomerID')['InvoiceNo'].nunique().reset_index()
    cancellation_frequency.rename(columns={'InvoiceNo': 'Cancellation_Frequency'}, inplace=True)
    customer_data = pd.merge(customer_data, cancellation_frequency, on='CustomerID', how='left')
    customer_data['Cancellation_Frequency'].fillna(0, inplace=True)
    customer_data['Cancellation_Rate'] = customer_data['Cancellation_Frequency'] / total_transactions['InvoiceNo']

    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)

    return customer_data
