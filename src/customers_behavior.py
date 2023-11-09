import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
unique_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','unique_products.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'customers_behavior.pkl')

def customers_Behavior(input_pickle_file, unique_pickle_file, output_pickle_file):
    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)
    
    if os.path.exists(unique_pickle_file):
        with open(unique_pickle_file, "rb") as file:
            customer_data = pickle.load(file)

 
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['InvoiceDay'] = df['InvoiceDate'].dt.date
    df['Day_Of_Week'] = df['InvoiceDate'].dt.dayofweek
    df['Hour'] = df['InvoiceDate'].dt.hour

    days_between_purchases = df.groupby('CustomerID')['InvoiceDay'].apply(lambda x: (x.diff().dropna()).apply(lambda y: y.days))
    average_days_between_purchases = days_between_purchases.groupby('CustomerID').mean().reset_index()
    average_days_between_purchases.rename(columns={'InvoiceDay': 'Average_Days_Between_Purchases'}, inplace=True)

    favorite_shopping_day = df.groupby(['CustomerID', 'Day_Of_Week']).size().reset_index(name='Count')
    favorite_shopping_day = favorite_shopping_day.loc[favorite_shopping_day.groupby('CustomerID')['Count'].idxmax()][['CustomerID', 'Day_Of_Week']]

    favorite_shopping_hour = df.groupby(['CustomerID', 'Hour']).size().reset_index(name='Count')
    favorite_shopping_hour = favorite_shopping_hour.loc[favorite_shopping_hour.groupby('CustomerID')['Count'].idxmax()][['CustomerID', 'Hour']]

    customer_data = pd.merge(customer_data, average_days_between_purchases, on='CustomerID')
    customer_data = pd.merge(customer_data, favorite_shopping_day, on='CustomerID')
    customer_data = pd.merge(customer_data, favorite_shopping_hour, on='CustomerID')

    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)
        
    return customer_data
