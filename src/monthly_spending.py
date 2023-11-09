import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'monthly_spending.pkl')

def monthly_Spending(input_pickle_file= input_pickle_path, output_pickle_file):
    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)

    df['Total_Spend'] = df['UnitPrice'] * df['Quantity']
    df["InvoiceDate"]=pd.to_datetime(df.InvoiceDate)
    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month

    #Calculate monthly spending for each customer
    monthly_spending = df.groupby(['CustomerID', 'Year', 'Month'])['Total_Spend'].sum().reset_index()

    with open(output_pickle_file, "wb") as file:
        pickle.dump(monthly_spending, file)
    
    return monthly_spending

print(monthly_Spending(input_pickle_path, output_pickle_path))

