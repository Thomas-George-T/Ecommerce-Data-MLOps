import  pickle
import os
import pandas as pd

PROJECT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','after_missing_values.pkl')
behavorial_pickle_path=os.path.join(PROJECT_DIR, 'data', 'processed','customers_behavior.pkl')
output_pickle_path = os.path.join(PROJECT_DIR, 'data','processed', 'geographic_features.pkl')

def geographic_Features(input_pickle_file, behavorial_pickle_file, output_pickle_file):
    if os.path.exists(input_pickle_file):
        with open(input_pickle_file, "rb") as file:
            df = pickle.load(file)
    
    if os.path.exists(behavorial_pickle_file):
        with open(behavorial_pickle_file, "rb") as file:
            customer_data = pickle.load(file)

    df['Country'].value_counts(normalize=True).head()
    customer_country = df.groupby(['CustomerID', 'Country']).size().reset_index(name='Number_of_Transactions')
    customer_main_country = customer_country.sort_values('Number_of_Transactions', ascending=False).drop_duplicates('CustomerID')
    customer_main_country['Is_UK'] = customer_main_country['Country'].apply(lambda x: 1 if x == 'United Kingdom' else 0)
    customer_data = pd.merge(customer_data, customer_main_country[['CustomerID', 'Is_UK']], on='CustomerID', how='left')

    with open(output_pickle_file, "wb") as file:
        pickle.dump(customer_data, file)

    return customer_data

