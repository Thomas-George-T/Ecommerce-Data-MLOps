"""
A module for removing anomalies from StockCode column if they have 0 or 1 
digit characters since the normal values are 5 or 6 digits.
"""

import pickle
import os

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_transaction_status.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_anomaly_code.pkl')

def handle_anomalous_codes(input_pickle_path=INPUT_PICKLE_PATH,
                           output_pickle_path=OUTPUT_PICKLE_PATH):
    """
    Load the DataFrame from the input pickle, remove rows with stock codes that 
    have 0 or 1 numeric characters, 
    then save the DataFrame back to a pickle and return its path.
    
    :param input_pickle_path: Path to the input pickle file.
    :param output_pickle_path: Path to the output pickle file.
    :return: Path to the saved pickle file.
    """
    # Load DataFrame from input pickle
    if os.path.exists(input_pickle_path):
        with open(input_pickle_path, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {input_pickle_path}")
    # Finding the stock codes with 0 and 1 numeric characters
    unique_stock_codes = df['StockCode'].unique()
    anomalous_stock_codes = [code for code in unique_stock_codes if
                             sum(c.isdigit() for c in str(code)) in (0, 1)]
    # Removing rows with these anomalous stock codes
    df = df[~df['StockCode'].isin(anomalous_stock_codes)]
    # Save the data to output pickle
    with open(output_pickle_path, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {output_pickle_path}.")
    return output_pickle_path
