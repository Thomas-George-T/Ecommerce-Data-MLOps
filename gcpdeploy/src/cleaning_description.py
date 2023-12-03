"""
A module for cleaning the description column
"""

import os
import pickle

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','after_anomaly_code.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','after_cleaning_description.pkl')

def cleaning_description(input_pickle_path=INPUT_PICKLE_PATH,
                            output_pickle_path=OUTPUT_PICKLE_PATH):
    """
    Load the DataFrame from the input pickle, 
    remove rows where the description cointains service related information
    and Standardize the text to uppercase
    save the DataFrame back to a pickle and return its path.
    
    """
    # Load DataFrame from input pickle
    if os.path.exists(input_pickle_path):
        with open(input_pickle_path, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {input_pickle_path}")

    service_related_descriptions = ["Next Day Carriage", "High Resolution Image"]

    df = df[~df['Description'].isin(service_related_descriptions)]

    df['Description'] = df['Description'].str.upper()

    with open(output_pickle_path, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {output_pickle_path}.")
    return output_pickle_path
