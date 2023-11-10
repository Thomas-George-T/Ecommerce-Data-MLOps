"""
A module for removing unit prices with a value of zero
"""


import os
import pickle

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','after_cleaning_description.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                'processed','after_removing_zero_unitprice.pkl')

def removing_zero(input_pickle_path=INPUT_PICKLE_PATH, output_pickle_path=OUTPUT_PICKLE_PATH):
    """
     Load the DataFrame from the input pickle, 
    remove rows where unit price is zero
    save the DataFrame back to a pickle and return its path
    
    """
    # Load DataFrame from input pickle
    if os.path.exists(input_pickle_path):
        with open(input_pickle_path, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {input_pickle_path}")

    df = df[df['UnitPrice'] > 0]


    with open(output_pickle_path, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {output_pickle_path}.")
    return output_pickle_path
