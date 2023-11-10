"""
Module to handle the loading of e-commerce dataset from either pickle or Excel file format.
"""

import pickle
import os
import pandas as pd

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Use the project directory to construct paths to other directories
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                   'processed', 'raw_data.pkl')
DEFAULT_EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'Online Retail.xlsx')

def load_data(pickle_path=DEFAULT_PICKLE_PATH, excel_path=DEFAULT_EXCEL_PATH):
    """
    Load the e-commerce dataset.
    First, try to load from the pickle file. If it doesn't exist, load from the excel file.
    Regardless of the source, save the loaded data as a pickle for future use and
    return the path to that pickle.
    
    :param pickle_path: Path to the pickle file.
    :param excel_path: Path to the Excel file.
    :return: Path to the saved pickle file.
    """
    # Placeholder for the DataFrame
    df = None
    # Check if pickle file exists
    if os.path.exists(pickle_path):
        with open(pickle_path, "rb") as file:
            df = pickle.load(file)
        print(f"Data loaded successfully from {pickle_path}.")
    # If pickle doesn't exist, load from Excel
    elif os.path.exists(excel_path):
        df = pd.read_excel(excel_path)
        print(f"Data loaded from {excel_path}.")
    else:
        error_message = f"No data found in the specified paths: {pickle_path} or {excel_path}"
        print(error_message)
        raise FileNotFoundError(error_message)
    # Save the data to pickle for future use (or re-save it if loaded from existing pickle)
    os.makedirs(os.path.dirname(pickle_path), exist_ok=True)
    with open(pickle_path, "wb") as file:
        pickle.dump(df, file)
    print(f"Data saved to {pickle_path} for future use.")
    return pickle_path
