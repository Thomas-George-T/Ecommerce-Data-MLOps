import os
import pandas as pd
import pickle


import os
import pandas as pd
import pickle

DEFAULT_PICKLE_PATH = "../data/raw_data.pkl"
DEFAULT_CSV_PATH = "../data/data.csv"

def load_data(pickle_path=DEFAULT_PICKLE_PATH, csv_path=DEFAULT_CSV_PATH):
    """
    Load the e-commerce dataset. 
    First, try to load from the pickle file. If it doesn't exist, load from the CSV.
    Regardless of the source, save the loaded data as a pickle for future use and return the path to that pickle.
    
    :param pickle_path: Path to the pickle file.
    :param csv_path: Path to the CSV file.
    :return: Path to the saved pickle file.
    """
    
    # Placeholder for the DataFrame
    df = None

    # Check if pickle file exists
    if os.path.exists(pickle_path):
        with open(pickle_path, "rb") as file:
            df = pickle.load(file)
        print(f"Data loaded successfully from {pickle_path}.")
    
    # If pickle doesn't exist, load CSV
    elif os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        print(f"Data loaded from {csv_path}.")

    else:
        error_message = f"No data found in the specified paths: {pickle_path} or {csv_path}"
        print(error_message)
        raise FileNotFoundError(error_message)

    # Save the data to pickle for future use (or re-save it if loaded from existing pickle)
    with open(pickle_path, "wb") as file:
        pickle.dump(df, file)
    
    print(f"Data saved to {pickle_path} for future use.")
    return pickle_path







