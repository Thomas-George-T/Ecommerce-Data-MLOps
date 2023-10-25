import os
import pandas as pd
import pickle


def load_data(pickle_path="../data/raw_data.pkl", csv_path="../data/data.csv"):
    """
    Load the e-commerce dataset. 
    First, try to load from the pickle file, if it doesn't exist, load from the CSV and then save it as a pickle for future use.

    :param pickle_path: Path to the pickle file.
    :param csv_path: Path to the CSV file.
    :return: Loaded DataFrame.
    """
    
    # Check if pickle file exists
    if os.path.exists(pickle_path):
        with open(pickle_path, "rb") as file:
            df = pickle.load(file)
        return df
    
    # If pickle doesn't exist, load CSV
    elif os.path.exists(csv_path):
        df = pd.read_csv(csv_path)

        # Save the data to pickle for future use
        with open(pickle_path, "wb") as file:
            pickle.dump(df, file)
        
        return df
    
    else:
        error_message = f"No data found in the specified paths: {pickle_path} or {csv_path}"
        print(error_message)
        raise FileNotFoundError(error_message)






