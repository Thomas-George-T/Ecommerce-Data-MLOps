import os
from pathlib import Path


def load_ecommerce_data(path=None, encoding="ISO-8859-1"):
    """
    Load the E-commerce dataset.

    Parameters:
    - path (str or Path): Path to the E-commerce CSV file. Default is set to the provided directory and file name.

    Returns:
    - pd.DataFrame: Loaded dataframe if the file exists and is not empty.
    """
    import pandas as pd

    # If no path provided, set default relative path
    if path is None:
        # Get the current directory (assuming this code is run from the src directory)
        current_dir = Path(__file__).parent
        # Create the relative path to the data directory
        path = current_dir.joinpath('../data/data.csv').resolve()

    # Check if the file exists
    if not os.path.exists(path):
        print(f"The file {path} does not exist!")
        raise FileNotFoundError(f"File {path} not found!")
    
    # Load the data to check if it's empty
    data = pd.read_csv(path, encoding=encoding)
    
    if data.empty:
        print(f"The file {path} is empty!")
        raise EmptyDataError(f"No data found in {path}!")
    
    return data
