"""
A test for checking outlier treatment
"""

import os
import pickle
from src.outlier_treatment import removing_outlier


# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','seasonality.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_outlier_treatment.pkl')


def test_outlier_treatment():
    """
    Test to check if outliers are removed
    """
    result = removing_outlier(input_pickle_path=INPUT_PICKLE_PATH,
                                    output_pickle_path=OUTPUT_PICKLE_PATH)

    assert result == OUTPUT_PICKLE_PATH, \
        f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."

    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)

    with open(INPUT_PICKLE_PATH, "rb") as file:
        df_input= pickle.load(file)

    #checking if outliers have been removed

    assert len(df_input) > len(df)
