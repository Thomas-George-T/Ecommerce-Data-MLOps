"""
A test for check the removal of zero unit prices
"""

import os
import pickle
from src.removing_zero_unitprice import removing_zero


# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                 'processed','after_cleaning_description.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data',
                                  'processed', 'after_removing_zero_unitprice.pkl')


def test_removing_zero():
    """
    Test to check if zero unit prices have been removed
    """
    result = removing_zero(input_pickle_path=INPUT_PICKLE_PATH,
                                    output_pickle_path=OUTPUT_PICKLE_PATH)

    assert result == OUTPUT_PICKLE_PATH, \
        f"Expected {OUTPUT_PICKLE_PATH}, but got {result}."

    with open(OUTPUT_PICKLE_PATH, "rb") as file:
        df = pickle.load(file)

    #checking if zero unit prices have been removed
    assert len(df[df['UnitPrice'] == 0]) == 0
