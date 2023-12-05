"""
A module to detect and remove outliers
"""


import os
import pickle
from sklearn.ensemble import IsolationForest

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','seasonality.pkl')
OUTPUT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed','after_outlier_treatment.pkl')

parquet_file_path = os.path.join(PROJECT_DIR, 'data', 'processed','df_outlier.parquet')

def removing_outlier(input_pickle_path=INPUT_PICKLE_PATH, output_pickle_path=OUTPUT_PICKLE_PATH):
    """
     Load the DataFrame from the input pickle, 
    detect and remove outliers
    save the DataFrame back to a pickle and return its path
    
    """
    # Load DataFrame from input pickle
    if os.path.exists(input_pickle_path):
        with open(input_pickle_path, "rb") as file:
            df = pickle.load(file)
    else:
        raise FileNotFoundError(f"No data found at the specified path: {input_pickle_path}")

    model = IsolationForest(contamination=0.05, random_state=0)

    df['Outlier_Scores'] = model.fit_predict(df.iloc[:, 1:].to_numpy())

    df['Is_Outlier'] = [1 if x == -1 else 0 for x in df['Outlier_Scores']]

    df_outlier = df[df['Is_Outlier'] == 1]

    df_cleaned = df[df['Is_Outlier'] == 0]

    #dropping the columns
    df_cleaned = df_cleaned.drop(columns=['Outlier_Scores', 'Is_Outlier'])

    #reseting the index
    df_cleaned.reset_index(drop=True, inplace=True)

    df_outlier.to_parquet(parquet_file_path, index=False)


    with open(output_pickle_path, "wb") as file:
        pickle.dump(df_cleaned, file)
    print(f"Data saved to {output_pickle_path}.")
    return output_pickle_path
