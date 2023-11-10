"""
Modularized Data pipeline to form DAGs in the future
"""
from download_data import ingest_data
from unzip_data import unzip_file
from data_loader import load_data
from missing_values_handler import handle_missing
from duplicates_handler import remove_duplicates
from transaction_status_handler import handle_transaction_status
from anomaly_code_handler import handle_anomalous_codes


if __name__ == "__main__":
    ZIPFILE_PATH = ingest_data(
        """https://archive.ics.uci.edu/static/public/352/online+retail.zip""")
    UNZIPPED_FILE = unzip_file(ZIPFILE_PATH, 'data')
    LOADED_DATA_PATH = load_data(excel_path=UNZIPPED_FILE)
    AFTER_MISSING_PATH = handle_missing(input_pickle_path=LOADED_DATA_PATH)
    AFTER_DUPLICATES_PATH = remove_duplicates(input_pickle_path=AFTER_MISSING_PATH)
    AFTER_TRANSACTION_PATH = handle_transaction_status(input_pickle_path=AFTER_DUPLICATES_PATH)
    AFTER_ANOMALY_CODE_PATH = handle_anomalous_codes(input_pickle_path=AFTER_TRANSACTION_PATH)
