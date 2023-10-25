"""
Functions to ingest and process data
"""
import zipfile

from .download_data import ingest_data


def unzip_file():
    """
    Function to unzip the downloaded data
    """
    zip_filename ='data/data.zip'
    extract_to = 'data/'
    try:
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"File {zip_filename} successfully unzipped to {extract_to}")
    except zipfile.BadZipFile:
        print(f"Failed to unzip {zip_filename}")


if __name__ == "__main__":
    ZIPFILE_PATH = ingest_data("https://archive.ics.uci.edu/static/public/352/online+retail.zip")
    unzip_file()
