"""
Modularized Data pipeline to form DAGs in the future
"""
from .download_data import ingest_data
from .unzip_data import unzip_file


if __name__ == "__main__":
    ZIPFILE_PATH = ingest_data("https://archive.ics.uci.edu/static/public/352/online+retail.zip")
    UNZIPPED_FILE = unzip_file(ZIPFILE_PATH, 'data/')
