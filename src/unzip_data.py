"""
Function to unzip data and make it available
"""
import zipfile

ZIP_FILENAME ='data/data.zip'
EXTRACT_TO = 'data/'

def unzip_file(zip_filename=ZIP_FILENAME, extract_to=EXTRACT_TO):
    """
    Function to unzip the downloaded data
    Args:
      zip_filename: zipfile path, a default is used if not specified
      extract_to: Path where the unzipped and extracted data is available
    Returns:
      extract_to: filepath where the data is available
    """
    try:
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"File {zip_filename} successfully unzipped to {extract_to}")
    except zipfile.BadZipFile:
        print(f"Failed to unzip {zip_filename}")
    return extract_to

if __name__ == "__main__":
    UNZIPPED_FILE = unzip_file(ZIP_FILENAME, EXTRACT_TO)
