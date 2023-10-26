"""
Function to unzip data and make it available
"""
import zipfile
import os

# Set the root directory variable using a relative path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

ZIP_FILENAME = os.path.join(ROOT_DIR, 'data','data.zip')
EXTRACT_TO = os.path.join(ROOT_DIR,'data')

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
    # Return unzipped file
    unzipped_file =  os.path.join(extract_to, 'Online Retail.xlsx')
    return unzipped_file

if __name__ == "__main__":
    UNZIPPED_FILE = unzip_file(ZIP_FILENAME, EXTRACT_TO)
