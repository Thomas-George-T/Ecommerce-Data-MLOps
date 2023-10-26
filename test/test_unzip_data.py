"""
Function to test the unzip_data functions
"""
import os
from src import unzip_data

# Define constants or variables for testing
# Set the root directory variable using a relative path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

ZIP_FILENAME = os.path.join(ROOT_DIR, 'data','data.zip')
EXTRACT_TO = os.path.join(ROOT_DIR,'data')
BAD_ZIP_FILENAME = os.path.join(ROOT_DIR, 'data', 'bad.zip')

# Test for successful unzipping
def test_unzip_file_successful():
    """
      Test for successful unzipping
    """
    # Call the function to unzip a valid file
    unzipped_file = unzip_data.unzip_file(ZIP_FILENAME, EXTRACT_TO)

    # Check if the function returned the expected unzipped file path
    assert unzipped_file == os.path.join(EXTRACT_TO, 'Online Retail.xlsx')

    # Check if the unzipped file exists
    assert os.path.isfile(unzipped_file)

# Test for handling a bad zip file
def test_unzip_file_bad_zip(tmp_path, capsys):
    """
      Test for handling a bad zip file
    """
    # Create a bad zip file in the temporary directory
    with open(BAD_ZIP_FILENAME, "wb") as file:
        file.write(b"This is not a valid zip file")

    # Create a temporary directory for testing
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    # Call the function to unzip a bad zip file
    unzip_data.unzip_file(BAD_ZIP_FILENAME, test_dir)

    # Check if the function printed the appropriate error message
    captured = capsys.readouterr()
    assert "Failed to unzip" in captured.out
