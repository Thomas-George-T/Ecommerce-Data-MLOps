"""
Function to test the unzip_data functions
"""
from src import unzip_data

# Define constants or variables for testing
ZIP_FILENAME = 'data/data.zip'
EXTRACT_TO = 'data/'
BAD_ZIP_FILENAME = 'data/bad.zip'

def test_unzip_file_successful(tmp_path):
    """
      Test for successful unzipping
    """
    # Create a temporary directory for testing
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    # Copy the sample zip file to the temporary directory
    zip_file = tmp_path / "data.zip"
    with open(ZIP_FILENAME, "rb") as src, open(zip_file, "wb") as dst:
        dst.write(src.read())

    # Call the function
    result = unzip_data.unzip_file(zip_file, test_dir)

    print(result)

    # Check if the function returned the expected extract_to path
    assert result == test_dir


def test_unzip_file_bad_zip(tmp_path, capsys):
    """
      Test for handling a bad zip file
    """
    # Create a temporary directory for testing
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    # Create a bad zip file in the temporary directory
    with open(BAD_ZIP_FILENAME, "wb") as file:
        file.write(b"This is not a valid zip file")

    # Call the function with the bad zip file
    result = unzip_data.unzip_file(BAD_ZIP_FILENAME, test_dir)

    # Check if the function returned the extract_to path
    assert result == test_dir

    # Check if the function printed the appropriate error message
    captured = capsys.readouterr()
    assert "Failed to unzip" in captured.out
