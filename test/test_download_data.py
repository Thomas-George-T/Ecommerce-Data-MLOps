"""
  Tests for downloda_data.py
"""
import os
import requests
import requests_mock
from src import download_data

DEFAULT_FILE_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"

def test_ingest_data(mocker):
    """
      Tests for checking print call
    """
    # arrange:
    # mocked dependencies
    mock_print = mocker.MagicMock(name='print')
    mocker.patch('src.download_data.print', new=mock_print)
    # act: invoking the tested code
    download_data.ingest_data(DEFAULT_FILE_URL)
    # assert: todo
    assert 2 == mock_print.call_count
def test_ingest_data_successful_download():
    """
      Test for checking successful download of the file
    """
    # Create a session and attach the requests_mock to it
    with requests.Session() as session:
        adapter = requests_mock.Adapter()
        # session.mount('http://', adapter)
        session.mount('https://', adapter)

        # Set the root directory variable using a relative path
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

        # Path to store the zipfile
        zipfile_path=os.path.join(root_dir, 'data','data.zip')

        # Define the mock response
        adapter.register_uri('GET', DEFAULT_FILE_URL, text=zipfile_path)

        # Call your function that makes the HTTP requests
        result = download_data.ingest_data(DEFAULT_FILE_URL)  # Replace with your actual function

        # Perform assertions
        assert result == zipfile_path
