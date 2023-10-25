"""
  Tests for downloda_data.py
"""
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
    assert 1 == mock_print.call_count


def test_ingest_data_successful_download(requests_mock):
    """
      Test for checking successful download of the file
    """
    # Mock the requests.get() method to return a successful response
    requests_mock.get(DEFAULT_FILE_URL, text="Test file content", status_code=200)

    # Call the function and check if it returns the correct zipfile path
    result = download_data.ingest_data(DEFAULT_FILE_URL)
    assert result == "data/data.zip"
