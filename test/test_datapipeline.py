import requests
import pytest
from src import datapipeline

def test_ingest_data(mocker):
    # arrange:
    # mocked dependencies
    mock_print = mocker.MagicMock(name='print')
    mocker.patch('src.datapipeline.print', new=mock_print) 
    
    # act: invoking the tested code
    datapipeline.ingest_data()
    
    # assert:
    assert 1 == mock_print.call_count


def test_unzip_file(mocker):
    # arrange:
    # mocked dependencies
    mock_ZipFile = mocker.MagicMock(name='ZipFile')
    mocker.patch('src.datapipeline.zipfile.ZipFile', new=mock_ZipFile)
    mock_print = mocker.MagicMock(name='print')
    mocker.patch('src.datapipeline.print', new=mock_print)
    mock_Exception = mocker.MagicMock(name='Exception')
    mocker.patch('src.datapipeline.Exception', new=mock_Exception)
    
    # act: invoking the tested code
    datapipeline.unzip_file()
    
    # assert:
    mock_Exception.assert_not_called()