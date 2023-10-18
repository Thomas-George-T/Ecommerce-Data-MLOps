"""
Tests for datapipeline functions
"""
from src import datapipeline

def test_ingest_data(mocker):
    """
    Test for ingest_data()
    """

    # arrange:
    # mocked dependencies

    mock_print = mocker.MagicMock(name='print')
    mocker.patch('src.datapipeline.print', new=mock_print)

    # act: invoking the tested code
    datapipeline.ingest_data()

    # assert:
    assert 1 == mock_print.call_count


def test_unzip_file(mocker):
    """
    Tests for unzip()
    """

    # arrange:
    # mocked dependencies

    mock_zipfile = mocker.MagicMock(name='ZipFile')
    mocker.patch('src.datapipeline.zipfile.ZipFile', new=mock_zipfile)

    mock_print = mocker.MagicMock(name='print')
    mocker.patch('src.datapipeline.print', new=mock_print)

    mock_exception = mocker.MagicMock(name='Exception')
    mocker.patch('src.datapipeline.Exception', new=mock_exception)

    # act: invoking the tested code
    datapipeline.unzip_file()

    # assert:
    mock_exception.assert_not_called()
