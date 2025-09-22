from utils.get_file_content import get_file_content
import pytest
from unittest.mock import MagicMock, patch
import logging


@pytest.fixture(scope="function")
def create_data():
    file_location = "s3://test_bucket/some/words/my_file.csv"


    yield file_location

    

def test_handles_wrong_input(caplog):
    caplog.set_level(logging.INFO)
    
    response  = get_file_content(1)

    assert "wrong input type for 'flocation'" in caplog.text
    assert response == -1

@patch("utils.get_file_content.boto3.client")
def test_return_dict(boto_mock, create_data):
    f_location = create_data

    my_mock = MagicMock()
    my_mock.get_object.return_value = {"hi":1}
    boto_mock.return_value = my_mock

    response = get_file_content(f_location)

    assert isinstance(response,dict)


@patch("utils.get_file_content.boto3.client")
def test_raises_error(boto_mock, caplog, create_data):
    f_location = create_data

    caplog.set_level(logging.ERROR)

    my_mock = MagicMock()
    my_mock.get_object.side_effect = Exception(
            "some error"
        )
    boto_mock.return_value = my_mock


    response = get_file_content(f_location)

    assert response == -1
    assert "Something went wrong,Exception: some error" in caplog.text


@patch("utils.get_file_content.re.compile")
def test_no_match_found(pattern_mock, caplog,create_data):
    f_location = create_data

    caplog.set_level(logging.INFO)
    
    my_mock = MagicMock()
    my_mock.match.return_value = None
    pattern_mock.return_value = my_mock


    response = get_file_content(f_location)


    assert response == -1
    assert "Invalid S3 URL: s3://test_bucket/some/words/my_file.csv" in caplog.text
