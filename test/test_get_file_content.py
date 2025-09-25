from utils.get_file_content import get_file_content
import pytest
from unittest.mock import MagicMock, patch
import logging
from botocore.response import StreamingBody
import io


@pytest.fixture(scope="function")
def create_data():
    file_location = "s3://test_bucket/some/words/my_file.csv"

    yield file_location


def test_handles_wrong_input(caplog):
    caplog.set_level(logging.INFO)

    response = get_file_content(1)

    assert "wrong input type for 'flocation'" in caplog.text
    assert response == None


@patch("utils.get_file_content.boto3.client")
def test_return_boto_stream(boto_mock, create_data):
    f_location = create_data

    my_mock = MagicMock()
    body = io.BytesIO(b"some content")
    mock_body = StreamingBody(body, len(body.getvalue()))
    my_mock.get_object.return_value = {"Body": mock_body}
    boto_mock.return_value = my_mock

    response = get_file_content(f_location)

    assert isinstance(response, StreamingBody)


@patch("utils.get_file_content.boto3.client")
def test_raises_error(boto_mock, caplog, create_data):
    f_location = create_data

    caplog.set_level(logging.ERROR)

    my_mock = MagicMock()
    my_mock.get_object.side_effect = Exception("some error")
    boto_mock.return_value = my_mock

    response = get_file_content(f_location)

    assert response == None
    assert "Failed to retrieve S3 object" in caplog.text


@patch("utils.get_file_content.re.compile")
def test_no_match_found(pattern_mock, caplog, create_data):
    f_location = create_data

    caplog.set_level(logging.INFO)

    my_mock = MagicMock()
    my_mock.match.return_value = None
    pattern_mock.return_value = my_mock

    response = get_file_content(f_location)

    assert response == None
    assert (
        "Invalid S3 URL: s3://test_bucket/some/words/my_file.csv"
        in caplog.text
    )
