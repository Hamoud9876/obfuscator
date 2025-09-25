import logging
from unittest.mock import MagicMock, patch
from utils.extract_file_format import extract_file_format
import pytest
import json


@pytest.fixture(scope="function")
def create_data():
    json_string = """{
        "file_to_obfuscator": "s3://test_bucket/my_file.csv",
        "pii_fields": ["name", "id","email"]
    }"""

    yield json_string


@patch("utils.extract_file_format.re.compile")
def test_format_not_found(pattern_mock, caplog, create_data):
    j_string = create_data
    jstring = json.loads(j_string)

    caplog.set_level(logging.INFO)

    my_mock = MagicMock()
    my_mock.search.return_value = None
    pattern_mock.return_value = my_mock

    response = extract_file_format(jstring["file_to_obfuscator"])

    assert response == None
    assert "no file format was found" in caplog.text


def test_return_correct_format(create_data):
    # csv
    j_string = create_data
    jstring = json.loads(j_string)

    response = extract_file_format(jstring["file_to_obfuscator"])

    assert response == "csv"

    # json
    txt = '{"file_to_obfuscator": "s3://test_bucket/my_file.json"}'
    jstring = json.loads(txt)

    response = extract_file_format(jstring["file_to_obfuscator"])

    assert response == "json"
