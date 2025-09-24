from src.obfuscator_main import obfuscator_main
import pytest
from unittest.mock import MagicMock, patch
import logging


@pytest.fixture(scope="function")
def create_data():
    json_string = '''{
        "file_to_obfuscator": "s3://test_bucket/my_file.csv",
        "pii_fields": ["name", "id","email"]
    }'''

    yield json_string

class TestObfuscator():
    def test_hadles_wrong_input(self):
        #missing pii_field
        json_string = '''{
        "file_to_obfuscator": "s3://test_bucket/my_file.csv"
    }'''
        
        with pytest.raises(Exception) as e:
            obfuscator_main(json_string)

        assert type(e.value) == ValueError
        assert "Missing 'pii_fields' in input JSON" in str(e.value)


        #missing file_to_obfuscator
        json_string = '''{
        "pii_fields": ["name", "id","email"]
    }'''
        with pytest.raises(Exception) as e:
            obfuscator_main(json_string)

        assert type(e.value) == ValueError
        assert "Missing 'file_to_obfuscator' in input JSON" in str(e.value)

    



        