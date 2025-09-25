from obfuscate_main import obfuscator_main
import pytest
from unittest.mock import patch
import logging
from botocore.response import StreamingBody
import io
import csv
import pandas as pd
import boto3
from moto import mock_aws
import os
from faker import Faker
import numpy as np
import time


fake = Faker()


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def create_data():
    json_string = """{
        "file_to_obfuscate": "s3://test_bucket/my_file.csv",
        "pii_fields": ["name", "id","email"]
    }"""

    yield json_string


@pytest.fixture(scope="function")
def s3_data():
    with mock_aws():
        s3 = boto3.client("s3")
        bucket_name = "test"
        key_name = "my/buck/test.csv"
        s3.create_bucket(
            Bucket="test",
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-2",
            },
        )

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id", "name", "age", "email"])
        writer.writerow([1, "James", 25, "hello@122"])
        writer.writerow([2, "Hamoud", 30, "hioo@as"])
        content = buffer.getvalue()

        s3.put_object(
            Body=content,
            Bucket=bucket_name,
            Key=key_name,
        )
        f"s3://{bucket_name}/{key_name}"

        json_string = f"""{{
        "file_to_obfuscate": "s3://{bucket_name}/{key_name}",
        "pii_fields": ["name", "id","email"]
    }}"""

        yield json_string


@pytest.fixture(scope="function")
def create_1mp_data():
    with mock_aws():
        s3 = boto3.client("s3")
        bucket_name = "test"
        key_name = "my/buck/test.csv"
        s3.create_bucket(
            Bucket="test",
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-2",
            },
        )

        n_rows = 5500

        df = pd.DataFrame(
            {
                "id": range(1, n_rows + 1),
                "name": [fake.name() for _ in range(n_rows)],
                "age": np.random.randint(18, 65, size=n_rows),
                "email": [fake.email() for _ in range(n_rows)],
                "course": [fake.job() for _ in range(n_rows)],
            }
        )

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Body=buffer,
            Bucket=bucket_name,
            Key=key_name,
        )
        f"s3://{bucket_name}/{key_name}"

        json_string = f"""{{
        "file_to_obfuscate": "s3://{bucket_name}/{key_name}",
        "pii_fields": ["name", "id","email"]
    }}"""

        yield json_string


class TestObfuscateUnitTest:
    # 1
    def test_hadles_wrong_input(self):
        # missing pii_field
        json_string = """{
        "file_to_obfuscate": "s3://test_bucket/my_file.csv"
    }"""

        with pytest.raises(Exception) as e:
            obfuscator_main(json_string)

        assert type(e.value) == ValueError
        assert "Missing 'pii_fields' in input JSON" in str(e.value)

        # missing file_to_obfuscate
        json_string = """{
        "pii_fields": ["name", "id","email"]
    }"""
        with pytest.raises(Exception) as e:
            obfuscator_main(json_string)

        assert type(e.value) == ValueError
        assert "Missing 'file_to_obfuscate' in input JSON" in str(e.value)

    # 2
    @patch("src.obfuscator_main.to_byte_stream")
    @patch("src.obfuscator_main.redact_pii")
    @patch("src.obfuscator_main.file_to_df")
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_exctract_fomrat(
        self,
        mock_ext_file,
        mock_file_content,
        mock_file_to_df,
        mock_redact_pii,
        mock_byte_stream,
        create_data,
    ):
        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = ""
        mock_file_to_df.return_value = ""
        mock_redact_pii.return_value = ""
        mock_byte_stream.return_value = ""

        j_string = create_data

        obfuscator_main(j_string)

        mock_ext_file.assert_called_once()
        mock_ext_file.assert_called_with("s3://test_bucket/my_file.csv")

    # 3
    @patch("src.obfuscator_main.extract_file_format")
    def test_exctract_format_error(self, mock_ext_file, caplog, create_data):
        mock_ext_file.return_value = None

        j_string = create_data

        caplog.set_level(logging.INFO)

        response = obfuscator_main(j_string)

        assert type(response) == dict
        assert response["status"] == 400
        assert "Failed to retreive file format" in caplog.text

    # 4
    @patch("src.obfuscator_main.to_byte_stream")
    @patch("src.obfuscator_main.redact_pii")
    @patch("src.obfuscator_main.file_to_df")
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_get_content(
        self,
        mock_ext_file,
        mock_file_content,
        mock_file_to_df,
        mock_redact_pii,
        mock_byte_stream,
        create_data,
    ):

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id", "name", "age"])
        writer.writerow([1, "James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))

        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = streaming_body
        mock_file_to_df.return_value = ""
        mock_redact_pii.return_value = ""
        mock_byte_stream.return_value = ""

        j_string = create_data

        obfuscator_main(j_string)

        mock_file_content.assert_called_once()
        mock_file_content.assert_called_with("s3://test_bucket/my_file.csv")

    # 5
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_get_content_error(
        self, mock_ext_file, mock_file_content, caplog, create_data
    ):
        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = None

        j_string = create_data

        caplog.set_level(logging.INFO)

        response = obfuscator_main(j_string)

        assert type(response) == dict
        assert response["status"] == 400
        assert "Failed to retreive file content" in caplog.text

    # 6
    @patch("src.obfuscator_main.to_byte_stream")
    @patch("src.obfuscator_main.redact_pii")
    @patch("src.obfuscator_main.file_to_df")
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_file_to_df(
        self,
        mock_ext_file,
        mock_file_content,
        mock_file_to_df,
        mock_redact_pii,
        mock_byte_stream,
        create_data,
    ):

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id", "name", "age"])
        writer.writerow([1, "James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))

        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = streaming_body
        df = pd.read_csv(streaming_body)
        mock_file_to_df.return_value = df
        mock_redact_pii.return_value = ""
        mock_byte_stream.return_value = ""

        j_string = create_data

        obfuscator_main(j_string)

        mock_file_to_df.assert_called_once()
        mock_file_to_df.assert_called_with(streaming_body, "csv")

    # 7
    @patch("src.obfuscator_main.file_to_df")
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_file_to_df_error(
        self,
        mock_ext_file,
        mock_file_content,
        mock_file_to_df,
        caplog,
        create_data,
    ):
        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = ""
        mock_file_to_df.return_value = None

        j_string = create_data

        caplog.set_level(logging.INFO)

        response = obfuscator_main(j_string)

        assert type(response) == dict
        assert response["status"] == 400
        assert "Failed to convert file content to df" in caplog.text

    # 8
    @patch("src.obfuscator_main.to_byte_stream")
    @patch("src.obfuscator_main.redact_pii")
    @patch("src.obfuscator_main.file_to_df")
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_redact_pii(
        self,
        mock_ext_file,
        mock_file_content,
        mock_file_to_df,
        mock_redact_pii,
        mock_byte_stream,
        create_data,
    ):

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id", "name", "age"])
        writer.writerow([1, "James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))

        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = streaming_body
        df = pd.read_csv(streaming_body)
        mock_file_to_df.return_value = df
        mock_redact_pii.return_value = pd.DataFrame()
        mock_byte_stream.return_value = ""

        j_string = create_data

        obfuscator_main(j_string)

        mock_redact_pii.assert_called_once()
        mock_redact_pii.assert_called_with(df, ["name", "id", "email"])

    @patch("src.obfuscator_main.to_byte_stream")
    @patch("src.obfuscator_main.redact_pii")
    @patch("src.obfuscator_main.file_to_df")
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_byte_stream(
        self,
        mock_ext_file,
        mock_file_content,
        mock_file_to_df,
        mock_redact_pii,
        mock_byte_stream,
        create_data,
    ):

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id", "name", "age"])
        writer.writerow([1, "James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))

        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = streaming_body
        df = pd.read_csv(streaming_body)
        mock_file_to_df.return_value = df
        df2 = pd.DataFrame()
        mock_redact_pii.return_value = df2
        mock_byte_stream.return_value = io.BytesIO()

        j_string = create_data

        obfuscator_main(j_string)

        mock_byte_stream.assert_called_once()
        mock_byte_stream.assert_called_with(df2, "csv")

    # 10
    @patch("src.obfuscator_main.to_byte_stream")
    @patch("src.obfuscator_main.redact_pii")
    @patch("src.obfuscator_main.file_to_df")
    @patch("src.obfuscator_main.get_file_content")
    @patch("src.obfuscator_main.extract_file_format")
    def test_byte_stream_error(
        self,
        mock_ext_file,
        mock_file_content,
        mock_file_to_df,
        mock_redact_pii,
        mock_byte_stream,
        caplog,
        create_data,
    ):
        mock_ext_file.return_value = "csv"
        mock_file_content.return_value = ""
        mock_file_to_df.return_value = ""
        mock_redact_pii.return_value = ""
        mock_byte_stream.return_value = None
        j_string = create_data

        caplog.set_level(logging.INFO)

        response = obfuscator_main(j_string)

        assert type(response) == dict
        assert response["status"] == 400
        assert "Failed to convert file to byte stream" in caplog.text


class TestObfuscateIntegrationTest:
    @mock_aws
    def test_full_cycle(self, s3_data):
        json_string = s3_data

        response = obfuscator_main(json_string)

        df = pd.read_csv(response)
        assert df["name"].loc[0] == "***"
        assert df["email"].loc[0] == "***"
        assert df["id"].loc[0] == "***"
        assert len(df["name"]) == 2

    @mock_aws
    def test_1mp_file(self, create_1mp_data):
        jstring = create_1mp_data

        start_time = time.time()
        obfuscator_main(jstring)

        end_time = time.time()

        total_time = round(end_time - start_time, 3)

        print(total_time)
        assert total_time < 60
