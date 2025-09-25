from utils.to_byte_stream import to_byte_stream
import pytest
import io
import csv
import numpy as np
import pandas as pd
from faker import Faker
from unittest.mock import patch
import logging
import json

fake = Faker()


@pytest.fixture(scope="function")
def create_data():
    n = 50

    buffer = io.StringIO()
    writer = csv.writer(buffer)

    writer.writerow(["id","name","age","email","course"])

    courses = ["Math", "Physics", "History", "Biology", "Computer Science"]

    for i in range(1,n+1):
        name = fake.name()
        age = np.random.randint(18, 35)
        email = fake.email()
        course = np.random.choice(courses)
        writer.writerow([i, name, age, email, course])

    buffer.seek(0)
    df =pd.read_csv(buffer)

    pii_fields = ["name", "email"]

    for field in  pii_fields:
        if field in df.columns:
            df[field] = '***'

    yield df



@pytest.fixture(scope="function")
def create_data_json():
    n = 50


    courses = ["Math", "Physics", "History", "Biology", "Computer Science"]


    data = [
        {"id":i, 
        "name":fake.name(), 
        "age" : np.random.randint(18, 35),
        "email" : fake.email(),
        "course" : np.random.choice(courses)
        } for i in range(1,n+1)
    ]

    to_json = json.dumps(data)

    byte_body = to_json.encode("utf-8")
    byte_stream = io.BytesIO(byte_body)

    df = pd.read_json(byte_stream, orient="records", lines=True)

    pii_fields = ["name", "email"]

    for field in  pii_fields:
        if field in df.columns:
            df[field] = '***'
    yield df




@pytest.fixture(scope="function")
def create_data_parquet():
    n = 50
    courses = ["Math", "Physics", "History", "Biology", "Computer Science"]

    df_original = pd.DataFrame([
        {
            "id": i,
            "name": fake.name(),
            "age": np.random.randint(18, 35),
            "email": fake.email(),
            "course": np.random.choice(courses)
        }
        for i in range(1, n+1)
    ])

    buffer = io.BytesIO()
    df_original.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    df = pd.read_parquet(buffer, engine="pyarrow")

    pii_fields = ["name", "email"]
    for field in pii_fields:
        if field in df.columns:
            df[field] = "***"

    yield df


def test_return_byte_stream_csv(create_data):
    df = create_data
    response = to_byte_stream(df, "csv")

    assert isinstance(response, io.BytesIO)


def test_convert_df_csv(create_data):
    df = create_data
    response = to_byte_stream(df, "csv")

    df_response = pd.read_csv(response)

    assert df_response.equals(df)


@patch("utils.to_byte_stream.pd.DataFrame.to_csv")
def test_csv_error(to_csv_mock, create_data, caplog):
    df = create_data
    to_csv_mock.side_effect = Exception("error")

    caplog.set_level(logging.INFO)

    response = to_byte_stream(df, "csv")


    assert response == None
    assert 'Something went wrong ,' in caplog.text


def test_return_byte_stream_json(create_data_json):
    df = create_data_json
    response = to_byte_stream(df, "json")

    assert isinstance(response, io.BytesIO)


def test_convert_df_json(create_data_json):
    df = create_data_json
    response = to_byte_stream(df, "json")

    df_response = pd.read_json(response, orient="records", lines=True)

    assert df_response.equals(df)


@patch("utils.to_byte_stream.pd.DataFrame.to_json")
def test_json_error(to_json_mock, create_data_json, caplog):
    df = create_data_json
    to_json_mock.side_effect = Exception("error")

    caplog.set_level(logging.INFO)

    response = to_byte_stream(df, "json")


    assert response == None
    assert 'Something went wrong ,' in caplog.text



def test_return_byte_stream_parquet(create_data_parquet):
    df = create_data_parquet
    response = to_byte_stream(df, "parquet")

    assert isinstance(response, io.BytesIO)


def test_convert_df_parquet(create_data_parquet):
    df = create_data_parquet
    response = to_byte_stream(df, "parquet")

    df_response = pd.read_parquet(response, engine="pyarrow")

    assert df_response.equals(df)


@patch("utils.to_byte_stream.pd.DataFrame.to_parquet")
def test_parquet_error(to_parquet_mock, create_data_parquet, caplog):
    df = create_data_parquet
    to_parquet_mock.side_effect = Exception("error")

    caplog.set_level(logging.INFO)

    response = to_byte_stream(df, "parquet")


    assert response == None
    assert 'Something went wrong ,' in caplog.text

