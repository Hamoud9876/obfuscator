from utils.file_to_df import file_to_df
import io
import csv
from botocore.response import StreamingBody
import pandas as pd
import json
from unittest.mock import MagicMock, patch
import logging


class TestUnit:
    def test_reads_csv(self):
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id","name","age"])
        writer.writerow([1,"James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))


        response = file_to_df(streaming_body, "csv")

        assert isinstance(response, pd.DataFrame)

    def test_csv_df_correct_structure(self):
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id","name","age"])
        writer.writerow([1,"James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))


        response = file_to_df(streaming_body, "csv")

        assert "id" in response.columns
        assert "name" in  response.columns
        assert "age" in response.columns


    def test_csv_df_correct_data(self):
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id","name","age"])
        writer.writerow([1,"James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))


        response = file_to_df(streaming_body, "csv")

        assert response["id"].loc[0] == 1
        assert response["name"].loc[0] == "James"
        assert response["age"].loc[0] == 25
        assert response["id"].loc[1] == 2
        assert response["name"].loc[1] == "Hamoud"
        assert response["age"].loc[1] == 30


    @patch("utils.file_to_df.pd")
    def test_csv_raises_error(self,pd_mock, caplog):
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["id","name","age"])
        writer.writerow([1,"James", 25])
        writer.writerow([2, "Hamoud", 30])

        body_bytes = buffer.getvalue().encode("utf-8")

        byte_stream = io.BytesIO(body_bytes)

        pd_mock.read_csv.side_effect = Exception("error")
        caplog.set_level(logging.ERROR)

        streaming_body = StreamingBody(byte_stream, len(body_bytes))

        response = file_to_df(streaming_body, "csv")

        assert response == None
        assert "Something went wrong " in caplog.text


    def test_reads_json(self):
        data = [{"id": 1, "name": "James", "age": 25},
                {"id": 2, "name": "Hamoud", "age": 30}
                ]
        
        to_json = json.dumps(data)

        byte_body = to_json.encode("utf-8")
        byte_stream = io.BytesIO(byte_body)

        streaming_body = StreamingBody(byte_stream, len(byte_body))

        response = file_to_df(streaming_body, "json")

        assert isinstance(response, pd.DataFrame)


    def test_json_df_correct_structure(self):
        data = [{"id": 1, "name": "James", "age": 25},
                {"id": 2, "name": "Hamoud", "age": 30}
                ]
        
        to_json = json.dumps(data)

        byte_body = to_json.encode("utf-8")
        byte_stream = io.BytesIO(byte_body)

        streaming_body = StreamingBody(byte_stream, len(byte_body))

        response = file_to_df(streaming_body, "json")

        assert "id" in response.columns
        assert "name" in  response.columns
        assert "age" in response.columns

    def test_json_df_correct_data(self):
        data = [{"id": 1, "name": "James", "age": 25},
                {"id": 2, "name": "Hamoud", "age": 30}
                ]
        
        to_json = json.dumps(data)

        byte_body = to_json.encode("utf-8")
        byte_stream = io.BytesIO(byte_body)

        streaming_body = StreamingBody(byte_stream, len(byte_body))

        response = file_to_df(streaming_body, "json")

        assert response["id"].loc[0] == 1
        assert response["name"].loc[0] == "James"
        assert response["age"].loc[0] == 25
        assert response["id"].loc[1] == 2
        assert response["name"].loc[1] == "Hamoud"
        assert response["age"].loc[1] == 30

    
    @patch("utils.file_to_df.pd")
    def test_json_raises_error(self,pd_mock, caplog):
        data = [{"id": 1, "name": "James", "age": 25},
                {"id": 2, "name": "Hamoud", "age": 30}
                ]
        
        to_json = json.dumps(data)

        byte_body = to_json.encode("utf-8")
        byte_stream = io.BytesIO(byte_body)

        streaming_body = StreamingBody(byte_stream, len(byte_body))

        response = file_to_df(streaming_body, "json")

        pd_mock.read_json.side_effect = Exception("error")
        caplog.set_level(logging.ERROR)

        response = file_to_df(streaming_body, "json")

        assert response == None
        assert "Something went wrong " in caplog.text


    def test_reads_parquet(self):
        df = pd.DataFrame([
        {"id": 1, "name": "James", "age": 25},
        {"id": 2, "name": "Hamoud", "age": 30}
    ])
        
        buffer = io.BytesIO()

        df.to_parquet(buffer, engine='pyarrow', index=False)

        buffer.seek(0)
        streaming_body = StreamingBody(buffer, len(buffer.getvalue()))

        response = file_to_df(streaming_body, "parquet")

        assert isinstance(response, pd.DataFrame)

    def test_parquet_df_correct_structure(self):
        df = pd.DataFrame([
        {"id": 1, "name": "James", "age": 25},
        {"id": 2, "name": "Hamoud", "age": 30}
    ])
        
        buffer = io.BytesIO()

        df.to_parquet(buffer, engine='pyarrow', index=False)

        buffer.seek(0)
        streaming_body = StreamingBody(buffer, len(buffer.getvalue()))

        response = file_to_df(streaming_body, "parquet")

        assert "id" in response.columns
        assert "name" in  response.columns
        assert "age" in response.columns


    def test_parquet_df_correct_data(self):
        df = pd.DataFrame([
        {"id": 1, "name": "James", "age": 25},
        {"id": 2, "name": "Hamoud", "age": 30}
    ])
        
        buffer = io.BytesIO()

        df.to_parquet(buffer, engine='pyarrow', index=False)

        buffer.seek(0)
        streaming_body = StreamingBody(buffer, len(buffer.getvalue()))

        response = file_to_df(streaming_body, "parquet")

        assert response["id"].loc[0] == 1
        assert response["name"].loc[0] == "James"
        assert response["age"].loc[0] == 25
        assert response["id"].loc[1] == 2
        assert response["name"].loc[1] == "Hamoud"
        assert response["age"].loc[1] == 30


    @patch("utils.file_to_df.pd")
    def test_parquet_raises_error(self,pd_mock, caplog):
        df = pd.DataFrame([
        {"id": 1, "name": "James", "age": 25},
        {"id": 2, "name": "Hamoud", "age": 30}
    ])
        
        buffer = io.BytesIO()

        df.to_parquet(buffer, engine='pyarrow', index=False)

        buffer.seek(0)
        streaming_body = StreamingBody(buffer, len(buffer.getvalue()))

        response = file_to_df(streaming_body, "parquet")

        pd_mock.read_parquet.side_effect = Exception("error")
        caplog.set_level(logging.ERROR)

        response = file_to_df(streaming_body, "parquet")

        assert response == None
        assert "Something went wrong " in caplog.text