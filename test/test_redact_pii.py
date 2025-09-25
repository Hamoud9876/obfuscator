import pandas as pd
import numpy as np
from faker import Faker
import csv
import io
import pytest
from utils.redact_pii import redact_pii


fake = Faker()


@pytest.fixture(scope='function')
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

    yield df, pii_fields


def test_mutability(create_data):
    df, pii_fields = create_data

    reponse = redact_pii(df,pii_fields)

    assert reponse is not df


def test_return_df(create_data):
    df, pii_fields = create_data

    reponse = redact_pii(df,pii_fields)

    assert isinstance(reponse, pd.DataFrame)

def test_redact_pii(create_data):
    df, pii_fields = create_data

    reponse = redact_pii(df,pii_fields)

    for field in pii_fields:
        assert reponse[field].loc[np.random.randint(0,9)] == '***'
