# GDPR project

data pipeline that obfuscates Personally Identifiable Information (PII) in files and returns a byte stream in the same format. Supports CSV, JSON, and Parquet formats.

---

## Project Overview

`GDPR project` allows you to safely handle sensitive data by redacting specified PII fields. The pipeline can fetch files from S3, process them in memory, redact sensitive columns, and produce a byte stream that preserves the original file format.  

**Key goals:**
- Support multiple file formats (CSV, JSON, Parquet)
- Redact PII fields without modifying the original data
- Enable testing and development with mocked AWS resources
- Efficient handling of large datasets
---

## Features

- Reads files from S3 or in-memory byte streams
- Supports CSV, JSON, and Parquet
- Redacts user-specified fields
- Returns a ready-to-use `BytesIO` stream
- Fully tested with unit and integration tests
- Mocked AWS environment for safe testing

---

## Getting Started

### Requirements

Python 3.11+ is recommended.  

Key dependencies:
- `pandas` – data wrangling
- `boto3` – AWS S3 integration
- `moto` – AWS mocking for tests
- `faker` – generate realistic fake test data
- `pytest` – testing framework

---

### Setup

1. Install project requirements:

```bash
make requirements
make dev-setup
make run-checks
```

##  Auther
Hamoud Alzafiry
[LinkedIn](https://www.linkedin.com/in/hamoud5)  
[GitHub](https://github.com/Hamoud9876)

