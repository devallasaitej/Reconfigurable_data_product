import pytest
from unittest.mock import patch, MagicMock
from scripts.s3_db import s3_file_read, db_file_write, main_s3_db


# Mock SparkSession creation
@pytest.fixture
def mock_spark():
    with patch("scripts.s3_db.create_spark_session") as mock_spark:
        yield mock_spark


# Test for s3_file_read
@patch("scripts.s3_db.boto3.client")
@patch("scripts.s3_db.file_pattern_check")
def test_s3_file_read(mock_file_pattern, mock_boto3_client, mock_spark):
    # Mock S3 client and file pattern check
    mock_file_pattern.return_value = "test_file.csv"
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    # Mocking Spark DataFrame
    mock_df = MagicMock()
    mock_spark.read.option.return_value.option.return_value.csv.return_value = mock_df
    mock_df.count.return_value = 100

    # Define inputs
    s3_conn = {"s3_access_key": "test_key", "s3_secret_key": "test_secret"}
    source_s3_path = "s3://test-bucket/test_file.csv"
    header = True
    delimiter = ","
    result = s3_file_read(
        s3_conn,
        source_s3_path,
        header,
        delimiter,
        "s3_access",
        "db_access",
        "target_table",
        "run_id",
        "dag_id",
        1,
        mock_spark,
    )

    # Assertions
    assert result is not None
    assert result[1] == "test_file.csv"
    assert result[0].count() == 100


# Test for db_file_write
@patch("scripts.s3_db.logging")
def test_db_file_write(mock_logging, mock_spark):
    # Mock DataFrame and connection details
    mock_df = MagicMock()
    mock_df.count.return_value = 200
    db_conn = {"db_host": "localhost", "db_username": "user", "db_password": "pass"}
    target_db = "MySQL"
    target_table = "test_db.test_table"

    # Call the function
    result = db_file_write(db_conn, target_db, target_table, mock_df, "append", mock_spark)

    # Assertions
    assert result == 200
    mock_logging.info.assert_any_call("Completed writing to target table.......")


# Test for main_s3_db
@patch("scripts.s3_db.s3_file_read")
@patch("scripts.s3_db.db_file_write")
@patch("scripts.s3_db.load_json")
def test_main_s3_db(mock_load_json, mock_file_write, mock_file_read, mock_spark):
    # Mock dependencies
    mock_load_json.return_value = {
        "s3_access": {"s3_access_key": "test_key", "s3_secret_key": "test_secret"},
        "db_access": {"host": "localhost", "username": "user", "password": "pass"},
    }
    mock_file_read.return_value = [MagicMock(), "test_file.csv"]
    mock_file_write.return_value = 100

    # Define input params
    input_params = {
        "source_s3_access": "s3_access",
        "target_access_db": "db_access",
        "source_s3_file_path": "s3://test-bucket/test_file.csv",
        "header_row": True,
        "file_delimiter": ",",
        "target_db": "MySQL",
        "target_table": "test_db.test_table",
        "load_type": "append",
        "run_id": "123",
        "dag_id": "test_dag",
        "task_order": 1,
    }

    # Call main function
    result = main_s3_db(**input_params)

    # Assertions
    assert result == 100
    mock_file_read.assert_called_once()
    mock_file_write.assert_called_once()
