import pytest
from unittest.mock import patch, MagicMock, mock_open
from scripts.utility_functions import (
    load_json,
    create_spark_session,
    db_create_spark_session,
    run_logger,
    file_pattern_check,
    move_and_rename_file_in_s3,
)

# Test load_json
@patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
@patch("os.path.dirname", return_value="/fake_dir")
@patch("os.path.abspath", return_value="/fake_dir/secrets.json")
def test_load_json(mock_abspath, mock_dirname, mock_file):
    data = load_json()
    assert data == {"key": "value"}

@patch("builtins.open", side_effect=FileNotFoundError)
def test_load_json_missing_file(mock_file):
    with pytest.raises(FileNotFoundError):
        load_json()

# Test create_spark_session
@patch("pyspark.sql.SparkSession.builder.getOrCreate")
def test_create_spark_session(mock_spark):
    mock_spark.return_value = MagicMock()
    s3_conn = {"s3_access_key": "test_key", "s3_secret_key": "test_secret"}
    spark = create_spark_session(s3_conn)
    assert spark is not None
    mock_spark.assert_called_once()

@patch("pyspark.sql.SparkSession.builder.getOrCreate")
def test_db_create_spark_session(mock_spark):
    mock_spark.return_value = MagicMock()
    spark = db_create_spark_session()
    assert spark is not None
    mock_spark.assert_called_once()

# Test run_logger
@patch("psycopg2.connect")
def test_run_logger_insert(mock_connect):
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    result = run_logger("test_dag", "test_run", "service", 1, "insert", "source_access",
                        "source_table", "file_dml", "opn", 0, "target_access", "target_table", "target_file", "success")
    assert result is True
    mock_cursor.execute.assert_called_once()

@patch("psycopg2.connect")
def test_run_logger_error(mock_connect):
    mock_connect.side_effect = Exception("Database connection error")
    result = run_logger("test_dag", "test_run", "service", 1, "insert", "source_access",
                        "source_table", "file_dml", "opn", 0, "target_access", "target_table", "target_file", "success")
    assert result is None

# Test file_pattern_check
@patch("boto3.client")
def test_file_pattern_check(mock_boto3):
    mock_s3 = mock_boto3.return_value
    mock_s3.list_objects.return_value = {
        "Contents": [{"Key": "test/file_20231118.csv"}, {"Key": "test/file_20231117.csv"}]
    }
    s3_conn = {"s3_access_key": "key", "s3_secret_key": "secret"}
    source_path = "s3://bucket/test/file_yyyymmdd.csv"

    result = file_pattern_check(source_path, s3_conn)
    assert result == "file_20231118.csv"

@patch("boto3.client")
def test_file_pattern_check_no_match(mock_boto3):
    mock_s3 = mock_boto3.return_value
    mock_s3.list_objects.return_value = {"Contents": []}
    s3_conn = {"s3_access_key": "key", "s3_secret_key": "secret"}
    source_path = "s3://bucket/test/file_yyyymmdd.csv"

    result = file_pattern_check(source_path, s3_conn)
    assert result is None

# Test move_and_rename_file_in_s3
@patch("boto3.client")
@patch("boto3.resource")
def test_move_and_rename_file_in_s3(mock_resource, mock_boto3):
    mock_s3 = mock_boto3.return_value
    mock_resource.return_value.Bucket.return_value.objects.filter.return_value = []

    s3_conn = {"s3_access_key": "key", "s3_secret_key": "secret"}
    target_s3 = "s3://bucket/test/"
    new_file_name = "new_file.csv"

    result = move_and_rename_file_in_s3(s3_conn, target_s3, new_file_name)
    assert result is True
    mock_s3.copy_object.assert_called_once()
