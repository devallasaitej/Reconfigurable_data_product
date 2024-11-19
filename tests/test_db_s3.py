import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from scripts.db_s3 import table_data_read, write_data_s3, main_db_s3


@pytest.fixture
def mock_spark():
    """Fixture to provide a mocked Spark session."""
    spark = SparkSession.builder.master("local[1]").appName("UnitTest").getOrCreate()
    data = [("Alice", 25), ("Bob", 30)]
    columns = ["Name", "Age"]
    mock_df = spark.createDataFrame(data, columns)
    yield spark, mock_df
    spark.stop()


@pytest.fixture
def mock_db_conn():
    """Fixture to provide mocked DB connection details."""
    return {
        "db_host": "localhost",
        "db_username": "test_user",
        "db_password": "test_password",
    }


@pytest.fixture
def mock_s3_conn():
    """Fixture to provide mocked S3 connection details."""
    return {
        "s3_access_key": "mock_access_key",
        "s3_secret_key": "mock_secret_key",
    }


@patch("db_s3.run_logger")
def test_table_data_read(mock_run_logger, mock_spark, mock_db_conn):
    """Test table_data_read for successful data read."""
    spark, mock_df = mock_spark
    source_table = "test_db.test_table"
    DML = ""
    dag_id, run_id, task_order = "dag1", "run1", 1
    db_access, s3_access, target_s3 = "db_access", "s3_access", "s3://mock-bucket/"
    source_db = "MySQL"

    mock_spark_read = MagicMock()
    mock_spark_read.count.return_value = 2

    with patch.object(spark.read, "format", return_value=mock_spark_read):
        result = table_data_read(mock_s3_conn, mock_db_conn, source_db, source_table, DML, dag_id, run_id, task_order, db_access, s3_access, target_s3, spark)
        assert result is not None
        assert result[1] == 2


@patch("db_s3.run_logger")
@patch("db_s3.boto3.client")
def test_write_data_s3(mock_boto3_client, mock_run_logger, mock_s3_conn, mock_spark):
    """Test write_data_s3 for successful data write."""
    spark, mock_df = mock_spark
    target_s3 = "s3://mock-bucket/"
    target_file_name = "output.csv"
    delimiter = ","
    dag_id, run_id, task_order = "dag1", "run1", 1
    db_access, source_table, s3_access = "db_access", "test_table", "s3_access"
    DML = "SELECT * FROM test_table"

    mock_s3_client_instance = MagicMock()
    mock_boto3_client.return_value = mock_s3_client_instance
    mock_move_and_rename = MagicMock(return_value=True)

    with patch("db_s3.move_and_rename_file_in_s3", mock_move_and_rename):
        result = write_data_s3(mock_s3_conn, target_s3, target_file_name, delimiter, mock_df, dag_id, run_id, task_order, db_access, source_table, s3_access, DML, spark)
        assert result is not None
        assert result[0] == 2  # Record count
        assert result[1] == target_file_name


@patch("db_s3.run_logger")
@patch("db_s3.load_json")
@patch("db_s3.create_spark_session")
def test_main_db_s3(mock_create_spark_session, mock_load_json, mock_run_logger, mock_s3_conn, mock_db_conn, mock_spark):
    """Test main_db_s3 function."""
    mock_s3_credentials = {
        "target_s3_access": {
            "s3_access_key": "mock_access_key",
            "s3_secret_key": "mock_secret_key",
        },
        "source_access_db": {
            "host": "localhost",
            "username": "test_user",
            "password": "test_password",
        },
    }
    mock_load_json.return_value = mock_s3_credentials
    mock_create_spark_session.return_value = mock_spark[0]

    input_params = {
        "target_s3_access": "target_s3_access",
        "source_access_db": "source_access_db",
        "source_db": "MySQL",
        "source_table": "test_db.test_table",
        "dml": "",
        "target_s3_file_path": "s3://mock-bucket/output.csv",
        "run_id": "run1",
        "dag_id": "dag1",
        "task_order": 1,
        "target_file_delimiter": ",",
    }

    with patch("db_s3.table_data_read", return_value=(mock_spark[1], 2)) as mock_read, \
            patch("db_s3.write_data_s3", return_value=[2, "output.csv"]) as mock_write:
        result = main_db_s3(**input_params)
        assert result is not None
        assert result[0] == 2  # Record count
        assert result[1] == "output.csv"
        mock_read.assert_called_once()
        mock_write.assert_called_once()
