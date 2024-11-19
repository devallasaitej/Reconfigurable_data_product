import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from scripts.db_db import table_data_read_write, main_db_db


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
def mock_sdb_conn():
    """Fixture to provide mocked source DB connection details."""
    return {"db_host": "localhost", "db_username": "user", "db_password": "password"}


@pytest.fixture
def mock_tdb_conn():
    """Fixture to provide mocked target DB connection details."""
    return {"db_host": "localhost", "db_username": "user", "db_password": "password"}


@patch("db_db.run_logger")
def test_table_data_read_write_success(mock_run_logger, mock_spark, mock_sdb_conn, mock_tdb_conn):
    """Test successful execution of table_data_read_write."""
    spark, mock_df = mock_spark
    source_table = "test_db.source_table"
    target_table = "test_db.target_table"
    DML = ""
    load_type = "overwrite"
    run_id = "run1"
    dag_id = "dag1"
    task_order = 1
    sdb_access = "sdb_access"
    tdb_access = "tdb_access"
    source_db = "MySQL"
    target_db = "MySQL"

    mock_spark_read = MagicMock()
    mock_spark_read.count.return_value = 2

    with patch.object(spark.read, "format", return_value=mock_spark_read), \
            patch.object(mock_df.write, "format") as mock_write:
        result = table_data_read_write(mock_sdb_conn, mock_tdb_conn, source_db, target_db, source_table, DML, target_table,
                                       load_type, run_id, dag_id, task_order, sdb_access, tdb_access, spark)
        assert result == 2
        mock_run_logger.assert_called_with(dag_id, run_id, "DB-DB", task_order, "insert", sdb_access, source_table,
                                           "(SELECT * FROM test_db.source_table) as query", "write", 2, tdb_access,
                                           target_table, '', "success")
        mock_write.return_value.save.assert_called_once()


@patch("db_db.run_logger")
def test_table_data_read_write_failure_read(mock_run_logger, mock_spark, mock_sdb_conn, mock_tdb_conn):
    """Test failure during reading data in table_data_read_write."""
    spark, _ = mock_spark
    source_table = "test_db.source_table"
    target_table = "test_db.target_table"
    DML = ""
    load_type = "overwrite"
    run_id = "run1"
    dag_id = "dag1"
    task_order = 1
    sdb_access = "sdb_access"
    tdb_access = "tdb_access"
    source_db = "MySQL"
    target_db = "MySQL"

    with patch.object(spark.read, "format", side_effect=Exception("Read error")):
        result = table_data_read_write(mock_sdb_conn, mock_tdb_conn, source_db, target_db, source_table, DML, target_table,
                                       load_type, run_id, dag_id, task_order, sdb_access, tdb_access, spark)
        assert result is None
        mock_run_logger.assert_called_with(dag_id, run_id, "DB-DB", task_order, "insert", sdb_access, source_table,
                                           "(SELECT * FROM test_db.source_table) as query", "read", 0, tdb_access,
                                           target_table, '', "failed")


@patch("db_db.run_logger")
@patch("db_db.load_json")
@patch("db_db.db_create_spark_session")
def test_main_db_db_success(mock_create_spark_session, mock_load_json, mock_run_logger, mock_sdb_conn, mock_tdb_conn, mock_spark):
    """Test successful execution of main_db_db."""
    mock_spark_instance, mock_df = mock_spark
    mock_create_spark_session.return_value = mock_spark_instance
    mock_load_json.return_value = {
        "source_access_db": {"host": "localhost", "username": "user", "password": "password"},
        "target_access_db": {"host": "localhost", "username": "user", "password": "password"}
    }

    input_params = {
        "source_access_db": "source_access_db",
        "target_access_db": "target_access_db",
        "source_db": "MySQL",
        "source_table": "test_db.source_table",
        "dml": "",
        "target_db": "MySQL",
        "target_table": "test_db.target_table",
        "load_type": "overwrite",
        "run_id": "run1",
        "dag_id": "dag1",
        "task_order": 1,
    }

    with patch("db_db.table_data_read_write", return_value=2):
        result = main_db_db(**input_params)
        assert result == 2
        mock_create_spark_session.assert_called_once()
        mock_run_logger.assert_called()


@patch("db_db.db_create_spark_session")
@patch("db_db.load_json")
def test_main_db_db_missing_key(mock_load_json, mock_create_spark_session):
    """Test main_db_db with missing key in input parameters."""
    mock_load_json.side_effect = KeyError("Missing key")

    input_params = {
        "source_access_db": "source_access_db",
        "target_access_db": "target_access_db",
    }

    result = main_db_db(**input_params)
    assert result is None
