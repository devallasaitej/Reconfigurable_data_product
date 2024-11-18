import os
import re
import json
import boto3
import logging
from datetime import datetime, time

from utility_functions import *

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def table_data_read(s3_conn, db_conn, source_db, source_table, DML, dag_id, run_id, task_order, db_access, s3_access, target_s3, spark):
    """
    Reads data from table using DML; if DML is blank, SELECT * will be used
    Inputs: DB connection details, table name, DML
    Output: Returns DataFrame and record count
    """

    # Form the query based on DML
    query = f"(SELECT * FROM {source_table}) as query" if DML == '' else f"({DML}) as query"
    logging.info(f"Executable Query: {query}")
    db_name = source_table.split('.')[0]

    # Construct JDBC URL
    url = f"jdbc:mysql://{db_conn['db_host']}:3306/{db_name}" if source_db == 'MySQL' else f"jdbc:postgresql://{db_conn['db_host']}:5432/{db_name}"

    try:
        # Attempt to read data from the database table
        read_df = (
            spark.read
            .format("jdbc")
            .option("url", url)
            .option("dbtable", query)
            .option("user", db_conn['db_username'])
            .option("password", db_conn['db_password'])
            .option("driver", "org.postgresql.Driver" if source_db == 'PSQL' else "com.mysql.jdbc.Driver")
            .load()
        )
    except Exception as e:
        logging.error(f"Error during reading from table: {e}")
        # Log error in reading data
        sanitized_dml = query.replace("'", "''")
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, sanitized_dml, 'read', 0, s3_access, target_s3, '', 'failed')
        return None

    # Check if the read DataFrame is valid and count records
    if read_df:
        try:
            rc = read_df.count()
            sanitized_dml = query.replace("'", "''")
            run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, sanitized_dml, 'read', rc, s3_access, target_s3, '', 'success')
            logging.info("Completed reading from table ......")
            return [read_df, rc]
        except Exception as e:
            logging.error(f"Error counting records in DataFrame: {e}")
            run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, sanitized_dml, 'count_records', 0, s3_access, target_s3, '', 'failed')
            return None
    else:
        logging.info("No records found or error in DataFrame creation.")
        sanitized_dml = query.replace("'", "''")
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, sanitized_dml, 'read', 0, s3_access, target_s3, '', 'failed')
        return None
  
def write_data_s3(s3_conn, target_s3, target_file_name, delimiter, input_df, dag_id, run_id, task_order, db_access, source_table, s3_access, DML, spark):
    """
    This function writes Spark DataFrame read from DB to an S3 path.
    Inputs: S3 connection details, S3 landing path, final file name, input DataFrame
    Output: Record count and file name, or None on failure
    """

    try:
        s3_parts_1 = target_s3.split('/')
        bucket_name = s3_parts_1[2]
        prefix = '/'.join(s3_parts_1[3:-1]) if target_s3.endswith(('.csv','.txt','.parquet')) else '/'.join(s3_parts_1[3:])
        
        # Access keys for S3
        access_key = s3_conn['s3_access_key']
        secret_key = s3_conn['s3_secret_key']

        # Create boto3 client
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    except Exception as e:
        logging.error(f"Failed to configure S3 access or boto3 client: {e}")
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, '', 's3_config', 0, s3_access, target_s3, '', 'failed')
        return None

    # Define file naming and paths
    try:
        current_time = datetime.now()
        timestamp = current_time.strftime("%Y%m%d%H%M%S")
        current_day = datetime.today().date()
        date = current_day.strftime("%Y%m%d")
        
        # Determine the final file name and path
        if not target_file_name:
            file_name = f"{source_table.split('.')[1]}_{timestamp}.csv"
            target_s3 += '/' if not target_s3.endswith('/') else ''
            file_path = f"s3a://{bucket_name}/{prefix}/{file_name}"
        else:
            if '_yyyymmddHHMMSS' in target_file_name:
                file_parts = target_file_name.split('_yyyymmddHHMMSS')
                file_name = f"{file_parts[0]}_{timestamp}{file_parts[1]}"
            elif '_yyyymmdd' in target_file_name:
                file_parts = target_file_name.split('_yyyymmdd')
                file_name = f"{file_parts[0]}_{date}{file_parts[1]}"
            else:
                file_name = target_file_name
            file_path = f"s3a://{bucket_name}/{prefix}/{file_name}"
    except Exception as e:
        logging.error(f"Error setting up file path and naming conventions: {e}")
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, '', 'file_setup', 0, s3_access, target_s3, '', 'failed')
        return None

    try:
        # Count records and write to S3
        rc = input_df.count()
        file_format = file_name.split('.')[-1]
        
        if file_format in ['txt', 'csv']:
            input_df.coalesce(1).write.format('csv').option('header', 'True').option("delimiter", delimiter).mode('overwrite').save(file_path)
        else:
            input_df.write.mode('overwrite').parquet(file_path)
    except Exception as e:
        logging.error(f"Error during DataFrame write operation to S3: {e}")
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, DML.replace("'", "''"), 'write', 0, s3_access, target_s3, '', 'failed')
        return None

    # Move and rename file in S3 if required
    try:
        res = move_and_rename_file_in_s3(s3_conn, target_s3, file_name)
    except Exception as e:
        logging.error(f"Error during file move and rename in S3: {e}")
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, DML.replace("'", "''"), 'move_rename', 0, s3_access, target_s3, '', 'failed')
        return None

    # Log success or failure based on the result
    query = DML.replace("'", "''")
    if res:
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, query, 'write', rc, s3_access, target_s3, file_name, 'success')
        logging.info("Completed writing file.....")
        return [rc, file_name]
    else:
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'insert', db_access, source_table, query, 'write', 0, s3_access, target_s3, '', 'failed')
        logging.info("Error during writing file.....")
        return None

def main_db_s3(**input_params):
    """
    Main function to call read and write functions
    Inputs: source parameters, target parameters
    Output: record count
    """
    try:
        s3_access = extract_widget_values(input_params, 'target_s3_access')
        db_access = extract_widget_values(input_params, 'source_access_db')
        source_db = extract_widget_values(input_params, 'source_db')
        source_table = extract_widget_values(input_params, 'source_table')
        DML = extract_widget_values(input_params, 'dml')
        target_s3 = extract_widget_values(input_params, 'target_s3_file_path')
        run_id = extract_widget_values(input_params, 'run_id')
        dag_id = extract_widget_values(input_params, 'dag_id')
        task_order = extract_widget_values(input_params, 'task_order')

        if target_s3.endswith(('.csv', '.txt', '.parquet')):
            target_file_name = target_s3.split('/')[-1]
        else:
            target_file_name = ''

        delimiter = extract_widget_values(input_params, 'target_file_delimiter')
        if delimiter == '':
            delimiter = ','

        secret_vals = load_json()
        s3_conn = {
            's3_access_key': secret_vals[s3_access]['s3_access_key'],
            's3_secret_key': secret_vals[s3_access]['s3_secret_key']
        }
        db_conn = {
            'db_host': secret_vals[db_access]['host'],
            'db_username': secret_vals[db_access]['username'],
            'db_password': secret_vals[db_access]['password']
        }

        try:
            spark = create_spark_session(s3_conn)
        except Exception as e:
            logging.error(f"Failed to create Spark session: {e}")
            return None
        
        # Attempt to read table data
        try:
            inputs = table_data_read(s3_conn, db_conn, source_db, source_table, DML, dag_id, run_id, task_order, db_access, s3_access, target_s3, spark)
            if inputs:
                # Attempt to write data to S3
                try:
                    result = write_data_s3(s3_conn, target_s3, target_file_name, delimiter, inputs[0], dag_id, run_id, task_order, db_access, source_table, s3_access, DML, spark)
                    if result:
                        logging.info(f"{result[0]} records transferred from DB to S3 {target_s3} with filename {result[1]}")
                except Exception as e:
                    logging.error(f"Error in writing data to S3: {e}")
                    run_logger(dag_id, run_id, 'DB-S3', task_order, 'update', db_access, source_table, DML, 'write', 0, s3_access, target_s3, '', 'failed')
                    return None
            else:
                result = None
        except Exception as e:
            logging.error(f"Error in reading data from table: {e}")
            return None

        if not result:
            logging.info("Failed to transfer file from S3 to Target table")
            run_logger(dag_id, run_id, 'DB-S3', task_order, 'update', db_access, source_table, DML, 'write', 0, s3_access, target_s3, '', 'failed')
        
    except Exception as e:
        logging.critical(f"Critical error in main function: {e}")
        run_logger(dag_id, run_id, 'DB-S3', task_order, 'update', db_access, source_table, DML, 'write', 0, s3_access, target_s3, '', 'failed')
        return None
    finally:
        # Ensuring Spark session is stopped even if there is an error
        try:
            spark.stop()
            logging.info("Stopping Spark Session......")
        except Exception as e:
            logging.error(f"Error while stopping Spark session: {str(e)}")

    return result
