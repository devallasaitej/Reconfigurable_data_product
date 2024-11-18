# Importing required libraries
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
 
# File Read function 
def s3_file_read(s3_conn, source_s3_path, header, delimiter, s3_access, db_access, target_table, run_id, dag_id, task_order,spark):
    """
    Function to connect to user provided S3 access point
    Read Files at S3 path
    Create Spark df of file
    Input: S3 connection keys, S3 File Path
    Output: Spark DataFrame, file name
    """
    logging.info('Executing S3 File read.......')

    # Accessing keys from connection inputs
    access_key = s3_conn.get('s3_access_key')
    secret_key = s3_conn.get('s3_secret_key')

    # Creating boto3 client
    try:
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    except Exception as e:
        logging.error(f"Error creating S3 client: {e}")
        run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'read', 0, db_access, target_table, '', 'failed')
        return None

    # Extracting bucket name, prefix & file name from path
    s3_parts = source_s3_path.split('/')
    bucket_name = s3_parts[2]

    if bucket_name == '':
        logging.error("Invalid S3 File Path")
        run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'read', 0, db_access, target_table, '', 'failed')
        return None

    prefix_1 = '/'.join(s3_parts[3:-1])
    prefix_2 = '/'.join(s3_parts[3:])

    # Check if file name is present in file path & create df accordingly
    try:
        if source_s3_path.endswith(('.txt', '.csv', '.parquet')):
            if ('yyyymmdd' in source_s3_path) or ('yyyymmddHHMMSS' in source_s3_path):
                # Calling file pattern check function to get latest file
                file_name = file_pattern_check(source_s3_path, s3_conn)
                if file_name:
                    file_path = 's3a://' + bucket_name + '/' + prefix_1 + '/' + file_name
                    file_format = file_name.split('.')[1]
                    if file_format == 'parquet':
                        df = spark.read.parquet(file_path)
                    else:
                        df = spark.read.option("inferSchema", "true").option("header", header).option("delimiter", delimiter).csv(file_path)
                else:
                    logging.error("No latest file found")
                    run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'read', 0, db_access, target_table, '', 'failed')
                    return None

                rc = df.count()
                # Calling run logger to insert record in log table
                run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, file_name, 'read', rc, db_access, target_table, '', 'success')
                return [df, file_name]
            else:
                file_format = source_s3_path.split('.')[-1]
                file_name = s3_parts[-1]
                # Reading file into Spark DataFrame
                if file_format == 'parquet':
                    df = spark.read.parquet(source_s3_path)
                else:
                    df = spark.read.format(file_format).option("inferSchema", "true").option("header", header).option("delimiter", delimiter).csv(source_s3_path)
                rc = df.count()
                # Calling run logger to insert record in log table
                run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, file_name, 'read', rc, db_access, target_table, '', 'success')
                return [df, file_name]
        else:
            file_name = ''
            file_count = 0
            # Listing all files from S3 location
            try:
                obj_list = s3.list_objects(Bucket=bucket_name, Prefix=prefix_2)
                objs = [item['Key'] for item in obj_list['Contents']]
            except Exception as e:
                logging.error(f"Error during listing files at S3: {e}")
                run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'read', 0, db_access, target_table, '', 'failed')
                return None

            for obj in objs:
                if obj.endswith(('.txt', '.csv', '.parquet')) and (obj.count('/') < 2):
                    file_format = obj.split('.')[-1]
                    file_count += 1
                    file = obj.split('/')[-1]
                    file_name = file_name + file + '|'
            file_name = file_name.strip('|')

            if file_name:
                if file_format == 'parquet':
                    df = spark.read.parquet(source_s3_path)
                    file_name = ''
                else:
                    df = spark.read.option("inferSchema", "true").option("header", header).option("delimiter", delimiter).csv(source_s3_path)
                rc = df.count()

                logging.info("Completed reading files to df ........")
                run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, 'MultipleFiles', 'read', rc, db_access, target_table, '', 'success')
                return [df, file_name]

            else:
                logging.info("No files found at source S3 ......")
                run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'read', 0, db_access, target_table, '', 'failed')
                return None
    except Exception as e:
        logging.error(f"Error during file read operation: {e}")
        run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'read', 0, db_access, target_table, '', 'failed')
        return None

def db_file_write(db_conn, target_db, target_table, input_df, load_type, spark):
    """
    Receives Spark DataFrame from reader function
    Writes df to target DB table
    Input: Target access connection details, Target Database table name, Spark df from reader function
    Output: True/False status of write, record count
    """
    logging.info("Executing file writing function .....")

    try:
        # Input record count
        try:
            rc = input_df.count()
        except Exception as e:
            logging.error(f"Failed to count records in DataFrame: {e}")
            return False, 0

        # Preparing URL string for JDBC connection
        try:
            if target_db == 'MySQL':
                db_name = target_table.split('.')[0]
                dbtable = target_table.split('.')[1]
                url = f"jdbc:mysql://{db_conn['db_host']}:3306/{db_name}"
            elif target_db == 'PSQL':
                db_name = target_table.split('.')[0]
                dbtable = target_table.split('.')[1] + '.' + target_table.split('.')[2]
                url = f"jdbc:postgresql://{db_conn['db_host']}:5432/{db_name}"
            else:
                logging.error(f"Unsupported database type: {target_db}")
                return False, 0
        except IndexError as e:
            logging.error(f"Error parsing target table name: {e}")
            return False, 0

        # Writing input DataFrame to the target table
        try:
            (
                input_df.write
                .format("jdbc")
                .option("url", url)
                .option("dbtable", dbtable)
                .option("user", db_conn['db_username'])
                .option("password", db_conn['db_password'])
                .mode(load_type)
                .save()
            )
            logging.info("Completed writing to target table.......")
            return rc
        except Exception as e:
            logging.error(f"Failed to write DataFrame to {target_table}: {e}")
            return None

    except Exception as e:
        logging.error(f"An unexpected error occurred in db_file_write: {e}")
        return None

# Main function
def main_s3_db(**input_params):
    """
    Main function to call read and write functions
    Inputs: source parameters, target parameters
    Output: record count
    """
    
    try:
        # Reading source & target input configs
        s3_access = extract_widget_values(input_params, 'source_s3_access')
        db_access = extract_widget_values(input_params, 'target_access_db')
        source_s3_path = extract_widget_values(input_params, 'source_s3_file_path')
        header = extract_widget_values(input_params, 'header_row')
        delimiter = extract_widget_values(input_params, 'file_delimiter')
        target_db = extract_widget_values(input_params, 'target_db')
        target_table = extract_widget_values(input_params, 'target_table')
        load_type = extract_widget_values(input_params, 'load_type')
        run_id = extract_widget_values(input_params, 'run_id')
        dag_id = extract_widget_values(input_params, 'dag_id')
        task_order = extract_widget_values(input_params, 'task_order')

        # If input delimiter is empty, set to comma
        if delimiter == '':
            delimiter = ','

        # Accessing secret credentials
        secret_vals = load_json()
        s3_conn = {}
        db_conn = {}
        s3_conn['s3_access_key'] = secret_vals[s3_access]['s3_access_key']
        s3_conn['s3_secret_key'] = secret_vals[s3_access]['s3_secret_key']
        db_conn['access'] = db_access
        db_conn['db_host'] = secret_vals[db_access]['host']
        db_conn['db_username'] = secret_vals[db_access]['username']
        db_conn['db_password'] = secret_vals[db_access]['password']

        try:
            # Creating Spark session
            spark = create_spark_session(s3_conn)
        except Exception as e:
            logging.error(f"Error creating Spark session: {e}")
            return None

        # Calling file read function
        inputs = s3_file_read(s3_conn, source_s3_path, header, delimiter, s3_access, db_access, target_table, run_id, dag_id, task_order,spark)

        if inputs:
            result = db_file_write(db_conn, target_db, target_table, inputs[0], load_type, spark)
        else:
            run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, f'{inputs[1]}', 'write', f'{result}', db_access, target_table, '', 'failed')
            result = None

        if result:
            logging.info("Updating run_log, marking status as success........ ")
            run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, f'{inputs[1]}', 'write', f'{result}', db_access, target_table, '', 'success')
            logging.info(f"{result} records transferred from {source_s3_path} to {target_table}")
        else:
            logging.info("Updating run_log, marking status as failed......... ")
            run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'write', 0, db_access, target_table, '', 'failed')
            logging.info("Failed to transfer file from S3 to Target table")

    except KeyError as e:
        logging.error(f"Missing configuration: {str(e)}")
        run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'write', 0, db_access, target_table, '', 'failed')
        logging.error("Error during S3-DB transfer due to missing key or incorrect value.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'write', 0, db_access, target_table, '', 'failed')
        logging.error("Unexpected error during S3-DB transfer.")
    finally:
        # Ensuring Spark session is stopped even if there is an error
        try:
            spark.stop()
            logging.info("Stopping Spark Session......")
        except Exception as e:
            logging.error(f"Error while stopping Spark session: {str(e)}")

    return result