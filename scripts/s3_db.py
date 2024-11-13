# Importing required libraries
import os
import re
import json
import boto3
import logging
import psycopg2
import pyspark
from datetime import datetime, time
from pyspark.sql import SparkSession

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

spark = None

# Initialize Spark session
def create_spark_session(s3_conn):
    logging.info("Creating Spark session ......")
    global spark
    try:
        if spark is None:
            spark = SparkSession.builder \
                .appName("S3_DB") \
                .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/postgresql-42.2.23.jar,/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.access.key", s3_conn['s3_access_key']) \
                .config("spark.hadoop.fs.s3a.secret.key", s3_conn['s3_secret_key']) \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .getOrCreate()
            logging.info("Spark session created successfully.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise  # Re-raise the exception so that it can be handled by the calling function

json_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(json_dir, 'secrets.json')

def run_logger(dag_id, run_id, service, task_order, log_op, source_access, source_s3_path, file_name,  opn, rc, target_access, target_table, target_file, status):
  """
  Inputs: SQL Query
  Output: Returns True if success
  """
  try :
    conn = psycopg2.connect(dbname="airflow" , user= "airflow", password= "airflow", host= "postgres", port= 5432)
    cursor = conn.cursor()

    if log_op == 'insert' :
      query = f"INSERT INTO pipeline_run_log VALUES( '{dag_id}','{run_id}','{service}', '{task_order}','{source_access}','{source_s3_path}','{file_name}','{opn}',{rc},'{target_access}','{target_table}','{target_file}','{status}',now())"
    elif log_op == 'update':
      query = f"UPDATE pipeline_run_log SET status='{status}' where run_id = '{run_id}' and operation='{opn}' "

    logging.info(f"Updating run log for {opn} operation....")
    # Execute the query and commit
    cursor.execute(query)
    conn.commit()
    logger.info("Run log updated successfully....")
    return True
  
  except (Exception, psycopg2.DatabaseError) as error:
    logging.info(f"Error: {error}")
  finally:
    if conn:
      cursor.close()
      conn.close()

def file_pattern_check(source_s3_path, s3_conn):
    """
    Inputs: S3 connection details, S3 file path
    Output: Return file with max timestamp of pattern present in file path
    """
    logging.info('Executing File pattern check......')
    
    try:
        # Parse S3 path and file pattern details
        s3_parts_1 = source_s3_path.split('/')
        bucket_name = s3_parts_1[2]
        prefix = '/'.join(s3_parts_1[3:-1])
        s3_file_part = s3_parts_1[-1]
        s3_parts_2 = s3_file_part.split('_')
        file_name_pattern = '_'.join(s3_parts_2[:-1])
        file_format = source_s3_path.split('.')[-1]
        logging.debug(f"Parsed S3 path - Bucket: {bucket_name}, Prefix: {prefix}")

        # Accessing keys from connection inputs
        access_key = s3_conn.get('s3_access_key')
        secret_key = s3_conn.get('s3_secret_key')

        # Creating boto3 client
        try:
            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        except Exception as e:
            logging.error(f"Failed to create S3 client: {e}")
            return None

        # Listing files from S3
        try:
            obj_list = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
            objs = [item['Key'] for item in obj_list.get('Contents', [])]
        except Exception as e:
            logging.error(f"Error accessing S3 bucket {bucket_name} with prefix {prefix}: {e}")
            return None
        
        # Filter files based on pattern
        obj_req = []
        match_list = []
        for obj in objs:
            if obj.endswith(('.txt', '.csv', '.parquet')) and ((prefix.count('/') + 1) == obj.count('/')):
                obj = obj.split('/')[-1]
                obj_req.append(obj)

        # Match files with the specific pattern
        if 'yyyymmddHHMMSS' in source_s3_path:
            pattern = rf"{file_name_pattern}_\d{{14}}\.{file_format}"
        elif 'yyyymmdd' in source_s3_path:
            pattern = rf"{file_name_pattern}_\d{{8}}\.{file_format}"
        else:
            pattern = None
        
        if pattern:
            try:
                for obj in obj_req:
                    if re.match(pattern, obj):
                        match_list.append(obj)
            except re.error as e:
                logging.error(f"Regex pattern error with {pattern}: {e}")
                return None

        if not match_list:
            logging.info(f"No files found at {source_s3_path} for input file pattern")
            return None
        else:
            # Selecting latest timestamp file for return
            latest_file = max(match_list)
            logging.info(f"Latest file for pattern - {pattern} at s3://{bucket_name}/{prefix}: {latest_file}")
            return latest_file

    except Exception as e:
        logging.error(f"An unexpected error occurred in file_pattern_check: {e}")
        return None
 
# File Read function 
def s3_file_read(s3_conn, source_s3_path, header, delimiter, s3_access, db_access, target_table, run_id, dag_id, task_order):
    """
    Function to connect to user provided S3 access point
    Read Files at S3 path
    Create Spark df of file
    Input: S3 connection keys, S3 File Path
    Output: Spark DataFrame, file name
    """
    logging.info('Executing S3 File read.......')

    try:
        # Creating Spark session
        create_spark_session(s3_conn)
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, '', 'read', 0, db_access, target_table, '', 'failed')
        return None

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

    # Setting Spark configs to access S3
    try:
        spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    except Exception as e:
        logging.error(f"Error setting Spark S3 configurations: {e}")
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

def db_file_write(db_conn, target_db, target_table, input_df, load_type):
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

def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)  # Parse the JSON file and convert to a dictionary
    return data

def extract_widget_values(input_params, key_prefix):
    """Extracts values from widget_inputs based on the key prefix."""
    for key, value in input_params.items():
        if key.startswith(key_prefix):
          return value

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
        secret_vals = load_json(json_path)
        s3_conn = {}
        db_conn = {}
        s3_conn['s3_access_key'] = secret_vals[s3_access]['s3_access_key']
        s3_conn['s3_secret_key'] = secret_vals[s3_access]['s3_secret_key']
        db_conn['access'] = db_access
        db_conn['db_host'] = secret_vals[db_access]['host']
        db_conn['db_username'] = secret_vals[db_access]['username']
        db_conn['db_password'] = secret_vals[db_access]['password']

        # Calling file read function
        inputs = s3_file_read(s3_conn, source_s3_path, header, delimiter, s3_access, db_access, target_table, run_id, dag_id, task_order)

        if inputs:
            result = db_file_write(db_conn, target_db, target_table, inputs[0], load_type)
        else:
            run_logger(dag_id, run_id, 'S3-DB', task_order, 'insert', s3_access, source_s3_path, f'{inputs[1]}', 'write', f'{result}', db_access, target_table, '', 'success')
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
        except Exception as e:
            logging.error(f"Error while stopping Spark session: {str(e)}")

    return result