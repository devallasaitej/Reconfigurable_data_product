import os
import re
import json
import boto3
import logging
import pyspark
import psycopg2
from datetime import datetime, time
from pyspark.sql import SparkSession

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def load_json():
    json_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(json_dir, 'secrets.json')
    with open(json_path, 'r') as f:
        data = json.load(f)  # Parse the JSON file and convert to a dictionary
    return data

def extract_widget_values(input_params, key_prefix):
    """Extracts values from widget_inputs based on the key prefix."""
    for key, value in input_params.items():
        if key.startswith(key_prefix):
          return value

# Initialize Spark session
def create_spark_session(s3_conn):
    logging.info("Creating Spark session ......")
    # global spark
    try:
        spark = SparkSession.builder \
                .appName("data_shift_shuttle") \
                .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/postgresql-42.2.23.jar,/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.access.key", s3_conn['s3_access_key']) \
                .config("spark.hadoop.fs.s3a.secret.key", s3_conn['s3_secret_key']) \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .getOrCreate()
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise  # Re-raise the exception so that it can be handled by the calling function

# Initialize Spark session
def db_create_spark_session():
    logging.info("Creating Spark session ......")
    # global spark
    try:
        spark = SparkSession.builder \
                .appName("data_shift_shuttle") \
                .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/postgresql-42.2.23.jar,/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
                .getOrCreate()
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise  # Re-raise the exception so that it can be handled by the calling function

def run_logger(dag_id, run_id, service, task_order, log_op, source_access, source_path_table, source_file_dml,  opn, rc, target_access, target_path_table, target_file, status):
  """
  Inputs: SQL Query
  Output: Returns True if success
  """
  try :
    conn = psycopg2.connect(dbname="airflow" , user= "airflow", password= "airflow", host= "postgres", port= 5432)
    cursor = conn.cursor()

    if log_op == 'insert' :
      query = f"INSERT INTO pipeline_run_log VALUES( '{dag_id}','{run_id}','{service}', '{task_order}','{source_access}','{source_path_table}','{source_file_dml}','{opn}',{rc},'{target_access}','{target_path_table}','{target_file}','{status}',now())"
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
    return None
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


def move_and_rename_file_in_s3(s3_conn, target_s3, new_file_name):
    """
    Moves and renames an S3 file to the required file name.
    Inputs: S3 connection details, S3 path, file name
    Output: True if successful, None if failed
    """

    try:
        # Access keys
        access_key = s3_conn['s3_access_key']
        secret_key = s3_conn['s3_secret_key']
        
        # Create boto3 client
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        # Parse bucket and prefix
        s3_parts = target_s3.split('/')
        bucket_name = s3_parts[2]
        prefix = '/'.join(s3_parts[3:-1]) if target_s3.endswith(('.csv', '.txt', '.parquet')) else '/'.join(s3_parts[3:])
        folder_prefix = f"{prefix.rstrip('/')}/{new_file_name}/"

        # Determine file format
        fformat = '.' + new_file_name.split('.')[-1]
        fformat = '.csv' if fformat in ['.csv', '.txt'] else fformat

        # List objects in the folder
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(fformat)]

        if not csv_files:
            logging.error("No files found to move and rename.")
            return None

        # Pick the last file matching the format
        last_csv_file = csv_files[-1]
        target_key = f"{prefix.rstrip('/')}/{new_file_name}"

        # Copy and delete
        s3_client.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{last_csv_file}", Key=target_key)
        s3_resource = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        bucket = s3_resource.Bucket(bucket_name)

        # Delete the original folder files
        for obj in bucket.objects.filter(Prefix=folder_prefix):
            s3_resource.Object(bucket_name, obj.key).delete()

        logging.info("Successfully moved and renamed the file.")
        return True

    except Exception as e:
        logging.error(f"Error during moving and renaming files: {e}")
        return None