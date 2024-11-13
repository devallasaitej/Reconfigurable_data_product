import re
import os
import json
import boto3
import psycopg2
import logging
from datetime import datetime, time

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

json_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(json_dir, 'secrets.json')

def run_logger(dag_id, run_id, service, task_order, log_op, source_access, source_path_table, source_file_dml,  opn, rc, target_access, target_path_table, file_name, status):
  """
  Inputs: SQL Query
  Output: Returns True if success
  """
  try :
    conn = psycopg2.connect(dbname="airflow" , user= "airflow", password= "airflow", host= "postgres", port= 5432)
    cursor = conn.cursor()

    if log_op == 'insert' :
      query = f"INSERT INTO pipeline_run_log VALUES( '{dag_id}','{run_id}','{service}', '{task_order}','{source_access}','{source_path_table}','{source_file_dml}','{opn}',{rc},'{target_access}','{target_path_table}','{file_name}','{status}',now())"
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
    try:
        logging.info('Executing File pattern check......')

        s3_parts_1 = source_s3_path.split('/')
        bucket_name = s3_parts_1[2]
        prefix = '/'.join(s3_parts_1[3:-1])
        s3_file_part = s3_parts_1[-1]
        s3_parts_2 = s3_file_part.split('_')
        file_name_pattern = '_'.join(s3_parts_2[:-1])
        file_format = source_s3_path.split('.')[-1]

        # Accessing keys from connection inputs
        access_key = s3_conn['s3_access_key']
        secret_key = s3_conn['s3_secret_key']

        # Creating boto3 client
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        
        # Listing required files from s3 location
        obj_list = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in obj_list:
            logging.error(f"No objects found at S3 path: {source_s3_path}")
            return None

        objs = [item['Key'] for item in obj_list['Contents']]
        obj_req = []
        match_list = []

        # Filter the objects based on file extensions and prefix structure
        for obj in objs:
            if obj.endswith(('.txt', '.csv', '.parquet')) and ((prefix.count('/') + 1) == obj.count('/')):
                obj = obj.split('/')[-1]
                obj_req.append(obj)

        # listing files with pattern match
        if 'yyyymmddHHMMSS' in source_s3_path:
            pattern = rf"{file_name_pattern}_\d{{14}}\.{file_format}"
            for obj in obj_req:
                if re.match(pattern, obj):
                    match_list.append(obj)
        elif 'yyyymmdd' in source_s3_path:
            pattern = rf"{file_name_pattern}_\d{{8}}\.{file_format}"
            for obj in obj_req:
                if re.match(pattern, obj):
                    match_list.append(obj)

        if len(match_list) == 0:
            logging.info(f"No files found at {source_s3_path} with input pattern")
            return None
        else:
            # Selecting latest timestamp file for return
            latest_file = max(match_list)
            logging.info(f"Latest file for pattern - {pattern} at s3://{bucket_name}/{prefix}: {latest_file}")
            return latest_file

    except boto3.exceptions.S3UploadFailedError as e:
        logging.error(f"S3 upload failed: {e}")
        return None
    except boto3.exceptions.S3DownloadError as e:
        logging.error(f"S3 download failed: {e}")
        return None
    except KeyError as e:
        logging.error(f"Missing key in connection details: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None

  
def s3_s3_copy(ss3_conn, ts3_conn, source_s3_file, target_s3_file, dag_id, run_id, task_order, ss3_access, ts3_access):
    """
    Moves objects from source S3 to target S3 with logging and error handling.
    Inputs: Source and target S3 connection details, file paths, and logging identifiers.
    Outputs: Returns a list containing status, filename, and file count.
    """
    
    # Helper function to log and return on failure
    def log_failure(message, error_code=0):
        logging.error(message)
        run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert', ss3_access, source_s3_file, '', 'copy', error_code, ts3_access, target_s3_file, '', 'failed')
        return None

    try:
        # Extract credentials and initialize S3 clients
        source_s3 = boto3.client('s3', aws_access_key_id=ss3_conn['s3_access_key'], aws_secret_access_key=ss3_conn['s3_secret_key'])
        target_s3 = boto3.client('s3', aws_access_key_id=ts3_conn['s3_access_key'], aws_secret_access_key=ts3_conn['s3_secret_key'])

        # Date and time stamps for file naming
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        date = datetime.today().strftime("%Y%m%d")

        # Bucket and prefix extraction
        source_bucket_name, source_object_key = extract_bucket_key(source_s3_file)
        target_bucket_name, target_object_key = extract_bucket_key(target_s3_file)

        if not source_bucket_name or not target_bucket_name:
            return log_failure("Invalid S3 paths provided.")

        # Handling for individual file or directory transfer
        if source_s3_file.endswith(('.txt', '.csv', '.parquet')):
            source_file, target_object_key, file_name = handle_file_copy(source_s3, target_s3, source_bucket_name, target_bucket_name, source_object_key, target_object_key, timestamp, date)
        else:
            file_name, file_count = handle_directory_copy(source_s3, target_s3, source_bucket_name, target_bucket_name, source_object_key, target_object_key)

        if not file_name:
            return log_failure("File transfer failed.")

        # Log success and return
        run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert', ss3_access, source_s3_file, '', 'copy', 1, ts3_access, target_s3_file, file_name, 'success')
        return [True, file_name, file_count]
        
    except Exception as e:
        logging.error(f"An error occurred in s3_s3_copy: {e}")
        return log_failure(f"Failed due to unexpected error: {e}")

def extract_bucket_key(s3_path):
    """Extract bucket name and object key from S3 path."""
    s3_parts = s3_path.split('/')
    bucket_name = s3_parts[2]
    object_key = '/'.join(s3_parts[3:])
    return bucket_name, object_key

def handle_file_copy(source_s3, target_s3, source_bucket_name, target_bucket_name, source_object_key, target_object_key, timestamp, date):
    """
    Handles file-level copy operations with filename pattern handling.
    """
    try:
        # Check for datetime patterns and adjust file name accordingly
        if 'yyyymmdd' in source_object_key or 'yyyymmddHHMMSS' in source_object_key:
            latest_file = file_pattern_check(source_object_key)
            if latest_file:
                source_object_key = source_object_key.replace('yyyymmddHHMMSS', timestamp).replace('yyyymmdd', date)
            else:
                logging.error("Unable to retrieve latest file with the given pattern.")
                return None, None, None

        # Copy file to target bucket
        target_s3.copy_object(Bucket=target_bucket_name, Key=target_object_key, CopySource={'Bucket': source_bucket_name, 'Key': source_object_key})
        logging.info(f"Successfully copied file to {target_bucket_name}/{target_object_key}")
        return latest_file, target_object_key, 1
    except Exception as e:
        logging.error(f"File copy error: {e}")
        return None, None, None

def handle_directory_copy(source_s3, target_s3, source_bucket_name, target_bucket_name, source_prefix, target_prefix):
    """
    Handles directory-level copy operations by iterating over all files in the source prefix.
    """
    try:
        # List all objects in source bucket with the given prefix
        response = source_s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_prefix)
        if 'Contents' not in response:
            logging.info("No files found at the source S3 path.")
            return None, 0

        file_count = 0
        for obj in response['Contents']:
            source_key = obj['Key']
            if not source_key.endswith('/'):
                destination_key = target_prefix + source_key[len(source_prefix):]
                target_s3.copy_object(Bucket=target_bucket_name, Key=destination_key, CopySource={'Bucket': source_bucket_name, 'Key': source_key})
                file_count += 1

        logging.info(f"Successfully copied {file_count} files to {target_bucket_name}/{target_prefix}")
        return target_prefix, file_count
    except Exception as e:
        logging.error(f"Directory copy error: {e}")
        return None, 0


def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)  # Parse the JSON file and convert to a dictionary
    return data

def extract_widget_values(input_params, key_prefix):
    """Extracts values from widget_inputs based on the key prefix."""
    for key, value in input_params.items():
        if key.startswith(key_prefix):
          return value

def main_s3_s3(**input_params):
    """
    Main function to call read and write functions
    Inputs: source parameters, target parameters
    Output: record count or status message
    """

    try:
        # Extracting values from input parameters
        source_s3_file = extract_widget_values(input_params, 'source_s3_file')
        target_s3_file = extract_widget_values(input_params, 'target_s3_file')
        ss3_access = extract_widget_values(input_params, 'source_s3_access')
        ts3_access = extract_widget_values(input_params, 'target_s3_access')
        run_id = extract_widget_values(input_params, 'run_id')
        dag_id = extract_widget_values(input_params, 'dag_id')
        task_order = extract_widget_values(input_params, 'task_order')

        # Loading S3 connection details
        secret_vals = load_json(json_path)
        ss3_conn = {
            's3_access_key': secret_vals[ss3_access]['s3_access_key'],
            's3_secret_key': secret_vals[ss3_access]['s3_secret_key']
        }
        ts3_conn = {
            's3_access_key': secret_vals[ts3_access]['s3_access_key'],
            's3_secret_key': secret_vals[ts3_access]['s3_secret_key']
        }

        # Calling the s3_s3_copy function
        result = s3_s3_copy(ss3_conn, ts3_conn, source_s3_file, target_s3_file, dag_id, run_id, task_order, ss3_access, ts3_access)

        if result:
            if len(result) == 3:
                file_name = result[1]
                logging.info(f"File Transfer Successful to Target S3 path with key {file_name}!")
            else:
                n = result[1]
                logging.info(f"{n} objects transferred from Source S3 to Target S3 path!")
        else:
            logging.warning("File transfer failed due to unknown issues.")

    except KeyError as e:
        logging.error(f"Missing key in input parameters or secret values: {e}")
        return None
    except Exception as e:
        logging.error(f"Error in main_s3_s3: {e}")
        return None

    return result
