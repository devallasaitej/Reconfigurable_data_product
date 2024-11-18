import re
import os
import json
import boto3
import pysftp
import logging
import pandas as pd
from datetime import datetime, time

from utility_functions import *
      
# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def sftp_file_pattern_check(sftp_conn, source_sftp_path):
    """
    Inputs: SFTP connection details, SFTP directory path
    Output: Returns file with max timestamp matching the pattern in the specified directory
    """
    logging.info('Executing file pattern check...')

    try:
        # Parse path and file pattern details
        sftp_parts_1 = source_sftp_path.split('/')
        prefix = '/'.join(sftp_parts_1[:-1])
        sftp_file_part = sftp_parts_1[-1]
        sftp_parts_2 = sftp_file_part.split('_')
        file_name_pattern = '_'.join(sftp_parts_2[:-1])
        file_format = source_sftp_path.split('.')[-1]
        
        # SFTP connection details
        hostname = sftp_conn['host']
        username = sftp_conn['username']
        password = sftp_conn['password'] 

        # Create pysftp CnOpts object to handle known host keys
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # Disable host key checking

        # Connect to the SFTP server
        with pysftp.Connection(host=hostname, username=username, password=password, port=22, cnopts=cnopts) as sftp:
            logging.info("Connection successful to SFTP server.")
            try:
                # Change to the specified directory and list files
                sftp.chdir(prefix)
                objs = sftp.listdir()
                logging.info(f"Files in directory {prefix}: {objs}")
            except Exception as e:
                logging.error(f"Error accessing directory {prefix} on SFTP server: {e}")
                return None

        # Pattern matching for files with specified format
        match_list = []
        if 'yyyymmddHHMMSS' in source_sftp_path:
            pattern = rf"{file_name_pattern}_\d{{14}}\.{file_format}"
        elif 'yyyymmdd' in source_sftp_path:
            pattern = rf"{file_name_pattern}_\d{{8}}\.{file_format}"
        else:
            logging.info(f"No matching timestamp pattern found in {source_sftp_path}")
            return None

        # Collect files matching the specified pattern
        for obj in objs:
            if re.match(pattern, obj):
                match_list.append(obj)

        # Check for matched files and select the latest one
        if not match_list:
            logging.info(f"No files found at {source_sftp_path} matching the pattern.")
            return None
        else:
            latest_file = max(match_list)
            logging.info(f"Latest file matching pattern '{pattern}' in '{prefix}': {latest_file}")
            return latest_file

    except Exception as e:
        logging.error(f"An error occurred in sftp_file_pattern_check: {e}")
        return None

def sftp_get_file(sftp_conn, source_sftp_path, dag_id, run_id, task_order, ssftp_access, ts3_access, target_s3_path):
    """
    This function reads a file from an SFTP location and writes a single file to a local path.
    Inputs: SFTP connection details, source file path
    Output: Returns local path if successful, None otherwise
    """

    logging.info("Entered sftp_get_file function .....")

    try:
        # Parse file and connection details
        sftp_parts_1 = source_sftp_path.split('/')
        prefix = '/'.join(sftp_parts_1[:-1])
        sftp_file_part = sftp_parts_1[-1]
        sftp_parts_2 = sftp_file_part.split('_')
        file_name_pattern = '_'.join(sftp_parts_2[:-1])
        file_format = source_sftp_path.split('.')[-1]
        
        hostname = sftp_conn['host']
        username = sftp_conn['username']
        password = sftp_conn['password']

        # Setup SFTP connection options
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # Disable host key checking
        
        # Local path setup
        local_temp_dir = '/opt/airflow/tempdata/'
        if not os.path.exists(local_temp_dir):
            os.makedirs(local_temp_dir)

        # Check for latest file pattern
        if source_sftp_path.endswith(('.txt', '.csv', '.parquet')):
            logging.info('Checking for the latest file in the pattern...')
            try:
                latest_file = sftp_file_pattern_check(sftp_conn, source_sftp_path)
                if latest_file:
                    remote_path = f"{prefix}/{latest_file}"
                else:
                    logging.error("No latest file found matching the pattern.")
                    run_logger(dag_id, run_id, 'SFTP-S3', task_order, 'insert', ssftp_access, source_sftp_path, '', 'read', 0, ts3_access, target_s3_path, '', 'failed')
                    return None
            except Exception as e:
                logging.error(f"Error in finding the latest file: {e}")
                return None

            local_path = os.path.join(local_temp_dir, latest_file)
            
            # SFTP connection and file download
            try:
                with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
                    logging.info("Connection successful to SFTP server.")
                    sftp.get(remote_path, local_path)  # Download the file
                    logging.info(f"File {latest_file} downloaded to local path {local_path}")

                    # Reading file content to get record count
                    try:
                        df = pd.read_csv(local_path)
                        rc = len(df)
                        logging.info(f"File contains {rc} records.")
                        run_logger(dag_id, run_id, 'SFTP-S3', task_order, 'insert', ssftp_access, source_sftp_path, latest_file, 'read', rc, ts3_access, target_s3_path, '', 'success')
                    except Exception as e:
                        logging.error(f"Error reading downloaded file for record count: {e}")
                        run_logger(dag_id, run_id, 'SFTP-S3', task_order, 'insert', ssftp_access, source_sftp_path, latest_file, 'read', 0, ts3_access, target_s3_path, '', 'failed')
                        return None
            except Exception as e:
                logging.error(f"Error during SFTP file download: {e}")
                run_logger(dag_id, run_id, 'SFTP-S3', task_order, 'insert', ssftp_access, source_sftp_path, '', 'read', 0, ts3_access, target_s3_path, '', 'failed')
                return None
            
            return local_path

    except Exception as e:
        logging.error(f"An unexpected error occurred in sftp_get_file: {e}")
        return None
      

def local_s3_transfer(s3_conn,local_path, target_s3_path, dag_id, run_id, task_order, ssftp_access, source_sftp_path, ts3_access, spark):

  """
  This function reads from DBFS local and writes to target S3
  Inputs: DBFS local path, S3 connection details, target path
  Output: Final S3 object key
  """

  s3_parts_1 = target_s3_path.split('/')
  bucket_name = s3_parts_1[2]
  prefix = '/'.join(s3_parts_1[3:-1])
  key = '/'.join(s3_parts_1[3:])

  access_key = s3_conn['s3_access_key']
  secret_key = s3_conn['s3_secret_key']

  # Initialize S3 client with credentials
  s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)  

  current_time = datetime.now()
  timestamp = current_time.strftime("%Y%m%d%H%M%S")
  current_day = datetime.today().date()
  date = current_day.strftime("%Y%m%d")  

  if target_s3_path.endswith(('.csv','.txt')):
    target_file_name = target_s3_path.split('/')[-1]
    if ('_yyyymmddHHMMSS' in target_file_name) :
      file_parts = target_file_name.split('_yyyymmddHHMMSS')
      file_name = file_parts[0]+'_'+timestamp+file_parts[1]
      target_object_key = '/'.join(s3_parts_1[3:-1])
      target_object_key = target_object_key + '/'+ file_name
      file_path = 's3a://'+bucket_name+'/'+target_object_key
    elif ('_yyyymmdd' in target_file_name) :
      file_parts = target_file_name.split('_yyyymmdd')
      file_name = file_parts[0]+'_'+date+file_parts[1]
      target_object_key = '/'.join(s3_parts_1[3:-1])
      target_object_key = target_object_key + '/'+ file_name
      file_path = 's3a://'+bucket_name+'/'+target_object_key
    else:
      target_object_key = key
      file_path = 's3a://'+bucket_name+'/'+target_object_key
      file_name = target_object_key.split('/')[-1]
  else:
    file_name = local_path.split('/')[-1]
    target_object_key = prefix+ '/'+ file_name
    file_path = 's3a://'+bucket_name+'/'+target_object_key

  file_format = file_name.split('.')[-1]

  if file_format == 'txt':
    delimiter = '\t'
  else:
    delimiter = ','
  try:
    latest_file_name = local_path.split('/')[-1]
    if local_path.split('.')[-1] == 'txt':
      s_delimiter = '\t'
    else:
      s_delimiter = ','
    input_df = spark.read.option("delimiter",s_delimiter).csv(local_path)
    print(input_df.printSchema())
    rc = input_df.count()
    input_df.coalesce(1).write.format('csv').option('header','False').option("delimiter",delimiter).mode('overwrite').save(file_path)
    res = move_and_rename_file_in_s3(s3_conn, target_s3_path, file_name)
    if res:
      run_logger(dag_id, run_id ,'SFTP-S3',task_order,'insert',ssftp_access , source_sftp_path , latest_file_name , 'write', rc, ts3_access, target_s3_path ,file_name ,'success')
      logging.info(f"File transfer successful to {bucket_name} with key {target_object_key}")
      return target_object_key
  except Exception as e:
    run_logger(dag_id, run_id ,'SFTP-S3',task_order,'insert',ssftp_access , source_sftp_path , latest_file_name , 'write', 0, ts3_access, target_s3_path, '' ,'failed')
    logging.error(f"Unable to write to S3: {e}")
    return None

def main_sftp_s3(**input_params):
    """
    Main function for transferring files from SFTP to S3 with exception handling.
    """
    try:
        # Extracting widget values
        try:
            ssftp_access = extract_widget_values(input_params, 'source_sftp_access')
            ts3_access = extract_widget_values(input_params, 'target_s3_access')
            source_sftp_path = extract_widget_values(input_params, 'source_sftp_file_path')
            target_s3_path = extract_widget_values(input_params, 'target_s3_file_path')
            run_id = extract_widget_values(input_params, 'run_id')
            dag_id = extract_widget_values(input_params, 'dag_id')
            task_order = extract_widget_values(input_params, 'task_order')
        except KeyError as e:
            logging.error(f"Missing required input parameter: {e}")
            return None

        # Loading secret values
        try:
            secret_vals = load_json()
            sftp_conn = {
                'host': secret_vals[ssftp_access]['host'],
                'username': secret_vals[ssftp_access]['username'],
                'password': secret_vals[ssftp_access]['password']
            }
            s3_conn = {
                's3_access_key': secret_vals[ts3_access]['s3_access_key'],
                's3_secret_key': secret_vals[ts3_access]['s3_secret_key']
            }
        except KeyError as e:
            logging.error(f"Missing required key in secrets: {e}")
            return None
        except Exception as e:
            logging.error(f"Error loading secret values: {e}")
            return None

        try:
            spark = create_spark_session(s3_conn)
        except Exception as e:
            logging.error(f"Failed to create Spark session: {e}")
            return None        

        # Fetching file from SFTP
        try:
            local_path = sftp_get_file(sftp_conn, source_sftp_path, dag_id, run_id, task_order, ssftp_access, ts3_access, target_s3_path)
            if not local_path:
                logging.error("Failed to fetch file from SFTP.")
                return None
        except Exception as e:
            logging.error(f"Error during SFTP file retrieval: {e}")
            return None

        # Transferring file from local to S3
        try:
            res = local_s3_transfer(s3_conn, local_path, target_s3_path, dag_id, run_id, task_order, ssftp_access, source_sftp_path, ts3_access, spark)
            if res:
                logging.info("File transfer Successful")
                return res
            else:
                logging.error("Failed to transfer file from local to S3")
                return None
        except Exception as e:
            logging.error(f"Error during S3 file transfer: {e}")
            return None

    except Exception as e:
        logging.error(f"An unexpected error occurred in main_sftp_s3: {e}")
        return None
    finally:
        # Ensuring Spark session is stopped even if there is an error
        try:
            spark.stop()
            logging.info("Stopping Spark Session......")
        except Exception as e:
            logging.error(f"Error while stopping Spark session: {str(e)}")    
  