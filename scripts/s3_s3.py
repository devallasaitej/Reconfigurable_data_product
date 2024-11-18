import re
import os
import json
import boto3
import logging
from datetime import datetime, time

from utility_functions import *

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

  
def s3_s3_copy(ss3_conn, ts3_conn, source_s3_file, target_s3_file, dag_id, run_id, task_order,ss3_access,ts3_access):
  """
  This function moves objects from source S3 to target S3
  Inputs: S3 connection details - source & Target, s3_paths - Source & Target, 
  Outputs: return list of status, filepath, filename
  """

  source_access_key = ss3_conn['s3_access_key']
  source_secret_key = ss3_conn['s3_secret_key']
  target_access_key = ts3_conn['s3_access_key']
  target_secret_key = ts3_conn['s3_secret_key']

  # Initialize S3 clients with different credentials
  source_s3 = boto3.client('s3', aws_access_key_id=source_access_key, aws_secret_access_key=source_secret_key)
  target_s3 = boto3.client('s3', aws_access_key_id=target_access_key, aws_secret_access_key=target_secret_key)
  
  current_time = datetime.now()
  timestamp = current_time.strftime("%Y%m%d%H%M%S")
  current_day = datetime.today().date()
  date = current_day.strftime("%Y%m%d")

  # Check if given path contains file name/file name pattern
  if source_s3_file.endswith(('.txt', '.csv', '.parquet')):
    # Retrieving buckets and prefix
    ss3_parts_1 = source_s3_file.split('/')
    source_bucket_name = ss3_parts_1[2]

    if source_bucket_name == '':
      logging.error("Invalid source file s3 path")
      run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
      return None

    source_object_key = '/'.join(ss3_parts_1[3:])
    source_file = ss3_parts_1[-1]

    ss3_parts_2 = target_s3_file.split('/')
    target_bucket_name = ss3_parts_2[2]

    if target_bucket_name == '':
      logging.error("Invalid target file s3 path")
      run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
      return None
    
    target_object_key = '/'.join(ss3_parts_2[3:])

    # Check if there's any datetime pattern in input Source S3 path
    if ('yyyymmdd' in source_file) or ('yyyymmddHHMMSS' in source_file):
      latest_file = file_pattern_check(source_s3_file,ss3_conn)
    
    if latest_file:
      source_object_key = '/'.join(ss3_parts_1[3:-1])
      source_object_key = source_object_key + '/'+ latest_file
      source_file = latest_file
    else :
      logging.error("Unable to get file of provided pattern!")
      run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
      return None

    # Check if target file path contains target file name
    if target_s3_file.endswith(('.txt', '.csv', '.parquet')):
      target_file_name = target_s3_file.split('/')[-1]
      if ('_yyyymmddHHMMSS' in target_s3_file) :
        file_parts = target_file_name.split('_yyyymmddHHMMSS')
        file_name = file_parts[0]+'_'+timestamp+file_parts[1]
        target_object_key = '/'.join(ss3_parts_2[3:-1])
        target_object_key = target_object_key + '/'+ file_name

      elif ('_yyyymmdd' in target_s3_file) :
        file_parts = target_file_name.split('_yyyymmdd')
        file_name = file_parts[0]+'_'+date+file_parts[1]
        target_object_key = '/'.join(ss3_parts_2[3:-1])
        target_object_key = target_object_key + '/'+ file_name
    else:
      # If target name is not present use source file name as key at target S3
      if not target_object_key.endswith('/') :
        file_name = source_file
        target_object_key = target_object_key+'/'+file_name
      else:
        file_name = source_file
        target_object_key = target_object_key + file_name

    # Copy the object from source bucket to target bucket
    target_s3.copy_object(Bucket=target_bucket_name, Key=target_object_key, CopySource={'Bucket': source_bucket_name, 'Key': source_object_key})
    tgt = 's3://'+target_bucket_name+'/'+target_object_key
    run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 1, ts3_access, target_s3_file, target_object_key,'success')
    return [True, file_name,1]
    

  else:

    ss3_parts_1 = source_s3_file.split('/')
    source_bucket_name = ss3_parts_1[2]

    if source_bucket_name == '':
      logging.error("Invalid source file s3 path")
      run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
      return None

    source_prefix = '/'.join(ss3_parts_1[3:])
    if not source_prefix.endswith('/'):
      source_prefix = source_prefix+'/'

    ss3_parts_2 = target_s3_file.split('/')
    target_bucket_name = ss3_parts_2[2]

    if target_bucket_name == '':
      logging.error("Invalid target file s3 path")
      run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
      return None

    target_object_key = '/'.join(ss3_parts_2[3:])
    if not target_object_key.endswith('/'):
      target_object_key = target_object_key+'/'

    # List all objects in the source bucket
    response = source_s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_prefix)

    if 'Contents' in response:
      objects_to_copy = response['Contents']
    
    # Check if there are any files present at given S3 path
    obj_list = []
    for obj in objects_to_copy:
      if obj['Key'].endswith('/'):
        continue
      else:
        obj_list.append(obj['Key'])
    # If no files present at given S3 Exit
    if len(obj_list) == 0:
      logging.info("No files at source s3 file path")
      run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
      return None
    else:
      try:
        # Copy each object to the destination path
        nobj=0
        for obj in objects_to_copy:
          source_object_key = obj['Key']
          destination_object_key = target_object_key + source_object_key[len(source_prefix):]
          # Copy the object from source bucket to destination bucket
          target_s3.copy_object(Bucket=target_bucket_name, Key=destination_object_key, CopySource={'Bucket': source_bucket_name, 'Key': source_object_key})
          nobj+=1
        tgt = 's3://'+target_bucket_name+'/'+destination_object_key
        run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', nobj, ts3_access, tgt, 'Multiple files','success')
        return [True,nobj]
      except Exception as e:
        logging.error(f"Unable to copy files to target s3:{e}")
        run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
        return None


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
        secret_vals = load_json()
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
