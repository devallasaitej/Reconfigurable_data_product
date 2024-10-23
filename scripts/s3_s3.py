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

def file_pattern_check(source_s3_path,s3_conn):
  """
  Inputs: S3 connection details, S3 file path
  Output: Return file with max timestamp of pattern present in file path
  """
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
  
  # listing required files from s3 location
  obj_list = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
  objs = [item['Key'] for item in obj_list['Contents']]
  obj_req = []
  match_list = []
  for obj in objs:
    if obj.endswith(('.txt', '.csv', '.parquet')) & ((prefix.count('/') + 1) == obj.count('/'))  :
      obj = obj.split('/')[-1]
      obj_req.append(obj)

  # listing files with pattern match
  if 'yyyymmddHHMMSS' in source_s3_path :
    pattern = rf"{file_name_pattern}_\d{{14}}\.{file_format}"
    for obj in obj_req :
      if re.match(pattern, obj):
        match_list.append(obj)
  elif 'yyyymmdd' in source_s3_path :
    pattern = rf"{file_name_pattern}_\d{{8}}\.{file_format}"
    for obj in obj_req:
      if re.match(pattern, obj):
        match_list.append(obj)

  if len(match_list) == 0:
    logging.info(f"No files found at {source_s3_path} with input pattern")
    #run_logger('S3-S3','insert','copy','',0,'','failed')
    return None
  else:
    # Selecting latest timestamp file for return
    latest_file = max(match_list)
    logging.info(f"Latest file for pattern - {pattern} at s3://{bucket_name}/{prefix}: {latest_file}")
    return latest_file
  
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
      #run_logger('S3-S3','insert','copy','',0,target_s3_file,'failed')
      run_logger(dag_id, run_id, 'S3-S3', task_order, 'insert' ,ss3_access, source_s3_file, '', 'copy', 0, ts3_access, target_s3_file, '','failed')
      return None

    source_object_key = '/'.join(ss3_parts_1[3:])
    source_file = ss3_parts_1[-1]

    ss3_parts_2 = target_s3_file.split('/')
    target_bucket_name = ss3_parts_2[2]

    if target_bucket_name == '':
      logging.error("Invalid target file s3 path")
      #run_logger('S3-S3','insert','copy','',0,target_s3_file,'failed')
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
      #run_logger('S3-S3','insert','copy','',0,target_s3_file,'failed')
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
  Inputs: source parameters, target prameters
  Output: record count
  """  

  source_s3_file = extract_widget_values(input_params,'source_s3_file')
  target_s3_file = extract_widget_values(input_params,'target_s3_file') 
  ss3_access = extract_widget_values(input_params,'source_s3_access')
  ts3_access = extract_widget_values(input_params,'target_s3_access') 
  ss3_conn = {}
  ts3_conn = {}
  secret_vals = load_json(json_path)
  ss3_conn['s3_access_key'] = secret_vals[ss3_access]['s3_access_key']
  ss3_conn['s3_secret_key'] = secret_vals[ss3_access]['s3_secret_key']
  ts3_conn['s3_access_key'] = secret_vals[ts3_access]['s3_access_key']
  ts3_conn['s3_secret_key'] = secret_vals[ts3_access]['s3_secret_key']
  run_id = extract_widget_values(input_params, 'run_id')
  dag_id = extract_widget_values(input_params, 'dag_id')
  task_order = extract_widget_values(input_params, 'task_order')

  result = s3_s3_copy(ss3_conn, ts3_conn, source_s3_file, target_s3_file, dag_id, run_id, task_order,ss3_access,ts3_access)
  if result:
    if len(result) == 3:
      file_name = result[1]
      logging.info(f"File Transfer Successful to Target S3 path with key {file_name}!")
    else:
      n = result[1]
      logging.info(f"{n} objects Transferred from Source S3 to Target S3 path!")
  else:
    logging.info("File Transfer Failed!")

  return result