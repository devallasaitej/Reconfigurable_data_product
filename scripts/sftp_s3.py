import boto3
import datetime, time
import pandas as pd
from datetime import datetime
import logging
import re
import os
import json
import psycopg2
import pysftp
import pyspark
from pyspark.sql import SparkSession

spark = None

# Initialize Spark session
def create_spark_session(s3_conn):
    logging.info("Creating Spark session ......")
    global spark
    if spark is None:
      spark = SparkSession.builder \
        .appName("SFTP_S3") \
        .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/postgresql-42.2.23.jar,/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar:/opt/airflow/jars/mysql-connector-java-8.0.32.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", s3_conn['s3_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_conn['s3_secret_key']) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
      
# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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


def sftp_file_pattern_check(sftp_conn, source_sftp_path):
  """
  Inputs: Sftp connection details, Sftp Directory
  Output: Return file with max timestamp of pattern present in Directory
  """
  logging.info('Executing File pattern check......')

  sftp_parts_1 = source_sftp_path.split('/')
  prefix = '/'.join(sftp_parts_1[:-1])
  sftp_file_part = sftp_parts_1[-1]
  sftp_parts_2 = sftp_file_part.split('_')
  file_name_pattern = '_'.join(sftp_parts_2[:-1])
  file_format = source_sftp_path.split('.')[-1]

  # Create pysftp CnOpts object to handle known host keys
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None  # Disable host key checking

  hostname = sftp_conn['host']
  username = sftp_conn['username']
  password = sftp_conn['password'] 

  # Connect to the SFTP server
  with pysftp.Connection(host=hostname, username=username, password=password, port=22, cnopts=cnopts) as sftp:
    print('Connection successful to SFTP .........')
    #Change to the specified directory
    sftp.chdir(prefix)
    #List files in the directory
    objs = sftp.listdir()

    print('objs:',objs)

  match_list = []
  # listing files with pattern match
  if 'yyyymmddHHMMSS' in source_sftp_path :
    pattern = rf"{file_name_pattern}_\d{{14}}\.{file_format}"
    for obj in objs :
      if re.match(pattern, obj):
        match_list.append(obj)
  elif 'yyyymmdd' in source_sftp_path :
    pattern = rf"{file_name_pattern}_\d{{8}}\.{file_format}"
    for obj in objs:
      if re.match(pattern, obj):
        match_list.append(obj)

  print('match_list:',match_list)
  if len(match_list) == 0:
    logging.info(f"No files found at {source_sftp_path} with input pattern")
    return None
  else:
    # Selecting latest timestamp file for return
    latest_file = max(match_list)
    print('latest file:',latest_file)
    logging.info(f"Latest file for pattern - {pattern} at {prefix}: {latest_file}")
    return latest_file
  

def move_and_rename_file_in_s3(s3_conn, target_s3, new_file_name):
  """
  This function moves the s3 file & renames it to required file name
  Inputs: S3 connection details, s3_path, file name
  Outputs: True
  """

  access_key = s3_conn['s3_access_key']
  secret_key = s3_conn['s3_secret_key']
  
  s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

  s3_parts = target_s3.split('/')
  bucket_name = s3_parts[2]
  if target_s3.endswith(('.csv','.txt','.parquet')):
    prefix = '/'.join(s3_parts[3:-1])
  else:
    prefix = '/'.join(s3_parts[3:])
  
  if prefix.endswith('/'):
    folder_prefix = prefix + new_file_name + '/'
  else :
    folder_prefix = prefix +'/' +new_file_name+'/'

  fformat = '.'+new_file_name.split('.')[-1]
  if fformat in ['.csv','.txt']:
    fformat = '.csv'
  else:
    fformat = fformat
  # List objects in the folder
  response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
  # Retrieve the filenames from the list of objects
  csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(f'{fformat}')]
  if csv_files:
    # Pick the last csv file
    last_csv_file = csv_files[-1]
    # Move the file to upper directory
    if prefix.endswith('/') :
      key = prefix + new_file_name
    else:
      key = prefix + '/' + new_file_name
    s3.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{last_csv_file}", Key= key)
    # Delete original directory
    s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket = s3.Bucket(f'{bucket_name}')
    for obj in bucket.objects.filter(Prefix= f'{folder_prefix}'):
      s3.Object(bucket.name,obj.key).delete()
    logging.info("Moved and Renamed files")
    return True
  else :
    logging.info("Error during moving & renaming files")
    return None
  

def sftp_get_file(sftp_conn, source_sftp_path, dag_id, run_id, task_order, ssftp_access, ts3_access, target_s3_path):

  """
  This function reads file from SFTP location and writes a single file to DBFS local
  Inputs: SFTP connection details, source file path
  Ouput: Returns local DBFS path
  """

  logging.info("Entered sftp_get_file function .....")
  
  sftp_parts_1 = source_sftp_path.split('/')
  prefix = '/'.join(sftp_parts_1[:-1])
  sftp_file_part = sftp_parts_1[-1]
  sftp_parts_2 = sftp_file_part.split('_')
  file_name_pattern = '_'.join(sftp_parts_2[:-1])
  file_format = source_sftp_path.split('.')[-1]
  
  hostname = sftp_conn['host']
  username = sftp_conn['username']
  password = sftp_conn['password']

  # Create pysftp CnOpts object to handle known host keys
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None  # Disable host key checking
    
  # Determine local path in Docker container (instead of DBFS)
  local_temp_dir = '/opt/airflow/tempdata/'  # Local directory in the Docker container
  if not os.path.exists(local_temp_dir):
        os.makedirs(local_temp_dir)
    
  if source_sftp_path.endswith(('.txt', '.csv', '.parquet')):
        logging.info('Entered func')
        latest_file = sftp_file_pattern_check(sftp_conn, source_sftp_path)
        if latest_file:
            remote_path = f"{prefix}/{latest_file}"
        else:
            logging.error("No latest file found")
            run_logger(dag_id, run_id ,'SFTP-S3',task_order,'insert',ssftp_access ,source_sftp_path , '' , 'read', 0, ts3_access, target_s3_path,'' ,'failed')
            return None
        
        local_path = os.path.join(local_temp_dir, latest_file)
        
        # Connect to the SFTP server and download the file to the local path
        with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
            logging.info("Connection successful to SFTP")
            sftp.get(remote_path, local_path)  # Download the file
            df = pd.read_csv(local_path)
            rc = len(df)
            logging.info(f"File {latest_file} downloaded to local path {local_path}")
            run_logger(dag_id, run_id ,'SFTP-S3',task_order,'insert',ssftp_access ,source_sftp_path , latest_file , 'read', rc, ts3_access, target_s3_path,'' ,'success')
        print('lc:',local_path)
        return local_path
  
  return None
      

def local_s3_transfer(s3_conn,local_path, target_s3_path, dag_id, run_id, task_order, ssftp_access, source_sftp_path, ts3_access):

  """
  This function reads from DBFS local and writes to target S3
  Inputs: DBFS local path, S3 connection details, target path
  Output: Final S3 object key
  """

  create_spark_session(s3_conn)

  s3_parts_1 = target_s3_path.split('/')
  bucket_name = s3_parts_1[2]
  prefix = '/'.join(s3_parts_1[3:-1])
  key = '/'.join(s3_parts_1[3:])

  access_key = s3_conn['s3_access_key']
  secret_key = s3_conn['s3_secret_key']

  # Setting Spark configs to access S3
  spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
  spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
  spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

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
      #run_logger('SFTP-S3','insert','write',file_name,rc, file_path,'success')
      run_logger(dag_id, run_id ,'SFTP-S3',task_order,'insert',ssftp_access , source_sftp_path , latest_file_name , 'write', rc, ts3_access, target_s3_path ,file_name ,'success')
      logging.info(f"File transfer successful to {bucket_name} with key {target_object_key}")
      return target_object_key
  except Exception as e:
    #run_logger('SFTP-S3','insert','write',file_name,0,target_s3_path,'failed')
    run_logger(dag_id, run_id ,'SFTP-S3',task_order,'insert',ssftp_access , source_sftp_path , latest_file_name , 'write', 0, ts3_access, target_s3_path, '' ,'failed')
    logging.error(f"Unable to write to S3: {e}")
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

def main_sftp_s3(**input_params):
  
  ssftp_access = extract_widget_values(input_params,'source_sftp_access')
  ts3_access = extract_widget_values(input_params,'target_s3_access')
  secret_vals = load_json(json_path)
  sftp_conn = {}
  sftp_conn['host'] = secret_vals[ssftp_access]['host']
  sftp_conn['username'] = secret_vals[ssftp_access]['username']
  sftp_conn['password'] = secret_vals[ssftp_access]['password']
  s3_conn = {}
  s3_conn['s3_access_key'] = secret_vals[ts3_access]['s3_access_key']
  s3_conn['s3_secret_key'] = secret_vals[ts3_access]['s3_secret_key']
  source_sftp_path = extract_widget_values(input_params,'source_sftp_file_path')
  target_s3_path = extract_widget_values(input_params,'target_s3_file_path')
  run_id = extract_widget_values(input_params, 'run_id')
  dag_id = extract_widget_values(input_params, 'dag_id')
  task_order = extract_widget_values(input_params, 'task_order') 
  
  local_path = sftp_get_file(sftp_conn, source_sftp_path, dag_id, run_id, task_order, ssftp_access, ts3_access, target_s3_path)
  if local_path :
    res = local_s3_transfer(s3_conn,local_path, target_s3_path, dag_id, run_id, task_order, ssftp_access, source_sftp_path, ts3_access)
    if res : 
      logging.info(f"File transfer Successful")
      return res
    else:
      #run_logger('SFTP-S3','insert','write','',0,target_s3_path,'failed')
      logging.info("Failed to transfer file from SFTP to S3")
      return None
  