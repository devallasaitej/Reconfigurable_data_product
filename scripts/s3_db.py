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

spark = None

# Initialize Spark session
def create_spark_session(s3_conn):
    logging.info("Creating Spark session ......")
    global spark
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
  print('prefix:',prefix)
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
    logging.info(f"No files found at {source_s3_path} for input file pattern")
    return None
  else:
    # Selecting latest timestamp file for return
    latest_file = max(match_list)
    logging.info(f"Latest file for pattern - {pattern} at s3a://{bucket_name}/{prefix}: {latest_file}")
    return latest_file
  

def s3_file_read(s3_conn, source_s3_path, header, delimiter,s3_access, db_access,target_table, run_id, dag_id, task_order) :

  """
  Function to connect to user provided S3 access point
  Read Files at S3 path
  Create Spark df of file
  Input: S3 connection keys, S3 File Path
  Output: Spark DataFrame, file name
  """
  logging.info('Executing S3 File read.......')

  create_spark_session(s3_conn)

  # Accessing keys from connection inputs
  access_key = s3_conn['s3_access_key']
  secret_key = s3_conn['s3_secret_key']
  
  # Creating boto3 client
  s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

  # Setting Spark configs to access S3
  spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
  spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
  spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

  # Extracting bucket name, prefix & file name from path
  s3_parts = source_s3_path.split('/')
  bucket_name = s3_parts[2] 

  if bucket_name == '':
    logging.error("Invalid S3 File Path")
    run_logger(dag_id, run_id ,'S3-DB',task_order,'insert',s3_access ,source_s3_path , '' , 'read', 0, db_access, target_table ,'','failed')
    return None
  
  prefix_1 = '/'.join(s3_parts[3:-1])
  prefix_2 = '/'.join(s3_parts[3:])

  # Check if file name is present in file path & create df accordingly
  if source_s3_path.endswith(('.txt', '.csv', '.parquet')):
    if ('yyyymmdd' in source_s3_path) or ('yyyymmddHHMMSS' in source_s3_path):
      # Calling file pattern check function to get latest file
      file_name = file_pattern_check(source_s3_path, s3_conn)
      if file_name :
        file_path = 's3a://'+bucket_name+'/'+prefix_1+'/'+file_name
        file_format = file_name.split('.')[1]
        if file_format == 'parquet' :
          df = spark.read.parquet(file_path)
        else:
          df = spark.read.option("inferSchema", "true").option("header", header).option("delimiter",delimiter).csv(file_path)
      else:
        logging.error("No latest file found")
        run_logger(dag_id, run_id ,'S3-DB',task_order,'insert',s3_access ,source_s3_path , '' , 'read', 0, db_access, target_table, '' ,'failed')
        return None
      
      rc = df.count()
      # Calling run logger to insert record in log table
      run_logger(dag_id, run_id ,'S3-DB',task_order,'insert', s3_access,source_s3_path, file_name, 'read',rc, db_access, target_table, '' ,'success')
      return [df,file_name]
        
    else :
      file_format = source_s3_path.split('.')[-1]
      file_name = s3_parts[-1]
      # Reading file into Spark DataFrame
      if file_format == 'parquet':
        df = spark.read.parquet(source_s3_path)
      else:
        df = spark.read.format(file_format).option("inferSchema", "true").option("header", header).option("delimiter",delimiter).csv(source_s3_path)
      rc = df.count()
      # Calling run logger to insert record in log table
      run_logger(dag_id, run_id ,'S3-DB',task_order,'insert', s3_access,source_s3_path, file_name, 'read',rc, db_access, target_table,'' ,'success')
      return [df,file_name]
 
  else:
    file_name = ''
    file_count = 0
    # listing all files from s3 location
    obj_list = s3.list_objects(Bucket=bucket_name, Prefix=prefix_2)
    try:
      objs = [item['Key'] for item in obj_list['Contents']]
    except Exception as e:
      logging.error(f"Error during listing files at s3: {e}")
      run_logger(dag_id, run_id ,'S3-DB',task_order,'insert',s3_access ,source_s3_path , '' , 'read', 0, db_access, target_table,'','failed')
      return None
    
    for obj in objs:
      if obj.endswith(('.txt', '.csv', '.parquet')) and (obj.count('/')<2):
        file_format = obj.split('.')[-1]
        file_count+=1
        file = obj.split('/')[-1]
        file_name = file_name + file + '|'
    file_name = file_name.strip('|')

    if file_name:
      if file_format == 'parquet':
        df = spark.read.parquet(source_s3_path)
        file_name =''
      else:
        df = spark.read.option("inferSchema", "true").option("header", header).option("delimiter",delimiter).csv(source_s3_path)
      rc= df.count()
     
      logging.info("Completed reading files to df ........")
      run_logger(dag_id, run_id ,'S3-DB',task_order,'insert', s3_access,source_s3_path,'MultipleFiles', 'read',rc, db_access, target_table,'' ,'success')
      return [df,file_name]
      
    else:
      logging.info("No files found at source s3 ......")
      run_logger(dag_id, run_id ,'S3-DB',task_order,'insert',s3_access ,source_s3_path , '' , 'read', 0, db_access, target_table,'','failed')
      return None


def db_file_write(db_conn, target_db, target_table, input_df, load_type):

  """
  Receives Spark DataFrame from reader function
  Writes df to target DB table
  Input: Target access connection details, Target Database table name, Spark df from reader function
  Output: True False status of write, record count
  """
  logging.info("Executing file writing function .....")


  # Input record count
  rc = input_df.count()
  # Preparing url string for JDBC connection
  if target_db == 'MySQL':
    db_name = target_table.split('.')[0]
    dbtable = target_table.split('.')[1]
    url = f"jdbc:mysql://{db_conn['db_host']}:3306/{db_name}" 
  elif target_db == 'PSQL':
    db_name = target_table.split('.')[0]
    dbtable = target_table.split('.')[1]+ '.'+ target_table.split('.')[2]
    url = f"jdbc:postgresql://{db_conn['db_host']}:5432/{db_name}" 

  # Writing input df to target table
  (
  input_df.write
  .format("jdbc")
  .option("url", url)
  .option("dbtable",dbtable)
  .option("user", db_conn['db_username'])
  .option("password", db_conn['db_password'])
  .mode(load_type)
  .save()
  )
  logging.info("Completed writing to target table.......")
  return rc

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
  Inputs: source parameters, target prameters
  Output: record count
  """
  s3_access = extract_widget_values(input_params,'source_s3_access')
  db_access = extract_widget_values(input_params,'target_access_db')
  secret_vals = load_json(json_path)
  s3_conn = {}
  s3_conn['s3_access_key'] = secret_vals[s3_access]['s3_access_key']
  s3_conn['s3_secret_key'] = secret_vals[s3_access]['s3_secret_key']
  db_conn = {}
  db_conn['access'] = db_access
  db_conn['db_host'] = secret_vals[db_access]['host']
  db_conn['db_username'] = secret_vals[db_access]['username']
  db_conn['db_password'] = secret_vals[db_access]['password']
  source_s3_path = extract_widget_values(input_params, 'source_s3_file_path')
  header = extract_widget_values(input_params, 'header_row')
  delimiter = extract_widget_values(input_params, 'file_delimiter')
  if delimiter == '' :
    delimiter = ','
  target_db = extract_widget_values(input_params, 'target_db')
  target_table = extract_widget_values(input_params, 'target_table')
  load_type = extract_widget_values(input_params, 'load_type')
  run_id = extract_widget_values(input_params, 'run_id')
  dag_id = extract_widget_values(input_params, 'dag_id')
  task_order = extract_widget_values(input_params, 'task_order')

 # Calling file read function
  inputs = s3_file_read(s3_conn, source_s3_path, header, delimiter, s3_access, db_access,target_table, run_id, dag_id, task_order)

  if inputs:
    result = db_file_write(db_conn, target_db, target_table, inputs[0],load_type)
  else:
    run_logger(dag_id, run_id ,'S3-DB',task_order,'insert',s3_access ,source_s3_path , f'{inputs[1]}' , 'write', f'{result}',db_access, target_table, '','success')
    result = None

  if result:
    logging.info("Updating run_log, marking status as success........ ")
    run_logger(dag_id, run_id ,'S3-DB',task_order,'insert',s3_access ,source_s3_path , f'{inputs[1]}' , 'write', f'{result}',db_access, target_table,'','success')
    logging.info(f"{result} records transferred from {source_s3_path} to {target_table}")
  else:
    logging.info("Updating run_log, marking status as failed......... ")
    run_logger(dag_id, run_id ,'S3-DB',task_order,'insert',s3_access ,source_s3_path , '' , 'write', 0 ,db_access, target_table,'','failed')
    logging.info("Failed to transfer file from S3 to Target table")
  
  spark.stop()

  return result