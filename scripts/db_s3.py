import boto3
import datetime, time
from datetime import datetime
import json
import logging
import re
import os
import psycopg2
import pyspark
from pyspark.sql import SparkSession

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark = None

# Initialize Spark session
def create_spark_session(s3_conn):
    logging.info("Creating Spark session ......")
    global spark
    if spark is None:
      spark = SparkSession.builder \
        .appName("DB_S3") \
        .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/postgresql-42.2.23.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar") \
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", s3_conn['s3_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_conn['s3_secret_key']) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

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

def table_data_read(s3_conn, db_conn, source_db, source_table, DML, dag_id, run_id, task_order,db_access,s3_access,target_s3):
  """
  Reads data from table using DML if DML is blank SELECT * will be used
  Inputs: DB connection details, table name, DML
  Output: returns dataframe, record count
  """

  create_spark_session(s3_conn)

  # Reading user input DML
  if DML == '':
    query = f"(SELECT * FROM {source_table})as query"
  else:
    query = '( '+ DML+' ) as query'
  
  print('Executable Query:',query)

  db_name = source_table.split('.')[0]

  # Preparing url string for JDBC connection
  if source_db == 'MySQL':
    url = f"jdbc:mysql://{db_conn['db_host']}:3306/{db_name}" 
  elif source_db == 'PSQL':
    url = f"jdbc:postgresql://{db_conn['db_host']}:5432/{db_name}" 
  
  try:
    read_df = (spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", query)
    .option("user", db_conn['db_username'])
    .option("password", db_conn['db_password'])
    .option("driver", "org.postgresql.Driver")
    .load()
    )
  except Exception as e:
    logging.error(f"Error during reading from table: {e}")
    #run_logger('DB-S3','insert','read', query, 0, target_s3 ,'failed')
    DML = query.replace("'", "''") 
    run_logger(dag_id, run_id, 'DB-S3', task_order ,'insert',db_access, source_table,DML,'read',0,s3_access,target_s3,'','failed')
    return None
  if read_df :
    rc = read_df.count()
    DML = query.replace("'", "''") 
    #.replace("'", "\\'")
    #run_logger('DB-S3','insert','read', query, rc, target_s3 ,'success')

    run_logger(dag_id, run_id, 'DB-S3', task_order ,'insert',db_access, source_table,DML,'read',rc,s3_access,target_s3,'','success')
    logging.info("Completed reading from table ......")
    return [read_df,rc]
  else:
    DML = query.replace("'", "''") 
    #run_logger('DB-S3','insert','read', query, 0, target_s3 ,'failed')
    run_logger(dag_id, run_id, 'DB-S3', task_order ,'insert',db_access, source_table,DML,'read',0,s3_access,target_s3,'','failed')
    logging.info("Error during reading from table ......")
    return None
  

def move_and_rename_file_in_s3(s3_conn, target_s3, new_file_name):
  """
  This function moves the s3 file & renames it to required file name
  Inputs: S3 connection details, s3_path, file name
  Outputs: True
  """

  access_key = s3_conn['s3_access_key']
  secret_key = s3_conn['s3_secret_key']
  
  # Creating boto3 client
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
    if prefix.endswith('/') :
      key = prefix + new_file_name
    else:
      key = prefix + '/' + new_file_name
    # Move the file to upper directory
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
  
def write_data_s3(s3_conn, target_s3, target_file_name, delimiter, input_df, dag_id, run_id, task_order, db_access, source_table, s3_access, DML ):

  """
  This function writes spark df read from DB to S3 path provided
  Inputs: S3 connection details, s3 landing path, final file name, input read df
  Output: record count, file name
  """

  s3_parts_1 = target_s3.split('/')
  print('s3_parts:',s3_parts_1)
  bucket_name = s3_parts_1[2]
  if target_s3.endswith(('.csv','.txt','.parquet')):
    prefix = '/'.join(s3_parts_1[3:-1])
  else:
    prefix = '/'.join(s3_parts_1[3:])

  # Accessing keys from connection inputs
  access_key = s3_conn['s3_access_key']
  secret_key = s3_conn['s3_secret_key']  

  # Setting Spark configs to access S3
  spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
  spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
  spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

  # Creating boto3 client
  s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

  current_time = datetime.now()
  timestamp = current_time.strftime("%Y%m%d%H%M%S")
  current_day = datetime.today().date()
  date = current_day.strftime("%Y%m%d")

  if target_file_name == '':
    file_name = source_table.split('.')[1]+'_'+timestamp+'.csv'
    if not target_s3.endswith('/'):
      target_s3 = target_s3 + '/'
    file_path = 's3a://'+bucket_name+'/'+prefix+'/'+file_name
  elif target_file_name != '':
    if ('_yyyymmddHHMMSS' in target_file_name) :
      file_parts = target_file_name.split('_yyyymmddHHMMSS')
      file_name = file_parts[0]+'_'+timestamp+file_parts[1]
      file_path = 's3a://'+bucket_name+'/'+prefix+'/'+file_name
    elif ('_yyyymmdd' in target_file_name) :
      file_parts = target_file_name.split('_yyyymmdd')
      file_name = file_parts[0]+'_'+date+file_parts[1]
      file_path = 's3a://'+bucket_name+'/'+prefix+'/'+file_name
      print('file_name:',file_name)
    else :
      file_name = target_file_name
      file_path = 's3a://'+bucket_name+'/'+prefix+'/'+file_name
    print('Target File name:',file_name)
  
  file_format = file_name.split('.')[-1]
  rc = input_df.count()

  if file_format in ['txt','csv']:
    input_df.coalesce(1).write.format('csv').option('header','True').option("delimiter",delimiter).mode('overwrite').save(file_path)
  else:
    input_df.write.mode('overwrite').parquet(file_path)

  res = move_and_rename_file_in_s3(s3_conn, target_s3, file_name )
  query = DML.replace("'", "''") 

  if res:
    #run_logger('DB-S3','insert','write','',rc,file_path, 'success')
    run_logger(dag_id, run_id, 'DB-S3', task_order ,'insert',db_access, source_table,query,'write',rc,s3_access,target_s3,file_name,'success')
    logging.info("Completed writing file.....")
    return [rc, file_name]
  else:
    #run_logger('DB-S3','insert','write','',0,target_s3, 'failed')
    run_logger(dag_id, run_id, 'DB-S3', task_order ,'insert',db_access, source_table,query,'write',0,s3_access,target_s3,'','failed')
    logging.info("Error during writing file.....")
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

def main_db_s3(**input_params):
  
  """
  Main function to call read and write functions
  Inputs: source parameters, target prameters
  Output: record count
  """

  s3_access = extract_widget_values(input_params,'target_s3_access')
  db_access = extract_widget_values(input_params,'source_access_db')
  secret_vals = load_json(json_path)
  s3_conn = {}
  s3_conn['s3_access_key'] = secret_vals[s3_access]['s3_access_key']
  s3_conn['s3_secret_key'] = secret_vals[s3_access]['s3_secret_key']
  db_conn = {}
  db_conn['db_host'] = secret_vals[db_access]['host']
  db_conn['db_username'] = secret_vals[db_access]['username']
  db_conn['db_password'] = secret_vals[db_access]['password']
  source_db = extract_widget_values(input_params,'source_db')
  source_table = extract_widget_values(input_params,'source_table')
  DML = extract_widget_values(input_params,'dml')
  target_s3 = extract_widget_values(input_params,'target_s3_file_path')
  run_id = extract_widget_values(input_params, 'run_id')
  dag_id = extract_widget_values(input_params, 'dag_id')
  task_order = extract_widget_values(input_params, 'task_order')

  if target_s3.endswith(('.csv','.txt','.parquet')):
    target_file_name = target_s3.split('/')[-1]
  else:
    target_file_name = ''
  
  delimiter = extract_widget_values(input_params,'target_file_delimiter')
  if delimiter == '' :
    delimiter = ','
  inputs = table_data_read(s3_conn, db_conn, source_db, source_table, DML, dag_id, run_id, task_order,db_access,s3_access,target_s3)
  if inputs:
    result = write_data_s3(s3_conn, target_s3, target_file_name, delimiter, inputs[0], dag_id, run_id, task_order, db_access, source_table, s3_access, DML)
    if result:
      logging.info(f"{result[0]} records transferred from DB to S3 {target_s3} with filename {result[1]}")
  else:
    ###run_logger('DB-S3','update','read','','','','failed')
    result = None
    
  if not result:
    logging.info("Failed to transfer file from S3 to Target table")
    run_logger(dag_id, run_id, 'DB-S3', task_order ,'update',db_access, source_table,DML,'write',0,s3_access,target_s3,'','failed')
    
  spark.stop()
  return result