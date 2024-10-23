import re
import os
import json
import boto3
import logging
import psycopg2
import pyspark
from datetime import datetime, time
from pyspark.sql import SparkSession

spark = None

# Initialize Spark session
def create_spark_session():
    logging.info("Creating Spark session ......")
    global spark
    if spark is None:
      spark = SparkSession.builder \
        .appName("DB_DB") \
        .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/postgresql-42.2.23.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar") \
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.2.23.jar") \
        .getOrCreate()

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

def table_data_read_write(sdb_conn, tdb_conn, source_db, target_db, source_table, DML, target_table, load_type, run_id, dag_id, task_order, sdb_access,tdb_access):
  """
  Reads data from source table using DML if DML is blank SELECT * will be used
  Write data to target table
  Inputs: DB connection details, source table name, DML, target table
  Output: record count
  """

  create_spark_session()

  # Reading user input DML
  if DML == '':
    query = f"(SELECT * FROM {source_table})as query"
  else:
    query = '( '+ DML+' ) as query'
  
  print('Executable Query:',query)

  sdb_name = source_table.split('.')[0]
  tdb_name = target_table.split('.')[0]

  # Preparing url string for source JDBC connection
  if source_db == 'MySQL':
    surl = f"jdbc:mysql://{sdb_conn['db_host']}:3306/{sdb_name}" 
  elif source_db == 'PSQL':
    surl = f"jdbc:postgresql://{sdb_conn['db_host']}:5432/{sdb_name}" 

  # Reading source query data
  try:
    read_df = (spark.read
    .format("jdbc")
    .option("url", surl)
    .option("dbtable", query)
    .option("user", sdb_conn['db_username'])
    .option("password", sdb_conn['db_password'])
    .load()
    )
  except Exception as e:
    logging.error(f"Unable to read data from table:{e}")
    query = query.replace("'", "''") 
    #run_logger('DB-DB','insert','read', query, 0, target_db ,'failed')
    run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert' , sdb_access, source_table , query, 'read', 0, tdb_access, target_table, '','failed')
    return None
  
  if read_df :
    rc = read_df.count()
    query = query.replace("'", "''")
    #.replace("'", "\\'")
    #run_logger('DB-DB','insert','read', query, rc, target_db ,'success')
    run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert' , sdb_access, source_table , query, 'read', rc, tdb_access, target_table, '','success')
    logging.info("Completed reading from table ......")

    # Preparing url string for target JDBC connection
    if target_db == 'MySQL':
      turl = f"jdbc:mysql://{tdb_conn['db_host']}:3306/{tdb_name}" 
    elif target_db == 'PSQL':
      turl = f"jdbc:postgresql://{tdb_conn['db_host']}:5432/{tdb_name}" 

    # Writing read df to target table
    try: 
      (
      read_df.write
      .format("jdbc")
      .option("url", turl)
      .option("dbtable",target_table)
      .option("user", tdb_conn['db_username'])
      .option("password", tdb_conn['db_password'])
      .mode(load_type)
      .save()
      )
      query =  query.replace("'", "''")
      logging.info("Completed writing to target table.......")
      run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert' , sdb_access, source_table , query, 'write', rc, tdb_access, target_table, '','success')
      return rc
    except Exception as e:
      logging.error(f"Unable to write data to table: {e}")
      query = query.replace("'", "''")
      run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert' , sdb_access, source_table , query, 'write', 0, tdb_access, target_table, '','failed')
      return None
  else:
    query = query.replace("'", "''")
    run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert' , sdb_access, source_table , query, 'write', 0, tdb_access, target_table, '','failed')
    logging.info("Error during reading from source table ......")
    return None

def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)  # Parse the JSON file and convert to a dictionary
    return data

def extract_widget_values(input_params, key_prefix):
    """Extracts values from widget_inputs based on the key prefix."""
    values = {}
    for key, value in input_params.items():
        if key.startswith(key_prefix):
          return value 

def main_db_db(**input_params):
  
  sdb_access = extract_widget_values(input_params,'source_access_db')
  tdb_access = extract_widget_values(input_params,'target_access_db')
  sdb_conn={}
  tdb_conn={}
  secret_vals = load_json(json_path)
  sdb_conn['db_host'] = secret_vals[sdb_access]['host']
  sdb_conn['db_username'] = secret_vals[sdb_access]['username']
  sdb_conn['db_password'] = secret_vals[sdb_access]['password']
  source_db = extract_widget_values(input_params,'source_db')
  source_table = extract_widget_values(input_params,'source_table')
  DML = extract_widget_values(input_params,'dml')
  tdb_conn['db_host'] = secret_vals[tdb_access]['host']
  tdb_conn['db_username'] = secret_vals[tdb_access]['username']
  tdb_conn['db_password'] = secret_vals[tdb_access]['password']
  target_db = extract_widget_values(input_params,'target_db')
  target_table = extract_widget_values(input_params,'target_table')
  load_type = extract_widget_values(input_params, 'load_type')
  run_id = extract_widget_values(input_params, 'run_id')
  dag_id = extract_widget_values(input_params, 'dag_id')
  task_order = extract_widget_values(input_params, 'task_order')  

  status = table_data_read_write(sdb_conn, tdb_conn, source_db, target_db, source_table, DML, target_table, load_type, run_id, dag_id, task_order, sdb_access,tdb_access)
  # Checking run
  if status :
    logging.info(f"{status} records loaded to table {target_table}")
    spark.stop()
    return status
  else:
    logging.info("Data load failed to target table!!")
    spark.stop()
    return None