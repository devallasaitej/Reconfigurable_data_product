import re
import os
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

def table_data_read_write(sdb_conn, tdb_conn, source_db, target_db, source_table, DML, target_table, load_type, run_id, dag_id, task_order, sdb_access, tdb_access):
    """
    Reads data from source table using DML if DML is blank SELECT * will be used
    Write data to target table
    Inputs: DB connection details, source table name, DML, target table
    Output: record count
    """
    try:
        # Separate try-except block for creating the Spark session
        try:
            create_spark_session()
            logging.info("Spark session created successfully.")
        except Exception as e:
            logging.error(f"Error creating Spark session: {e}")
            return None

        # Reading user input DML
        if DML == '':
            query = f"(SELECT * FROM {source_table}) as query"
        else:
            query = f"( {DML} ) as query"

        logging.info(f"Executable Query: {query}")

        sdb_name = source_table.split('.')[0]
        tdb_name = target_table.split('.')[0]

        # Preparing URL string for source JDBC connection
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
                       .load())
        except Exception as e:
            logging.error(f"Unable to read data from table: {e}")
            query = query.replace("'", "''") 
            run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert', sdb_access, source_table, query, 'read', 0, tdb_access, target_table, '', 'failed')
            return None

        if read_df:
            rc = read_df.count()
            query = query.replace("'", "''")
            run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert', sdb_access, source_table, query, 'read', rc, tdb_access, target_table, '', 'success')
            logging.info("Completed reading from source table ......")

            # Preparing URL string for target JDBC connection
            if target_db == 'MySQL':
                turl = f"jdbc:mysql://{tdb_conn['db_host']}:3306/{tdb_name}"
            elif target_db == 'PSQL':
                turl = f"jdbc:postgresql://{tdb_conn['db_host']}:5432/{tdb_name}"

            # Writing read data to target table
            try:
                (read_df.write
                 .format("jdbc")
                 .option("url", turl)
                 .option("dbtable", target_table)
                 .option("user", tdb_conn['db_username'])
                 .option("password", tdb_conn['db_password'])
                 .mode(load_type)
                 .save())
                query = query.replace("'", "''")
                logging.info("Completed writing to target table.......")
                run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert', sdb_access, source_table, query, 'write', rc, tdb_access, target_table, '', 'success')
                return rc
            except Exception as e:
                logging.error(f"Unable to write data to table: {e}")
                query = query.replace("'", "''")
                run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert', sdb_access, source_table, query, 'write', 0, tdb_access, target_table, '', 'failed')
                return None
        else:
            query = query.replace("'", "''")
            run_logger(dag_id, run_id, 'DB-DB', task_order, 'insert', sdb_access, source_table, query, 'write', 0, tdb_access, target_table, '', 'failed')
            logging.error("Error during reading from source table ......")
            return None
    except KeyError as e:
        logging.error(f"Missing key in connection parameters: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred in table_data_read_write: {e}")
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
    """
    Main function to perform data read and write operations between databases.
    Inputs: input_params (dictionary of parameters passed to the function)
    Outputs: Returns status or None based on success or failure.
    """
    try:
        # Extracting input values from the provided parameters
        sdb_access = extract_widget_values(input_params, 'source_access_db')
        tdb_access = extract_widget_values(input_params, 'target_access_db')
        source_db = extract_widget_values(input_params, 'source_db')
        source_table = extract_widget_values(input_params, 'source_table')
        DML = extract_widget_values(input_params, 'dml')
        target_db = extract_widget_values(input_params, 'target_db')
        target_table = extract_widget_values(input_params, 'target_table')
        load_type = extract_widget_values(input_params, 'load_type')
        run_id = extract_widget_values(input_params, 'run_id')
        dag_id = extract_widget_values(input_params, 'dag_id')
        task_order = extract_widget_values(input_params, 'task_order')

        # Initializing the connection dictionaries
        sdb_conn = {}
        tdb_conn = {}

        # Loading connection details from the JSON file
        secret_vals = load_json(json_path)
        sdb_conn['db_host'] = secret_vals[sdb_access]['host']
        sdb_conn['db_username'] = secret_vals[sdb_access]['username']
        sdb_conn['db_password'] = secret_vals[sdb_access]['password']
        tdb_conn['db_host'] = secret_vals[tdb_access]['host']
        tdb_conn['db_username'] = secret_vals[tdb_access]['username']
        tdb_conn['db_password'] = secret_vals[tdb_access]['password']

        # Calling the function to read and write data
        status = table_data_read_write(sdb_conn, tdb_conn, source_db, target_db, source_table, DML, target_table, load_type, run_id, dag_id, task_order, sdb_access, tdb_access)

        # Checking the status and logging the result
        if status:
            logging.info(f"{status} records loaded to table {target_table}")
            spark.stop()  # Stopping the Spark session
            return status
        else:
            logging.error("Data load failed to target table!!")
            spark.stop()
            return None

    except KeyError as e:
        # This handles cases where keys like 'source_access_db' or 'target_access_db' are missing
        logging.error(f"Missing key in input parameters or connection details: {e}")
        spark.stop()
        return None
    except FileNotFoundError as e:
        # Handles case where the JSON file path is incorrect or the file does not exist
        logging.error(f"File not found: {e}")
        spark.stop()
        return None
    except json.JSONDecodeError as e:
        # Handles errors related to JSON file loading or malformed JSON
        logging.error(f"Error decoding JSON: {e}")
        spark.stop()
        return None
    except Exception as e:
        # Catching any other exceptions and logging the error
        logging.error(f"An unexpected error occurred: {e}")
        spark.stop()
        return None
