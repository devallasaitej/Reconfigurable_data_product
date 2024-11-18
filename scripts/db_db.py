import re
import os
import json
import boto3
import logging
from datetime import datetime, time


from utility_functions import *

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def table_data_read_write(sdb_conn, tdb_conn, source_db, target_db, source_table, DML, target_table, load_type, run_id, dag_id, task_order, sdb_access, tdb_access, spark):
    """
    Reads data from source table using DML if DML is blank SELECT * will be used
    Write data to target table
    Inputs: DB connection details, source table name, DML, target table
    Output: record count
    """
    try:
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
        secret_vals = load_json()
        sdb_conn['db_host'] = secret_vals[sdb_access]['host']
        sdb_conn['db_username'] = secret_vals[sdb_access]['username']
        sdb_conn['db_password'] = secret_vals[sdb_access]['password']
        tdb_conn['db_host'] = secret_vals[tdb_access]['host']
        tdb_conn['db_username'] = secret_vals[tdb_access]['username']
        tdb_conn['db_password'] = secret_vals[tdb_access]['password']

        try:
            spark = db_create_spark_session()
        except Exception as e:
            logging.error(f"Failed to create Spark session: {e}")
            return None

        # Calling the function to read and write data
        status = table_data_read_write(sdb_conn, tdb_conn, source_db, target_db, source_table, DML, target_table, load_type, run_id, dag_id, task_order, sdb_access, tdb_access, spark)

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
