
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import uuid

# Import service functions
from sftp_s3 import main_sftp_s3
from s3_db import main_s3_db
from db_s3 import main_db_s3
from email_notification import main_email


run_id = str(uuid.uuid4())

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'pipeline_Demo',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)

# Start and End Dummy Operators
start_task = DummyOperator(
    task_id='Start',
    dag=dag
)

# Email Notification Task
email_notification_task = PythonOperator(
    task_id='Email_notification',
    python_callable=main_email,
    op_kwargs={
        'notification_recipients': 'sdevalla@purdue.edu',
        'dag_id': 'pipeline_Demo',
        'run_id': '{{ dag_run.run_id }}'
    },
    dag=dag
)

end_task = DummyOperator(
    task_id='End',
    dag=dag
)


SFTP_to_S3 = PythonOperator(
    task_id='SFTP_to_S3',
    python_callable=main_sftp_s3,
    op_kwargs={**{'target_s3_file_path_1': 's3a://sdevalla-buck/sftp_file_receiver/sftp_s3_yyyymmdd.csv', 'source_sftp_access_1': 'sd_sftp', 'target_s3_access_1': 'sd_s3_access', 'source_sftp_file_path_1': '/sftp_restricted/sftp_yyyymmddHHMMSS.txt', 'select_service_1': 'SFTP to S3', 'dag_id': 'pipeline_Demo'}, 'run_id': '{{ dag_run.run_id }}', 'task_order': '0'},
    dag=dag,
)

S3_to_DB = PythonOperator(
    task_id='S3_to_DB',
    python_callable=main_s3_db,
    op_kwargs={**{'header_row_2': 'True', 'select_service_2': 'S3 to DB', 'target_db_2': 'PSQL', 'load_type_2': 'Append', 'target_table_2': 'airflow.public.reddit_d', 'file_delimiter_2': '', 'target_access_db_2': 'psql_db', 'source_s3_file_path_2': 's3a://sdevalla-buck/sftp_file_receiver/sftp_s3_yyyymmdd.csv', 'source_s3_access_2': 'sd_s3_access', 'dag_id': 'pipeline_Demo'}, 'run_id': '{{ dag_run.run_id }}', 'task_order': '1'},
    dag=dag,
)

DB_to_S3 = PythonOperator(
    task_id='DB_to_S3',
    python_callable=main_db_s3,
    op_kwargs={**{'source_table_3': 'airflow.public.reddit_d', 'source_db_3': 'PSQL', 'select_service_3': 'DB to S3', 'target_file_delimiter_3': '', 'target_s3_access_3': 'sd_s3_access', 'target_s3_file_path_3': 's3a://sdevalla-buck/db_data/db_s3_yyyymmddHHMMSS.parquet', 'dml_3': "SELECT * FROM airflow.public.reddit_d where flair = 'Blog'", 'source_access_db_3': 'psql_db', 'dag_id': 'pipeline_Demo'}, 'run_id': '{{ dag_run.run_id }}', 'task_order': '2'},
    dag=dag,
)
start_task >> SFTP_to_S3
SFTP_to_S3 >> S3_to_DB
S3_to_DB >> DB_to_S3
DB_to_S3 >> email_notification_task >> end_task
