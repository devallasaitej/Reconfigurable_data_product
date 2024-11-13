
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import uuid

# Import service functions
from s3_db import main_s3_db
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
    'pipeline_test',
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
        'notification_recipients': '',
        'dag_id': 'pipeline_test',
        'run_id': '{{ dag_run.run_id }}'
    },
    dag=dag
)

end_task = DummyOperator(
    task_id='End',
    dag=dag
)


S3_to_DB = PythonOperator(
    task_id='S3_to_DB',
    python_callable=main_s3_db,
    op_kwargs={**{'source_s3_file_path_1': 's3a://sdevalla-buck/sftp_file_receiver/sftp_s3_20241014.csv', 'source_s3_access_1': 'sd_s3_access', 'target_access_db_1': 'psql_db', 'header_row_1': 'True', 'file_delimiter_1': '', 'target_db_1': 'PSQL', 'select_service_1': 'S3 to DB', 'target_table_1': 'airflow.public.reddit_d', 'load_type_1': 'Append', 'dag_id': 'pipeline_test'}, 'run_id': '{{ dag_run.run_id }}', 'task_order': '0'},
    dag=dag,
)
start_task >> S3_to_DB
S3_to_DB >> email_notification_task >> end_task
