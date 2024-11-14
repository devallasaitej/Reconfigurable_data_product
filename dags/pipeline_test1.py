
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import uuid

# Import service functions
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
    'pipeline_test1',
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
        'dag_id': 'pipeline_test1',
        'run_id': '{{ dag_run.run_id }}'
    },
    dag=dag
)

end_task = DummyOperator(
    task_id='End',
    dag=dag
)


DB_to_S3 = PythonOperator(
    task_id='DB_to_S3',
    python_callable=main_db_s3,
    op_kwargs={**{'target_file_delimiter_1': '|', 'source_access_db_1': 'psql_db', 'dml_1': '', 'select_service_1': 'DB to S3', 'source_table_1': 'airflow.public.reddit_d', 'source_db_1': 'PSQL', 'target_s3_access_1': 'sd_s3_access', 'target_s3_file_path_1': 's3a://sdevalla-buck/sftp_file_receiver/db_s3_test_yyyymmdd.txt', 'dag_id': 'pipeline_test1'}, 'run_id': '{{ dag_run.run_id }}', 'task_order': '0'},
    dag=dag,
)
start_task >> DB_to_S3
DB_to_S3 >> email_notification_task >> end_task
