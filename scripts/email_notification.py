import boto3
import re
import psycopg2
import datetime, time
import logging
import json
import os
import pandas as pd
from botocore.exceptions import ClientError

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

json_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(json_dir, 'secrets.json')


def send_email(subject, body_html, sender, recipients, ses_conn):

  """
  This function sends email notification to recipeints on run status
  Inputs: Email content, receiver email address list
  Output: Success/Failure message
  """
  access_key = ses_conn['access_key']
  secret_key = ses_conn['secret_key']

  #Creating boto3 ses client
  ses_client = boto3.client('ses', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name = 'us-east-2')

  # Create a MIME message
  body_text = "This email requires HTML support. Please view in a HTML-compatible email client."
  charset = "UTF-8"
  
  # Assemble the email
  try:
      response = ses_client.send_email(
          Destination={
              'ToAddresses': recipients,
          },
          Message={
              'Body': {
                  'Html': {
                      'Charset': charset,
                      'Data': body_html,
                  },
                  'Text': {
                      'Charset': charset,
                      'Data': body_text,
                  },
              },
              'Subject': {
                  'Charset': charset,
                  'Data': subject,
              },
          },
          Source=sender,
      )
  except ClientError as e:
      print(e.response['Error']['Message'])
  else:
      print("Email sent! Message ID:", response['MessageId'])


def dataframe_to_html_table(df):
    # Convert DataFrame to HTML table
    html_table = df.to_html(index=False)
    # Add inline CSS styling to color the header
    styled_header = '<th style="background-color: #FB451D; color: white;">'
    html_table = html_table.replace('<th>', styled_header)
    # Format the HTML table
    formatted_html_table = f'<html><body>{html_table}</body></html>'
    return formatted_html_table

def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)  # Parse the JSON file and convert to a dictionary
    return data

def extract_widget_values(input_params, key_prefix):
    """Extracts values from widget_inputs based on the key prefix."""
    for key, value in input_params.items():
        if key.startswith(key_prefix):
          return value 


def main_email(**input_params):
  
  secret_vals = load_json(json_path)
  run_id = extract_widget_values(input_params, 'run_id')
  dag_id = extract_widget_values(input_params, 'dag_id')
  notify_recipient = extract_widget_values(input_params, 'notification_recipients')
  ses_conn = {}
  ses_conn['access_key'] = secret_vals['ses_access']['access_key']
  ses_conn['secret_key'] = secret_vals['ses_access']['secret_key']
  subject = f"{dag_id} run status for run id - {run_id}"

  try :
    conn = psycopg2.connect(dbname="airflow" , user= "airflow", password= "airflow", host= "postgres", port= 5432)
    
    # Reading data into a Pandas DataFrame
    query = f"""
    SELECT A.Pipeline, A.Run_id, A.Service, A.task_order, A.Source_Path_Table, A.Source_File_DML, 
           A.record_count AS Records, A.Target_Path_Table, A.Target_file, A.Status
    FROM public.pipeline_run_log A
    INNER JOIN (
        SELECT Pipeline, Run_id, Service, task_order, MAX(timestamp) AS maxt 
        FROM public.pipeline_run_log
        WHERE run_id = '{run_id}' 
        GROUP BY Pipeline, Run_id, Service, task_order
    ) B 
    ON A.pipeline = B.pipeline 
    AND A.run_id = B.run_id 
    AND A.task_order = B.task_order 
    AND A.timestamp = B.maxt
    ORDER BY task_order;
    """
    df = pd.read_sql_query(query, conn)
    html_table = dataframe_to_html_table(df)
    body_html = f"<html><body><p>Hi,</p><p>Please find the status of pipeline: {dag_id} with run id: {run_id}</p>{html_table}</body></html>"
    sender = "noreplyd22snotification@gmail.com"
    if notify_recipient != '' :
       recipients = notify_recipient.split(',')
    else:
       recipients = []
    if len(recipients) > 0:
       send_email(subject, body_html, sender, recipients, ses_conn)
    else:
       logging.info('No recipients to send email!')   

  except (Exception, psycopg2.DatabaseError) as error:
    logging.info(f"Error: {error}")
  finally:
    if conn:
      conn.close()
