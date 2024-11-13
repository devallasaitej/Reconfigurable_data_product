# Importing required libraries
import os
import sys
import json
import psycopg2
import streamlit as st
from datetime import datetime

from scripts import s3_db, db_s3, s3_s3, db_db, sftp_s3, email_notification

# Set page config
st.set_page_config(
    page_title="Data Shift Shuttle",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS with Streamlit theme variables
st.markdown("""
<style>
/* Style for the dropdown and the card */
.stButton>button {
    width: 100%;
    height: 60px;
    font-size: 18px;
}

/* Horizontal line */
.horizontal-line {
    border-top: 1px solid #ddd;
    margin-top: 10px;
    margin-bottom: 20px;
}

/* Card style that adapts to theme */
.card {
    background-color: var(--background-color); /* Dynamic background */
    color: var(--text-color); /* Dynamic text color */
    padding: 20px;
    border-radius: 10px;
    box-shadow: 0 0 10px rgba(0, 0, 0, 0.3);
    margin-top: 10px;
}

/* Title style */
.card h2 {
    font-size: 24px;
    text-align: center;
}

/* Subtitle style */
.card p {
    font-size: 14px;
}

/* Widget labels */
.label {
    font-size: 12px;
    margin-bottom: 4px;
    display: block;
}
</style>
""", unsafe_allow_html=True)

# Title
st.markdown("<h1 style='text-align: center;margin-top: -70px;margin-bottom: 30px;'>🚀 Data Shift Shuttle</h1>", unsafe_allow_html=True)

# Updated card content for all operations
card_content = {
    "S3 - DB": """
    <h2 style='margin: 0;'>Notebook Widgets Input Guide:</h2>
    <ul>
        <li><strong>Source_S3_Access:</strong> Valid S3 access name defined with credentials.</li>
        <li><strong>Source_S3_File_Path:</strong> File path at S3 location which needs to be processed.</li>
        <li><strong>Allowed input patterns for Source_S3_File_Path:</strong>
            <ul>
                <li>Exact S3 file path can be provided: <code>s3://bucket-name/prefix/filename</code></li>
                <li>Name Pattern with date: <code>s3://bucket-name/prefix/filename_yyyymmdd.format</code> [File with latest date will be selected]</li>
                <li>Name pattern with datetime: <code>s3://bucket-name/prefix/filename_yyyymmddHHMMSS.format</code> [File with latest timestamp will be selected]</li>
                <li>Just path to directory: <code>s3://bucket-name/prefix/</code> [Note: All files present in directory will be processed in this case]</li>
            </ul>
        </li>
        <li><strong>Header Row:</strong> True by default, can be changed to False using dropdown option.</li>
        <li><strong>File Delimiter:</strong> ',' will be used as default in final file.</li>
        <li><strong>Load Type:</strong> Append mode by default, can be changed to Overwrite using dropdown option.</li>
        <li><strong>Target DB:</strong> Target database type, can be selected from dropdown.</li>
        <li><strong>Target DB Access:</strong> Valid DB access name defined with credentials.</li>
        <li><strong>Target DB Table:</strong> Target table name with pattern: <code>schema.table_name</code>.</li>
    </ul>
    """,
    "DB - S3": """
    <h2 style='margin: 0;'>Notebook Widgets Input Guide:</h2>
    <ul>
        <li><strong>Source DB:</strong> Source database type, can be selected from dropdown.</li>
        <li><strong>Source DB Access:</strong> Valid DB access name defined with credentials.</li>
        <li><strong>Source DB Table:</strong> Target table name with pattern: <code>schema.table_name</code>.</li>
        <li><strong>DML:</strong> SQL query can be input to get required data. If left blank, <code>SELECT * FROM Source DB Table</code> will be used.</li>
        <li><strong>Target_S3_Access:</strong> Valid S3 access name defined with credentials.</li>
        <li><strong>Target_S3_File_Path:</strong> File path at destination S3 bucket.</li>
        <li><strong>Allowed input patterns for Target_S3_File_Path:</strong>
            <ul>
                <li>Exact S3 file path: <code>s3://bucket-name/prefix/filename</code></li>
                <li>Name Pattern with date: <code>s3://bucket-name/prefix/filename_yyyymmdd.format</code></li>
                <li>Name pattern with datetime: <code>s3://bucket-name/prefix/filename_yyyymmddHHMMSS.format</code></li>
                <li>Directory path: <code>s3://bucket-name/prefix/</code></li>
            </ul>
        </li>
        <li><strong>Target Delimiter:</strong> ',' will be used as default in the final file.</li>
    </ul>
    """,
    "S3 - S3": """
    <h2 style='margin: 0;'>Notebook Widgets Input Guide:</h2>
    <ul>
        <li><strong>Source_S3_Access:</strong> Valid S3 access name defined with credentials.</li>
        <li><strong>Source_S3_File_Path:</strong> File path at S3 location which needs to be processed.</li>
        <li><strong>Allowed input patterns for Source_S3_File_Path:</strong>
            <ul>
                <li>Exact S3 file path: <code>s3://bucket-name/prefix/filename</code></li>
                <li>Name Pattern with date: <code>s3://bucket-name/prefix/filename_yyyymmdd.format</code> [File with latest date will be selected]</li>
                <li>Name pattern with datetime: <code>s3://bucket-name/prefix/filename_yyyymmddHHMMSS.format</code> [File with latest timestamp will be selected]</li>
                <li>Directory path: <code>s3://bucket-name/prefix/</code></li>
            </ul>
        </li>
        <li><strong>Target_S3_Access:</strong> Valid S3 access name defined with credentials.</li>
        <li><strong>Target_S3_File_Path:</strong> File path at destination S3 bucket.</li>
    </ul>
    """,
    "SFTP - S3": """
    <h2 style='margin: 0;'>Notebook Widgets Input Guide:</h2>
    <ul>
        <li><strong>Source SFTP Access:</strong> Valid SFTP access name defined with credentials.</li>
        <li><strong>Source_SFTP_File_Path:</strong> File path in SFTP Server.</li>
        <li><strong>Allowed input patterns for Source_SFTP_File_Path:</strong>
            <ul>
                <li>Exact SFTP file path: <code>/Home/Dir-1/Dir-2/filename</code></li>
                <li>Name Pattern with date: <code>/Home/Dir-1/Dir-2/filename_yyyymmdd.format</code></li>
                <li>Name pattern with datetime: <code>/Home/Dir-1/Dir-2/filename_yyyymmddHHMMSS.format</code></li>
            </ul>
        </li>
        <li><strong>Target_S3_Access:</strong> Valid S3 access name defined with credentials.</li>
        <li><strong>Target_S3_File_Path:</strong> File path at destination S3 bucket.</li>
    </ul>
    """,
    "DB - DB": """
    <h2 style='margin: 0;'>Notebook Widgets Input Guide:</h2>
    <ul>
        <li><strong>Source DB:</strong> Source database type, can be selected from dropdown.</li>
        <li><strong>Source DB Access:</strong> Valid DB access name defined with credentials.</li>
        <li><strong>Source DB Table:</strong> Target table name with pattern: <code>schema.table_name</code>.</li>
        <li><strong>DML:</strong> SQL query can be input to get required data. If left blank, <code>SELECT * FROM Source DB Table</code> will be used.</li>
        <li><strong>Target DB:</strong> Target database type, can be selected from dropdown.</li>
        <li><strong>Target DB Access:</strong> Valid DB access name defined with credentials.</li>
        <li><strong>Target DB Table:</strong> Target table name with pattern: <code>schema.table_name</code>.</li>
    </ul>
    """
}

# Define session state to track dropdown states (open/close)
if 'dropdown_state' not in st.session_state:
    st.session_state.dropdown_state = {
        "S3 - DB": False,
        "DB - S3": False,
        "S3 - S3": False,
        "SFTP - S3": False,
        "DB - DB": False
    }

# Function to toggle card visibility based on dropdown selection
def toggle_dropdown(service_name):
    st.session_state.dropdown_state[service_name] = not st.session_state.dropdown_state[service_name]

# Create columns for dropdown buttons
cols = st.columns(len(card_content))

# Render dropdown buttons and cards
for index, service_name in enumerate(card_content.keys()):
    with cols[index]:
        if st.button(service_name):
            toggle_dropdown(service_name)
        
        # Show card if dropdown is toggled on
        if st.session_state.dropdown_state[service_name]:
            st.markdown(f"""
            <div class="card">
                <h2>{service_name}</h2>
                {card_content[service_name]}
            
            """, unsafe_allow_html=True)

# Add a horizontal line after the dropdowns
st.markdown('<div class="horizontal-line"></div>', unsafe_allow_html=True)


# Initialize session state variables
if 'service_count' not in st.session_state:
    st.session_state.service_count = 0  # Start with 0 services

if 'pipeline_deployed' not in st.session_state:
    st.session_state.pipeline_deployed = False  # Track if the pipeline has been deployed

if 'services' not in st.session_state:
    st.session_state.services = []  # Store all the services selected

if 'pipeline_name' not in st.session_state:
    st.session_state.pipeline_name = ""  # Store the pipeline name

if 'pipeline_notification_recipients' not in st.session_state:
    st.session_state.pipeline_notification_recipients = ""  # Store the pipeline notification recipients

if 'cron_schedule' not in st.session_state:
    st.session_state.cron_schedule = ""

# Function to render widgets for each service
def render_service_widgets(service_num):
    service_key = f'service_{service_num}'  # Unique key for each service
    service = st.selectbox(f"Select Service for task {service_num}", [
        "",  # Empty option for default selection
        "S3 to DB",
        "DB to S3",
        "S3 to S3",
        "SFTP to S3",
        "DB to DB"
    ], key=f"select_service_{service_num}")

    # Save the selected service to the session state
    if service:
        st.session_state.services.append(service)

    # Show widgets based on the selected service
    if service == "S3 to DB":
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            #st.markdown("<span class='label'>Source_S3_Access</span>", unsafe_allow_html=True)
            st.text_input("Source S3 Access *", key=f"source_s3_access_{service_num}")

        with col2:
            #st.markdown("<span class='label'>Source_S3_File_Path</span>", unsafe_allow_html=True)
            st.text_input(f"Source S3 File_Path * (s3a://)", key=f"source_s3_file_path_{service_num}")

        with col3:
            #st.markdown("<span class='label'>Header_Row</span>", unsafe_allow_html=True)
            st.selectbox("Header Row", ["True", "False"], key=f"header_row_{service_num}")

        with col4:
            #st.markdown("<span class='label'>File_Delimiter</span>", unsafe_allow_html=True)
            st.text_input("File Delimiter (Default: ',')", key=f"file_delimiter_{service_num}")

        col5, col6, col7, col8 = st.columns([1, 1, 1, 1])  # Second row

        with col5:
            #st.markdown("<span class='label'>Load_Type</span>", unsafe_allow_html=True)
            st.selectbox("Load Type", ["Append", "Overwrite"], key=f"load_type_{service_num}")        

        with col6:
            #st.markdown("<span class='label'>Target_DB</span>", unsafe_allow_html=True)
            st.selectbox("Target DB", ["MySQL", "PSQL"], key=f"target_db_{service_num}")

        with col7:
            #st.markdown("<span class='label'>Target_Access_DB</span>", unsafe_allow_html=True)
            st.text_input("Target Access DB *", key=f"target_access_db_{service_num}")

        with col8:
            #st.markdown("<span class='label'>Target_DB_Table</span>", unsafe_allow_html=True)
            st.text_input("Target Table *", key=f"target_table_{service_num}")


    # Widgets for "DB to S3" (similar logic for other services)
    if service == "DB to S3":
        col1, col2, col3, col4 = st.columns([1, 1, 1, 2])

        with col1:
            #st.markdown("<span class='label'>Source_DB</span>", unsafe_allow_html=True)
            st.selectbox("Source DB", ["MySQL", "PSQL"], key=f"source_db_{service_num}")

        with col2:
            #st.markdown("<span class='label'>Source_DB_Access</span>", unsafe_allow_html=True)
            st.text_input("Source Access DB *", key=f"source_access_db_{service_num}")

        with col3:
            #st.markdown("<span class='label'>Source_DB_Table</span>", unsafe_allow_html=True)
            st.text_input("Source Table *", key=f"source_table_{service_num}")

        with col4:
            #st.markdown("<span class='label'>DML</span>", unsafe_allow_html=True)
            st.text_input("Query (Default: SELECT * FROM Source Table)", key=f"dml_{service_num}")

        col5, col6, col7 = st.columns([1, 3, 1])  # Second row

        with col5:
            #st.markdown("<span class='label'>Target_S3_Access</span>", unsafe_allow_html=True)
            st.text_input("Target S3 Access *", key=f"target_s3_access_{service_num}")

        with col6:
            #st.markdown("<span class='label'>Target_S3_File_Path</span>", unsafe_allow_html=True)
            st.text_input("Target S3 File Path * (s3a://)", key=f"target_s3_file_path_{service_num}")

        with col7:
            #st.markdown("<span class='label'>Target_File_Delimiter</span>", unsafe_allow_html=True)
            st.text_input("Target File Delimiter (Default: ',')", key=f"target_file_delimiter_{service_num}")


    if service == "S3 to S3":
        col1, col2, col3, col4 = st.columns([1,2,1,1])

        # First row of widgets
        with col1:
            #st.markdown("<span class='label'>Source_S3_Access</span>", unsafe_allow_html=True)
            st.text_input("Source S3 Access *", key=f"source_s3_access_{service_num}")

        with col2:
            #st.markdown("<span class='label'>Source_S3_File_Path</span>", unsafe_allow_html=True)
            st.text_input("Source S3 File Path * (s3a://)", key=f"source_s3_file_path_{service_num}")

        with col3:
            #st.markdown("<span class='label'>Target_S3_Access</span>", unsafe_allow_html=True)
            st.text_input("Target S3 Access *", key=f"target_s3_access_{service_num}")

        with col4:
            #st.markdown("<span class='label'>Target_S3_File_Path</span>", unsafe_allow_html=True)
            st.text_input("Target S3 File Path * (s3a://)", key=f"target_s3_file_path_{service_num}")


    if service == "SFTP to S3":
        col1, col2, col3, col4 = st.columns([1,2,1,2])

        # First row of widgets
        with col1:
            #st.markdown("<span class='label'>Source_SFTP_Access</span>", unsafe_allow_html=True)
            st.text_input("Source SFTP Access *", key=f"source_sftp_access_{service_num}")

        with col2:
            #st.markdown("<span class='label'>Source_SFTP_File_Path</span>", unsafe_allow_html=True)
            st.text_input("Source SFTP File Path *", key=f"source_sftp_file_path_{service_num}")

        with col3:
            #st.markdown("<span class='label'>Target_S3_Access</span>", unsafe_allow_html=True)
            st.text_input("Target S3 Access *", key=f"target_s3_access_{service_num}")

        with col4:
            #st.markdown("<span class='label'>Target_S3_File_Path</span>", unsafe_allow_html=True)
            st.text_input("Target S3 File Path * (s3a://)", key=f"target_s3_file_path_{service_num}")

    if service == "DB to DB":
        col1, col2, col3, col4 = st.columns([1,2,1,2])

        # First row of widgets
        with col1:
            #st.markdown("<span class='label'>Source_DB</span>", unsafe_allow_html=True)
            st.selectbox("Source DB", ["MySQL", "PSQL"], key=f"source_db_{service_num}")

        with col2:
            #st.markdown("<span class='label'>Source_DB_Access</span>", unsafe_allow_html=True)
            st.text_input("Source Access DB *", key=f"source_access_db_{service_num}")

        with col3:
            #st.markdown("<span class='label'>Source_DB_Table</span>", unsafe_allow_html=True)
            st.text_input(f"Source Table *", key=f"source_table_{service_num}")

        with col4:
            #st.markdown("<span class='label'>DML</span>", unsafe_allow_html=True)
            st.text_input("Query (Default: SELECT * FROM Source Table)", key=f"dml_{service_num}")

        # Second row of widgets
        col5, col6, col7, col8 = st.columns([1, 1, 1, 1])  # Extend Notification_Recipients column

        with col5:
            #st.markdown("<span class='label'>Target_DB</span>", unsafe_allow_html=True)
            st.selectbox("Target DB", ["MySQL", "PSQL"], key=f"target_db_{service_num}")

        with col6:
            #st.markdown("<span class='label'>Target_DB_Access</span>", unsafe_allow_html=True)
            st.text_input("Target Access DB *", key=f"target_access_db_{service_num}")

        with col7:
            #st.markdown("<span class='label'>Target_DB_Table</span>", unsafe_allow_html=True)
            st.text_input("Target Table *", key=f"target_table_{service_num}")

        with col8:
            #st.markdown("<span class='label'>Load_Type</span>", unsafe_allow_html=True)
            st.selectbox("Load Type", ["Append", "Overwrite"], key=f"load_type_{service_num}")

# Function to gather all inputs and create a nested JSON structure
def create_pipeline_json():
    pipeline_data = {
        "Pipeline Name": st.session_state.pipeline_name,
        "Pipeline Notification Recipients": st.session_state.pipeline_notification_recipients,
        "Cron Schedule": st.session_state.cron_schedule,
        "Services": {}
    }

    # Loop through all services added and capture their inputs
    for i in range(1, st.session_state.service_count + 1):
        service_name = st.session_state.get(f"select_service_{i}", f"Service {i}")
        service_data = {}

        # Collect all widgets for the service
        for key in st.session_state.keys():
            if f"_{i}" in key:  # Capture all keys related to service i
                service_data[key] = st.session_state[key]

        # Add the service data to the pipeline dictionary
        pipeline_data["Services"][service_name] = service_data

    return pipeline_data

# Function to save the JSON to a local file (inside Docker)
def save_pipeline_json():
    pipeline_json = create_pipeline_json()

    # Define the path inside Docker (current working directory or a specific folder)
    file_path = os.path.join(os.getcwd(), 'pipeline_config.json')

    # Save the JSON data to the file
    with open(file_path, 'w') as json_file:
        json.dump(pipeline_json, json_file, indent=4)

    st.success(f"Pipeline configuration saved to {file_path}")
    return pipeline_json

# Function to insert the JSON into PostgreSQL
def insert_pipeline_json_to_postgres(json_data):
    try:
        # Establish connection to PostgreSQL
        conn = psycopg2.connect(
            host='postgres',   # Use 'localhost' if PostgreSQL is running on the same Docker network
            database='airflow',
            user='airflow',
            password='airflow'
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS pipelines (
            id SERIAL PRIMARY KEY,
            pipeline_name VARCHAR(255),
            notification_recipients TEXT,
            cron_schedule VARCHAR(20),
            services JSONB
        )
        '''
        cursor.execute(create_table_query)

        # Insert the JSON data into the database
        insert_query = '''
        INSERT INTO pipelines (pipeline_name, notification_recipients, cron_schedule,  services)
        VALUES (%s, %s, %s, %s)
        '''
        cursor.execute(insert_query, (
            json_data["Pipeline Name"],
            json_data["Pipeline Notification Recipients"],
            json_data["Cron Schedule"],
            json.dumps(json_data["Services"])  # Convert services dict to JSON string
        ))

        conn.commit()  # Commit the changes
        st.success("Pipeline configuration saved to PostgreSQL!")

    except Exception as e:
        st.error(f"Error saving to PostgreSQL: {e}")

    finally:
        cursor.close()
        conn.close()

# Function to map service name to corresponding function
def get_service_function(service_name):
    if service_name == "S3 to DB":
        return s3_db.main_s3_db
    elif service_name == "DB to S3":
        return db_s3.main_db_s3
    elif service_name == "SFTP to S3":
        return sftp_s3.main_sftp_s3
    elif service_name == "S3 to S3":
        return s3_s3.main_s3_s3
    elif service_name == "DB to DB":
        return db_db.main_db_db
    elif service_name == "main_email":
        return email_notification.main_email

from airflow import DAG

# Function to generate a dynamic Airflow DAG from pipeline JSON
def generate_airflow_dag(pipeline_json):
    # Extract pipeline name and schedule
    dag_id = f"pipeline_{pipeline_json['Pipeline Name']}"
    cron_schedule = pipeline_json["Cron Schedule"]

    # Maintain the order of services
    service_order = list(pipeline_json["Services"].keys())

    # Generate import statements for the services
    import_statements = ""
    for service_name in service_order:
        service_function = get_service_function(service_name)  # Get the service function
        service_module_name = service_function.__module__  # Get the module name for import
        service_module_name = service_module_name.replace("scripts.", "")
        import_statements += f"from {service_module_name} import {service_function.__name__}\n"
    
    # Import the main_email function
    email_function = get_service_function("main_email")
    email_module_name = email_function.__module__
    email_module_name = email_module_name.replace("scripts.", "")
    import_statements += f"from {email_module_name} import main_email\n" 

    # Generate the DAG code string
    dag_code = f"""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import uuid

# Import service functions
{import_statements}

run_id = str(uuid.uuid4())

# Default args
default_args = {{
    'owner': 'airflow',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    schedule_interval='{cron_schedule}',
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
    op_kwargs={{
        'notification_recipients': '{pipeline_json["Pipeline Notification Recipients"]}',
        'dag_id': '{dag_id}',
        'run_id': '{{{{ dag_run.run_id }}}}'
    }},
    dag=dag
)

end_task = DummyOperator(
    task_id='End',
    dag=dag
)

"""

    # Dynamically generate the PythonOperator code for each task
    for service_num, service_name in enumerate(service_order):
        service_config = pipeline_json["Services"][service_name]
        service_config['dag_id'] = dag_id
        service_function_name = get_service_function(service_name).__name__

        task_code = f"""
{service_name.replace(' ', '_')} = PythonOperator(
    task_id='{service_name.replace(' ', '_')}',
    python_callable={service_function_name},
    op_kwargs={{**{service_config}, 'run_id': '{{{{ dag_run.run_id }}}}', 'task_order': '{service_num}'}},
    dag=dag,
)
"""
        dag_code += task_code

    # Add dependencies
    dag_code += f"start_task >> {service_order[0].replace(' ', '_')}\n"
    for i in range(len(service_order) - 1):
        dag_code += f"{service_order[i].replace(' ', '_')} >> {service_order[i + 1].replace(' ', '_')}\n"
    dag_code += f"{service_order[-1].replace(' ', '_')} >> email_notification_task >> end_task\n"

    # Path to save the DAG in the correct Airflow DAGs folder
    dag_file_path = "/opt/airflow/dags/"  # Make sure your Docker volume is mapped to this folder
    dag_file_name = os.path.join(dag_file_path, f"{dag_id}.py")

    # Write the generated DAG code to a Python file in the DAGs folder
    with open(dag_file_name, "w") as dag_file:
        dag_file.write(dag_code)

    # Return the generated DAG ID
    return dag_id


# Show the "Deploy a Pipeline" button initially
if not st.session_state.pipeline_deployed:
    if st.button("Deploy a Pipeline"):
        st.session_state.pipeline_deployed = True
        st.session_state.service_count = 1  # Start with the first service

# If the pipeline has been deployed, show the service dropdowns and widgets
if st.session_state.pipeline_deployed:
    # Pipeline name and notification recipients inputs
    col1,col2 = st.columns([2,2])
    with col1:
        st.text_input("Pipeline Name", key='pipeline_name')
    with col2:
        st.text_input("Cron Schedule (e.g. '0 0 * * *')", key='cron_schedule')
    st.text_input("Notification Recipients ( ' , ' separated )", key='pipeline_notification_recipients')
    

    # Render widgets for each service added
    for i in range(1, st.session_state.service_count + 1):
        st.subheader(f"Task {i}")
        render_service_widgets(i)

    # Only show "Add a service" if the user has selected the current service
    if st.session_state.get(f"select_service_{st.session_state.service_count}", ""):
        if st.button("Add a service"):
            st.session_state.service_count += 1  # Increment service count after the click

    # Button to deploy all services
    if st.button("Deploy Airflow DAG"):
        # Save the pipeline configuration to JSON and insert into PostgreSQL
        pipeline_json = save_pipeline_json()
        insert_pipeline_json_to_postgres(pipeline_json)

        # Call the function to generate and save the Airflow DAG
        dag_id = generate_airflow_dag(pipeline_json)
        st.success(f"Airflow DAG {dag_id} deployed successfully!")



