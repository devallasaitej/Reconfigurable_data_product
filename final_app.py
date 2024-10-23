import streamlit as st
import json
import os
import psycopg2
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="Data Shift Shuttle",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed"
)
# Custom CSS for styling the card and dropdown
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

/* Card style */
.card {
    background-color: white;
    padding: 20px;
    border-radius: 10px;
    box-shadow: 0 0 10px rgba(0, 0, 0, 0.3);
    margin-top: 10px;
}

/* Title style */
.card h2 {
    font-size: 24px; /* Reduced size */
    text-align: center; /* Centered title */
}

/* Subtitle style */
.card p {
    font-size: 14px; /* Reduced size */
}

/* Widget labels */
.label {
    font-size: 12px; /* Smaller size for labels */
    margin-bottom: 4px; /* Space below label */
    display: block; /* Make labels block elements */
}
</style>
""", unsafe_allow_html=True)

# Title
st.markdown("<h1 style='text-align: center;margin-top: -70px;margin-bottom: 30px;'>🚀 Data Shift Shuttle</h1>", unsafe_allow_html=True)

# Updated card content for all operations
card_content = {
    "S3 - DB": """
    """,
    "DB - S3": """
    """,
    "S3 - S3": """
    """,
    "SFTP - S3": """
    """,
    "DB - DB": """
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
            </div>
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
            st.markdown("<span class='label'>Source_S3_Access</span>", unsafe_allow_html=True)
            st.text_input(f"Source_S3_Access_{service_num}", key=f"source_s3_access_{service_num}")

        with col2:
            st.markdown("<span class='label'>Source_S3_File_Path</span>", unsafe_allow_html=True)
            st.text_input(f"Source_S3_File_Path_{service_num}", key=f"source_s3_file_path_{service_num}")

        with col3:
            st.markdown("<span class='label'>Header_Row</span>", unsafe_allow_html=True)
            st.selectbox(f"Header_Row_{service_num}", ["True", "False"], key=f"header_row_{service_num}")

        with col4:
            st.markdown("<span class='label'>File_Delimiter</span>", unsafe_allow_html=True)
            st.text_input(f"File_Delimiter_{service_num}", key=f"file_delimiter_{service_num}")

        col5, col6, col7, col8 = st.columns([1, 1, 1, 1])  # Second row

        with col5:
            st.markdown("<span class='label'>Load_Type</span>", unsafe_allow_html=True)
            st.selectbox(f"Load_Type_{service_num}", ["Append", "Overwrite"], key=f"load_type_{service_num}")        

        with col6:
            st.markdown("<span class='label'>Target_DB</span>", unsafe_allow_html=True)
            st.selectbox(f"Target_DB_{service_num}", ["MySQL", "PSQL"], key=f"target_db_{service_num}")

        with col7:
            st.markdown("<span class='label'>Target_DB_Access</span>", unsafe_allow_html=True)
            st.text_input(f"Target_DB_Access_{service_num}", key=f"target_db_access_{service_num}")

        with col8:
            st.markdown("<span class='label'>Target_DB_Table</span>", unsafe_allow_html=True)
            st.text_input(f"Target_DB_Table_{service_num}", key=f"target_db_table_{service_num}")


    # Widgets for "DB to S3" (similar logic for other services)
    if service == "DB to S3":
        col1, col2, col3, col4 = st.columns([1, 1, 1, 2])

        with col1:
            st.markdown("<span class='label'>Source_DB</span>", unsafe_allow_html=True)
            st.selectbox(f"Source_DB_{service_num}", ["MySQL", "PSQL"], key=f"source_db_{service_num}")

        with col2:
            st.markdown("<span class='label'>Source_DB_Access</span>", unsafe_allow_html=True)
            st.text_input(f"Source_DB_Access_{service_num}", key=f"source_db_access_{service_num}")

        with col3:
            st.markdown("<span class='label'>Source_DB_Table</span>", unsafe_allow_html=True)
            st.text_input(f"Source_DB_Table_{service_num}", key=f"source_db_table_{service_num}")

        with col4:
            st.markdown("<span class='label'>DML</span>", unsafe_allow_html=True)
            st.text_input(f"DML_{service_num}", key=f"dml_{service_num}")

        col5, col6, col7 = st.columns([1, 3, 1])  # Second row

        with col5:
            st.markdown("<span class='label'>Target_S3_Access</span>", unsafe_allow_html=True)
            st.text_input(f"Target_S3_Access_{service_num}", key=f"target_s3_access_{service_num}")

        with col6:
            st.markdown("<span class='label'>Target_S3_File_Path</span>", unsafe_allow_html=True)
            st.text_input(f"Target_S3_File_Path_{service_num}", key=f"target_s3_file_path_{service_num}")

        with col7:
            st.markdown("<span class='label'>Target_File_Delimiter</span>", unsafe_allow_html=True)
            st.text_input(f"Target_File_Delimiter_{service_num}", key=f"target_file_delimiter_{service_num}")


    if service == "S3 to S3":
        col1, col2, col3, col4 = st.columns([1,2,1,1])

        # First row of widgets
        with col1:
            st.markdown("<span class='label'>Source_S3_Access</span>", unsafe_allow_html=True)
            source_file_path = st.text_input(f"Source_S3_Access_{service_num}", key=f"source_s3_access_{service_num}")

        with col2:
            st.markdown("<span class='label'>Source_S3_File_Path</span>", unsafe_allow_html=True)
            source_file_path = st.text_input(f"Source_S3_File_Path_{service_num}", key=f"source_s3_file_path_{service_num}")

        with col3:
            st.markdown("<span class='label'>Target_S3_Access</span>", unsafe_allow_html=True)
            file_delimiter = st.text_input(f"Target_S3_Access_{service_num}", key=f"target_s3_access_{service_num}")

        with col4:
            st.markdown("<span class='label'>Target_S3_File_Path</span>", unsafe_allow_html=True)
            file_delimiter = st.text_input(f"Target_S3_File_Path_{service_num}", key=f"target_s3_file_path_{service_num}")


    if service == "SFTP to S3":
        col1, col2, col3, col4 = st.columns([1,2,1,2])

        # First row of widgets
        with col1:
            st.markdown("<span class='label'>Source_SFTP_Access</span>", unsafe_allow_html=True)
            source_file_path = st.text_input(f"Source_SFTP_Access_{service_num}", key=f"source_sftp_access_{service_num}")

        with col2:
            st.markdown("<span class='label'>Source_SFTP_File_Path</span>", unsafe_allow_html=True)
            source_file_path = st.text_input(f"Source_SFTP_File_Path_{service_num}", key=f"source_sftp_file_path_{service_num}")

        with col3:
            st.markdown("<span class='label'>Target_S3_Access</span>", unsafe_allow_html=True)
            file_delimiter = st.text_input(f"Target_S3_Access_{service_num}", key=f"target_s3_access_{service_num}")

        with col4:
            st.markdown("<span class='label'>Target_S3_File_Path</span>", unsafe_allow_html=True)
            file_delimiter = st.text_input(f"Target_S3_File_Path_{service_num}", key=f"target_s3_file_path_{service_num}")

    if service == "DB to DB":
        col1, col2, col3, col4 = st.columns([1,2,1,2])

        # First row of widgets
        with col1:
            st.markdown("<span class='label'>Source_DB</span>", unsafe_allow_html=True)
            target_db = st.selectbox(f"Source_DB_{service_num}", ["MySQL", "PSQL"], key=f"source_db_{service_num}")

        with col2:
            st.markdown("<span class='label'>Source_DB_Access</span>", unsafe_allow_html=True)
            source_file_path = st.text_input(f"Source_DB_Access_{service_num}", key=f"source_db_access_{service_num}")

        with col3:
            st.markdown("<span class='label'>Source_DB_Table</span>", unsafe_allow_html=True)
            source_file_path = st.text_input(f"Source_DB_Table_{service_num}", key=f"source_db_table_{service_num}")

        with col4:
            st.markdown("<span class='label'>DML</span>", unsafe_allow_html=True)
            file_delimiter = st.text_input(f"DML_{service_num}", key=f"dml_{service_num}")

        # Second row of widgets
        col5, col6, col7, col8 = st.columns([1, 1, 1, 1])  # Extend Notification_Recipients column

        with col5:
            st.markdown("<span class='label'>Target_DB</span>", unsafe_allow_html=True)
            target_db = st.selectbox(f"Target_DB_{service_num}", ["MySQL", "PSQL"], key=f"target_db_{service_num}")

        with col6:
            st.markdown("<span class='label'>Target_DB_Access</span>", unsafe_allow_html=True)
            target_db_access = st.text_input(f"Target_DB_Access_{service_num}", key=f"target_db_access_{service_num}")

        with col7:
            st.markdown("<span class='label'>Target_DB_Table</span>", unsafe_allow_html=True)
            target_db_table = st.text_input(f"Target_DB_Table_{service_num}", key=f"target_db_table_{service_num}")

        with col8:
            st.markdown("<span class='label'>Load_Type</span>", unsafe_allow_html=True)
            target_db = st.selectbox(f"Load_Type_{service_num}", ["Append", "Overwrite"], key=f"load_type_{service_num}")

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

# Show the "Deploy a Pipeline" button initially
if not st.session_state.pipeline_deployed:
    if st.button("Deploy a Pipeline"):
        st.session_state.pipeline_deployed = True
        st.session_state.service_count = 1  # Start with the first service

# If the pipeline has been deployed, show the service dropdowns and widgets
if st.session_state.pipeline_deployed:
    # Pipeline name and notification recipients inputs
    st.text_input("Pipeline Name", key='pipeline_name')
    st.text_input("Notification Recipients", key='pipeline_notification_recipients')
    st.text_input("Cron Schedule (e.g. '0 0 * * *')", key='cron_schedule')

    # Render widgets for each service added
    for i in range(1, st.session_state.service_count + 1):
        st.subheader(f"Service {i}")
        render_service_widgets(i)

    # Only show "Add a service" if the user has selected the current service
    if st.session_state.get(f"select_service_{st.session_state.service_count}", ""):
        if st.button("Add a service"):
            st.session_state.service_count += 1  # Increment service count after the click

    # Button to deploy all services
    if st.button("Deploy All Services"):
        st.write("Deploying the following services:")
        for i in range(1, st.session_state.service_count + 1):
            service_name = st.session_state.get(f"select_service_{i}", "No service selected")
            st.write(f"Service {i}: {service_name}")

        # Save the pipeline configuration to JSON and insert into PostgreSQL
        pipeline_json = save_pipeline_json()
        insert_pipeline_json_to_postgres(pipeline_json)

####

# Function to generate the Airflow DAG dynamically
def generate_airflow_dag(pipeline_json):
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime

    # Define the DAG
    dag = DAG(
        dag_id=pipeline_json["Pipeline Name"].replace(" ", "_").lower(),
        schedule_interval=pipeline_json["Cron Schedule"],
        start_date=datetime(2024, 1, 1),  # Change to your preferred start date
        catchup=False
    )

    # Create tasks based on the services in the pipeline
    tasks = {}
    for service_name, service_config in pipeline_json["Services"].items():
        # Define a task for each service using a specific Python script
        task_id = service_name.replace(" ", "_").lower()
        tasks[task_id] = PythonOperator(
            task_id=task_id,
            python_callable=globals().get(f"run_{task_id}"),  # Assumes function exists for each service
            op_kwargs=service_config,  # Pass service config as kwargs to the callable
            dag=dag,
        )

    # Set task dependencies based on order of service creation
    for i in range(1, len(tasks)):
        tasks[list(tasks.keys())[i - 1]] >> tasks[list(tasks.keys())[i]]  # Sequential execution

    # Return the generated DAG
    return dag

# Function to save the generated DAG to a file
def save_dag(dag):
    dag_file_path = os.path.join('/path/to/your/airflow/dags', f"{dag.dag_id}.py")  # Change to your Airflow DAGs directory
    
    with open(dag_file_path, 'w') as dag_file:
        dag_file.write(f"""from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def run_{dag.dag_id}(**kwargs):
    # Call your service Python script here
    pass

dag = DAG(
    dag_id='{dag.dag_id}',
    schedule_interval='{dag.schedule_interval}',
    start_date=datetime(2024, 1, 1),  # Change to your preferred start date
    catchup=False
)

""")
        for task in dag.tasks:
            dag_file.write(f"{task.task_id} = PythonOperator(\n")
            dag_file.write(f"    task_id='{task.task_id}',\n")
            dag_file.write(f"    python_callable=run_{task.task_id},\n")
            dag_file.write("    op_kwargs={})\n\n")

    st.success(f"DAG {dag.dag_id} saved successfully!")


# Button to deploy Airflow DAG
if st.button("Deploy Airflow DAG"):
    pipeline_json = save_pipeline_json()
    insert_pipeline_json_to_postgres(pipeline_json)

    # Generate the Airflow DAG
    dag = generate_airflow_dag(pipeline_json)

    # Save the generated DAG
    save_dag(dag)