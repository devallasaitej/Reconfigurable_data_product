# Data Shift Shuttle
Data Shift Shuttle is a versatile, reconfigurable data transfer tool designed to automate and streamline data movement between various environments, such as AWS S3, relational databases, and SFTP servers. Originally built using Databricks as a solution to reduce the repetitive tasks associated with manual data pipeline creation, Data Shift Shuttle leverages open-source technologies like Streamlit and Airflow to provide an intuitive user interface and automated orchestration for seamless data transfers.

With Data Shift Shuttle, users can dynamically configure data pipelines, define source and target environments, and deploy pipelines without the need for manual DAG authoring in Airflow. This tool enhances both efficiency and governance by automatically logging all data movements, tracking records processed, and providing comprehensive email notifications upon completion of each pipeline run.

## Key Features
* User-Friendly UI: Configure and stack services through a simple Streamlit interface.
* Dynamic DAG Generation: Automatically generate Airflow DAGs based on user input without manual coding.
* Comprehensive Logging: Track every aspect of the data pipeline, including file names, records processed, and task status.
* Email Notifications: Receive detailed email notifications on pipeline execution, including success/failure and transfer details.
* Enhanced Governance: Maintain a clear audit trail of data transfers, improving compliance and transparency.

## Architecture
<img width="808" alt="image" src="https://github.com/user-attachments/assets/23efe0be-225b-44b9-987e-50d24d2aae02">

## Front-end UI
<img width="1456" alt="image" src="https://github.com/user-attachments/assets/2768f59f-badc-4de2-a2b8-91b4f1b22783">
<img width="1387" alt="image" src="https://github.com/user-attachments/assets/f0f21115-ff14-46bf-aabf-2498a5d78acc">


