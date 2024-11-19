FROM python:3.9-slim

# Install Java and other dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    build-essential \
    libpq-dev \
    netcat-traditional \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set Airflow home
ENV AIRFLOW_HOME=/opt/airflow

WORKDIR $AIRFLOW_HOME

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Streamlit
RUN pip install --no-cache-dir streamlit

# Download necessary JARs
RUN mkdir -p $AIRFLOW_HOME/jars && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $AIRFLOW_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -P $AIRFLOW_HOME/jars/ && \
    wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -P $AIRFLOW_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -P $AIRFLOW_HOME/jars/



# Copy the start script
COPY start_airflow.sh .
RUN chmod +x start_airflow.sh

# Copy DAGs, scripts, and tests
COPY dags/ $AIRFLOW_HOME/dags/
COPY scripts/ $AIRFLOW_HOME/scripts/
COPY tests/ $AIRFLOW_HOME/tests/

# Copy Streamlit app
COPY streamlit_app.py $AIRFLOW_HOME/

# Expose the ports for Airflow and Streamlit
EXPOSE 8080 8888

# Run Airflow webserver and scheduler
CMD ["./start_airflow.sh"]