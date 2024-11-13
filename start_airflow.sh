#!/bin/bash
set -e

# Function to check if PostgreSQL is ready
postgres_ready() {
  python << END
import sys
import psycopg2
try:
    psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres"
    )
except psycopg2.OperationalError:
    sys.exit(-1)
sys.exit(0)
END
}

# Wait for PostgreSQL to be ready
until postgres_ready; do
  >&2 echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

>&2 echo "PostgreSQL is up - executing command"

# Initialize the database
airflow db init

# Create a user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# # Create the results directory if it doesn't exist
# mkdir -p /opt/airflow/tests/results

# # Run tests
# timestamp=$(date +"%Y%m%d_%H%M%S")
# pytest /opt/airflow/tests > /opt/airflow/tests/results/test_results_$timestamp.xml 2>&1 || echo "Warning: Some tests failed, but continuing startup."

# # Print the contents of the test results file
# echo "Test Results:"
# cat /opt/airflow/tests/results/test_results_$timestamp.xml

# Start Airflow
airflow scheduler &
exec airflow webserver