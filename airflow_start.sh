#!/bin/bash

echo " Starting Airflow services..."

# Activate the virtual environment
source airflow_env/bin/activate

# Start scheduler in background
echo " Starting Airflow Scheduler..."
airflow scheduler > scheduler.log 2>&1 &

# Start webserver in background on port 8080
echo " Starting Airflow Webserver on http://localhost:8080 ..."
airflow webserver --port 8080 > webserver.log 2>&1 &

echo " Airflow is starting. Check scheduler.log and webserver.log for logs."
