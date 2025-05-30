#!/bin/bash

# ========= Configuration =========
# Set your Airflow home directory (modify if needed)
export AIRFLOW_HOME=~/airflow
export PATH=$PATH:$HOME/.local/bin

# ========= Check if scheduler is already running =========
echo "[INFO] Checking if Airflow scheduler is already running..."

SCHEDULER_RUNNING=$(ps aux | grep "airflow scheduler" | grep -v grep)

if [ -z "$SCHEDULER_RUNNING" ]; then
    echo "[INFO] Scheduler not found. Starting it now..."
    nohup airflow scheduler > "$AIRFLOW_HOME/scheduler.log" 2>&1 &
    echo "[INFO] Scheduler started successfully (running in background, logs in scheduler.log)"
else
    echo "[INFO] Scheduler is already running. No need to start again."
fi

# ========= Start Airflow Web Server =========
echo "[INFO] Starting Airflow Web Server on port 8080..."
airflow webserver --port 8080
