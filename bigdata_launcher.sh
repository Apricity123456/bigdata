#!/bin/bash

# ========= Configuration =========
export AIRFLOW_HOME=~/airflow
export PATH=$PATH:$HOME/.local/bin
ELASTIC_BIN=~/elasticsearch/bin/elasticsearch         # 修改为你的 elasticsearch 路径
KIBANA_BIN=~/kibana/bin/kibana                         # 修改为你的 kibana 路径

echo "========== Starting Big Data Project Services =========="

# ========= Start Elasticsearch =========
echo "[INFO] Checking if Elasticsearch is running on port 9200..."
if nc -z localhost 9200; then
    echo "[INFO] Elasticsearch is already running."
else
    echo "[INFO] Starting Elasticsearch..."
    nohup $ELASTIC_BIN > "$AIRFLOW_HOME/elasticsearch.log" 2>&1 &
    echo "[INFO] Elasticsearch started in background (logs in elasticsearch.log)"
fi

# ========= Start Kibana =========
echo "[INFO] Checking if Kibana is running on port 5601..."
if nc -z localhost 5601; then
    echo "[INFO] Kibana is already running."
else
    echo "[INFO] Starting Kibana..."
    nohup $KIBANA_BIN > "$AIRFLOW_HOME/kibana.log" 2>&1 &
    echo "[INFO] Kibana started in background (logs in kibana.log)"
fi

# ========= Start Airflow Scheduler =========
echo "[INFO] Checking if Airflow scheduler is already running..."
if ps aux | grep "airflow scheduler" | grep -v grep > /dev/null; then
    echo "[INFO] Airflow scheduler is already running."
else
    echo "[INFO] Starting Airflow scheduler..."
    nohup airflow scheduler > "$AIRFLOW_HOME/scheduler.log" 2>&1 &
    echo "[INFO] Scheduler started successfully (logs in scheduler.log)"
fi

# ========= Start Airflow Web Server =========
echo "[INFO] Starting Airflow Web Server on port 8080..."
airflow webserver --port 8080


