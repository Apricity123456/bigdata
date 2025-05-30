#!/bin/bash

# ========= Configuration =========
export AIRFLOW_HOME=~/airflow
export PATH=$PATH:$HOME/.local/bin
ELASTIC_BIN=~/elasticsearch/bin/elasticsearch         # <--- Modify this path
KIBANA_BIN=~/kibana/bin/kibana                         # <--- Modify this path

echo "========== Starting Big Data Project Services =========="

# ========= Start Elasticsearch =========
echo "[INFO] Checking if Elasticsearch is running on port 9200..."
if nc -z localhost 9200; then
    echo "[INFO] Elasticsearch is already running."
else
    echo "[INFO] Starting Elasticsearch..."
    nohup $ELASTIC_BIN > "$AIRFLOW_HOME/elasticsearch.log" 2>&1 &
    echo "[INFO] Elasticsearch started (logs in elasticsearch.log)"
    # sleep 3
fi

# ========= Start Kibana =========
echo "[INFO] Checking if Kibana is running on port 5601..."
if nc -z localhost 5601; then
    echo "[INFO] Kibana is already running."
else
    echo "[INFO] Starting Kibana..."
    nohup $KIBANA_BIN > "$AIRFLOW_HOME/kibana.log" 2>&1 &
    echo "[INFO] Kibana started (logs in kibana.log)"
    # sleep 5
fi

# ========= Start Airflow Scheduler =========
echo "[INFO] Checking if Airflow scheduler is running..."
if ps aux | grep "airflow scheduler" | grep -v grep > /dev/null; then
    echo "[INFO] Airflow scheduler is already running."
else
    echo "[INFO] Starting Airflow scheduler..."
    nohup airflow scheduler > "$AIRFLOW_HOME/scheduler.log" 2>&1 &
    echo "[INFO] Airflow scheduler started (logs in scheduler.log)"
    # sleep 2
fi

# ========= Start Airflow Web Server =========
echo "[INFO] Starting Airflow Web Server on port 8080..."
nohup airflow webserver --port 8080 > "$AIRFLOW_HOME/webserver.log" 2>&1 &
sleep 5

# ========= Open Web Pages =========
echo "[INFO] Opening web interfaces in your browser..."
xdg-open http://localhost:8080      # Airflow
xdg-open http://localhost:5601      # Kibana
xdg-open http://localhost:9200      # Elasticsearch

echo "========== All services launched successfully =========="
