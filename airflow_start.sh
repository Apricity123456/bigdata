#!/bin/bash

# 设置 AIRFLOW_HOME 路径（可根据实际路径修改）
export AIRFLOW_HOME=~/airflow

# 创建并激活虚拟环境（如果需要）
# python3 -m venv venv
# source venv/bin/activate

# 安装依赖（确保 requirements.txt 存在）
# pip install -r requirements.txt

# 初始化数据库
airflow db init

# 创建默认用户（只首次执行需要）
airflow users create \
    --username admin \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com \
    --password admin

# 启动调度器（后台运行）
airflow scheduler &

# 启动 Web UI（默认端口 8080）
airflow webserver --port 8080
