from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 默认参数配置
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# 定义 DAG
dag = DAG(
    'als_movie_pipeline',
    default_args=default_args,
    description='Pipeline with ALS-based movie recommendation',
    schedule_interval=None,
    catchup=False,
)

# 任务 1：抓取 IMDb 数据
ingest_imdb = BashOperator(
    task_id='ingest_imdb',
    bash_command='python3 /home/lucas/work/bigdata/scripts/ingest_imdb_api.py',
    dag=dag,
)

# 任务 2：抓取 Netflix 数据
ingest_netflix = BashOperator(
    task_id='ingest_netflix',
    bash_command='python3 /home/lucas/work/bigdata/scripts/ingest_netflix_api.py',
    dag=dag,
)

# 任务 3：格式化 IMDb 数据
format_imdb = BashOperator(
    task_id='format_imdb',
    bash_command='python3 /home/lucas/work/bigdata/scripts/format_imdb.py',
    dag=dag,
)

# 任务 4：格式化 Netflix 数据
format_netflix = BashOperator(
    task_id='format_netflix',
    bash_command='python3 /home/lucas/work/bigdata/scripts/format_netflix.py',
    dag=dag,
)

# 任务 5：ALS 推荐训练
als_train = BashOperator(
    task_id='als_recommend',
    bash_command='python3 /home/lucas/work/bigdata/scripts/als_recommend.py',
    dag=dag,
)

# 任务 6：索引 ALS 推荐结果到 Elasticsearch
index_als = BashOperator(
    task_id='index_als',
    bash_command='python3 /home/lucas/work/bigdata/scripts/index_als.py',
    dag=dag,
)

# 设置依赖关系
ingest_imdb >> format_imdb
ingest_netflix >> format_netflix
[format_imdb, format_netflix] >> als_train
als_train >> index_als
