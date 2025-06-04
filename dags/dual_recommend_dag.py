from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# 默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# 定义 DAG
dag = DAG(
    'dual_recommend_pipeline',
    default_args=default_args,
    description='Run both ALS and heuristic recommendation pipelines in parallel',
    schedule_interval=None,
    catchup=False,
)

# 脚本路径函数（统一路径管理）
scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
def script_cmd(script_name):
    return f'python3 {os.path.join(scripts_dir, script_name)}'

# === 任务定义 ===
ingest_imdb = BashOperator(
    task_id='ingest_imdb',
    bash_command=script_cmd('ingest_imdb_api.py'),
    dag=dag,
)

ingest_netflix = BashOperator(
    task_id='ingest_netflix',
    bash_command=script_cmd('ingest_netflix_api.py'),
    dag=dag,
)

format_imdb = BashOperator(
    task_id='format_imdb',
    bash_command=script_cmd('format_imdb.py'),
    dag=dag,
)

format_netflix = BashOperator(
    task_id='format_netflix',
    bash_command=script_cmd('format_netflix.py'),
    dag=dag,
)

als_recommend = BashOperator(
    task_id='als_recommend',
    bash_command=script_cmd('als_recommend.py'),
    dag=dag,
)

index_als = BashOperator(
    task_id='index_als',
    bash_command=script_cmd('index_als.py'),
    dag=dag,
)

combine_recommend = BashOperator(
    task_id='combine_recommend',
    bash_command=script_cmd('combine_recommend.py'),
    dag=dag,
)

index_elastic = BashOperator(
    task_id='index_elastic',
    bash_command=script_cmd('index_elastic.py'),
    dag=dag,
)

# === 依赖关系设置 ===
ingest_imdb >> format_imdb
ingest_netflix >> format_netflix

[format_imdb, format_netflix] >> als_recommend
[format_imdb, format_netflix] >> combine_recommend

als_recommend >> index_als
combine_recommend >> index_elastic
