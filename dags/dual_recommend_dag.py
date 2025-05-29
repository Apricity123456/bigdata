from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dual_recommend_pipeline',
    default_args=default_args,
    description='Run both ALS and heuristic recommendation pipelines in parallel',
    schedule_interval=None,
    catchup=False,
)

# Ingestion tasks
ingest_imdb = BashOperator(
    task_id='ingest_imdb',
    bash_command='python3 /home/lucas/work/bigdata/scripts/ingest_imdb_api.py',
    dag=dag,
)

ingest_netflix = BashOperator(
    task_id='ingest_netflix',
    bash_command='python3 /home/lucas/work/bigdata/scripts/ingest_netflix_api.py',
    dag=dag,
)

# Formatting tasks
format_imdb = BashOperator(
    task_id='format_imdb',
    bash_command='python3 /home/lucas/work/bigdata/scripts/format_imdb.py',
    dag=dag,
)

format_netflix = BashOperator(
    task_id='format_netflix',
    bash_command='python3 /home/lucas/work/bigdata/scripts/format_netflix.py',
    dag=dag,
)

# ALS recommendation + indexing
als_recommend = BashOperator(
    task_id='als_recommend',
    bash_command='python3 /home/lucas/work/bigdata/scripts/als_recommend.py',
    dag=dag,
)

index_als = BashOperator(
    task_id='index_als',
    bash_command='python3 /home/lucas/work/bigdata/scripts/index_als.py',
    dag=dag,
)

# Heuristic recommendation + indexing
combine_recommend = BashOperator(
    task_id='combine_recommend',
    bash_command='python3 /home/lucas/work/bigdata/scripts/combine_recommend.py',
    dag=dag,
)

index_elastic = BashOperator(
    task_id='index_elastic',
    bash_command='python3 /home/lucas/work/bigdata/scripts/index_elastic.py',
    dag=dag,
)

# Define task dependencies
ingest_imdb >> format_imdb
ingest_netflix >> format_netflix

[format_imdb, format_netflix] >> als_recommend
[format_imdb, format_netflix] >> combine_recommend

als_recommend >> index_als
combine_recommend >> index_elastic
