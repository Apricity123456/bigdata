from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dag_utils import script_cmd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'als_movie_pipeline',
    default_args=default_args,
    description='Pipeline with ALS-based movie recommendation',
    schedule_interval=None,
    catchup=False,
)

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

als_train = BashOperator(
    task_id='als_recommend',
    bash_command=script_cmd('als_recommend.py'),
    dag=dag,
)

index_als = BashOperator(
    task_id='index_als',
    bash_command=script_cmd('index_als.py'),
    dag=dag,
)

ingest_imdb >> format_imdb
ingest_netflix >> format_netflix
[format_imdb, format_netflix] >> als_train
als_train >> index_als
