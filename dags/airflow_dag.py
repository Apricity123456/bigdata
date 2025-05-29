from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'movie_data_pipeline',
    default_args=default_args,
    description='Pipeline to process and index movie data from IMDb and Netflix',
    schedule_interval=None,
    catchup=False,
)

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

combine = BashOperator(
    task_id='combine_recommend',
    bash_command='python3 /home/lucas/work/bigdata/scripts/combine_recommend.py',
    dag=dag,
)

index_elastic = BashOperator(
    task_id='index_elastic',
    bash_command='python3 /home/lucas/work/bigdata/scripts/index_elastic.py',
    dag=dag,
)

ingest_imdb >> format_imdb
ingest_netflix >> format_netflix
[format_imdb, format_netflix] >> combine
combine >> index_elastic
