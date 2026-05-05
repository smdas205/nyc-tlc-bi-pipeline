import os
import sys
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.task_group import TaskGroup

from airflow import DAG

parent_path = os.path.abspath("/opt/airflow/")

if parent_path not in sys.path:
    sys.path.append(parent_path)

from spark_jobs.ingest_tlc import download

with DAG(
    dag_id="ingest_tlc_files",
    start_date=datetime(2022, 1, 1),
    schedule="@monthly",
    max_active_runs=1,
    catchup=False,
) as dag:

    def download_yellow(**context):
        hook = RedisHook(redis_conn_id="redis_default")
        r = hook.get_conn()
        execution_date = context["logical_date"]
        year = execution_date.year
        month = execution_date.strftime("%m")
        label = execution_date.strftime("%Y-%m")
        download(r, "yellow", year, month, label)

    def download_green(**context):
        hook = RedisHook(redis_conn_id="redis_default")
        r = hook.get_conn()
        execution_date = context["logical_date"]
        year = execution_date.year
        month = execution_date.strftime("%m")
        label = execution_date.strftime("%Y-%m")
        download(r, "green", year, month, label)

    def download_fhv(**context):
        hook = RedisHook(redis_conn_id="redis_default")
        r = hook.get_conn()
        execution_date = context["logical_date"]
        year = execution_date.year
        month = execution_date.strftime("%m")
        label = execution_date.strftime("%Y-%m")
        download(r, "fhv", year, month, label)

    def download_fhvhv(**context):
        hook = RedisHook(redis_conn_id="redis_default")
        r = hook.get_conn()
        execution_date = context["logical_date"]
        year = execution_date.year
        month = execution_date.strftime("%m")
        label = execution_date.strftime("%Y-%m")
        download(r, "fhvhv", year, month, label)

    with TaskGroup("yellow") as tg_yellow:
        PythonOperator(task_id="download_yellow", python_callable=download_yellow)
    with TaskGroup("green") as tg_green:
        PythonOperator(task_id="download_green", python_callable=download_green)
    with TaskGroup("fhv") as tg_fhv:
        PythonOperator(task_id="download_fhv", python_callable=download_fhv)
    with TaskGroup("fhvhv") as tg_fhvhv:
        PythonOperator(task_id="download_fhvhv", python_callable=download_fhvhv)

    tg_yellow >> tg_green >> tg_fhv >> tg_fhvhv
