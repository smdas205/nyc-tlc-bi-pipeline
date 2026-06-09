import os
import sys

import pendulum
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

parent_path = os.path.abspath("/opt/airflow/")

if parent_path not in sys.path:
    sys.path.append(parent_path)

from spark.jobs.ingest_tlc import download

# DAG to download files from NYC TLC server
with DAG(
    dag_id="ingest_tlc_files",
    start_date=pendulum.datetime(2022, 1, 1, tz="America/New_York"),
    end_date=pendulum.datetime(2023, 12, 31, tz="America/New York"),
    schedule="@monthly",
    max_active_runs=1,
    catchup=False,
) as dag:
    """ Functions for different taxi types.
    Each function calls the script with its own taxi type. """

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

    # Task Groups for all types
    with TaskGroup("yellow") as tg_yellow:
        PythonOperator(task_id="download_yellow", python_callable=download_yellow)
    with TaskGroup("green") as tg_green:
        PythonOperator(task_id="download_green", python_callable=download_green)
    with TaskGroup("fhv") as tg_fhv:
        PythonOperator(task_id="download_fhv", python_callable=download_fhv)
    with TaskGroup("fhvhv") as tg_fhvhv:
        PythonOperator(task_id="download_fhvhv", python_callable=download_fhvhv)

    # Order of DAG
    tg_yellow >> tg_green >> tg_fhv >> tg_fhvhv
