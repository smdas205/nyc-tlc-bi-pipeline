from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

SCRIPT_PATH = "/opt/airflow/spark_jobs"
INPUT_PATH = "/opt/spark/data/raw"
TEMP_PATH = "/opt/spark/data/tmp"
# TEMP_PATH = "/opt/airflow/data/tmp"
OUTPUT_PATH = "/opt/spark/data/processed"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="process_tlc_files",
    default_args=default_args,
    schedule_interval="@monthly",
    max_active_runs=1,
    max_active_tasks=1,
    catchup=True,
) as dag:
    # ── Yellow ────────────────────────────────────────────────────────────────
    with TaskGroup("yellow") as tg_yellow:
        clean_yellow = SparkSubmitOperator(
            task_id="clean_yellow",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/clean_trips.py",
            application_args=[
                "yellow",
                INPUT_PATH,
                "{{ logical_date.strftime('%Y-%m') }}",
                TEMP_PATH,
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        enrich_yellow = SparkSubmitOperator(
            task_id="enrich_yellow",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/enrich_trips.py",
            application_args=[
                "yellow",
                TEMP_PATH,
                OUTPUT_PATH + "/yellow/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        validate_yellow = SparkSubmitOperator(
            task_id="validate_yellow",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/validate_trips.py",
            application_args=[
                "yellow",
                OUTPUT_PATH + "/yellow/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        cleanup_yellow = BashOperator(
            task_id="cleanup_yellow",
            bash_command="rm -rf " + TEMP_PATH,
            trigger_rule="all_success",
        )
        clean_yellow >> enrich_yellow >> validate_yellow >> cleanup_yellow

    # ── Green ─────────────────────────────────────────────────────────────────
    with TaskGroup("green") as tg_green:
        clean_green = SparkSubmitOperator(
            task_id="clean_green",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/clean_trips.py",
            application_args=[
                "green",
                INPUT_PATH,
                "{{ logical_date.strftime('%Y-%m') }}",
                TEMP_PATH,
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        enrich_green = SparkSubmitOperator(
            task_id="enrich_green",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/enrich_trips.py",
            application_args=[
                "green",
                TEMP_PATH,
                OUTPUT_PATH + "/green/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        validate_green = SparkSubmitOperator(
            task_id="validate_green",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/validate_trips.py",
            application_args=[
                "green",
                OUTPUT_PATH + "/green/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        cleanup_green = BashOperator(
            task_id="cleanup_green",
            bash_command="rm -rf " + TEMP_PATH,
            trigger_rule="all_success",
        )
        clean_green >> enrich_green >> validate_green >> cleanup_green

    # ── FHV ───────────────────────────────────────────────────────────────────
    with TaskGroup("fhv") as tg_fhv:
        clean_fhv = SparkSubmitOperator(
            task_id="clean_fhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/clean_trips.py",
            application_args=[
                "fhv",
                INPUT_PATH,
                "{{ logical_date.strftime('%Y-%m') }}",
                TEMP_PATH,
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        enrich_fhv = SparkSubmitOperator(
            task_id="enrich_fhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/enrich_trips.py",
            application_args=[
                "fhv",
                TEMP_PATH,
                OUTPUT_PATH + "/fhv/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        validate_fhv = SparkSubmitOperator(
            task_id="validate_fhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/validate_trips.py",
            application_args=[
                "fhv",
                OUTPUT_PATH + "/fhv/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={"spark.hadoop.fs.permissions.umask-mode": "000"},
        )
        cleanup_fhv = BashOperator(
            task_id="cleanup_fhv",
            bash_command="rm -rf " + TEMP_PATH,
            trigger_rule="all_success",
        )
        clean_fhv >> enrich_fhv >> validate_fhv >> cleanup_fhv

    # ── HVFHV ─────────────────────────────────────────────────────────────────
    with TaskGroup("fhvhv") as tg_fhvhv:
        clean_hvfhv = SparkSubmitOperator(
            task_id="clean_fhvhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/clean_trips.py",
            application_args=[
                "fhvhv",
                INPUT_PATH,
                "{{ logical_date.strftime('%Y-%m') }}",
                TEMP_PATH,
            ],
            conf={
                "spark.executor.memory": "1900m",
                "spark.hadoop.fs.permissions.umask-mode": "000",
            },
        )
        enrich_fhvhv = SparkSubmitOperator(
            task_id="enrich_fhvhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/enrich_trips.py",
            application_args=[
                "fhvhv",
                TEMP_PATH,
                OUTPUT_PATH + "/fhvhv/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={
                "spark.executor.memory": "1900m",
                "spark.hadoop.fs.permissions.umask-mode": "000",
            },
        )
        validate_fhvhv = SparkSubmitOperator(
            task_id="validate_fhvhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/validate_trips.py",
            application_args=[
                "fhvhv",
                OUTPUT_PATH + "/fhvhv/{{ logical_date.strftime('%Y-%m') }}",
            ],
            conf={
                "spark.executor.memory": "1900m",
                "spark.hadoop.fs.permissions.umask-mode": "000",
            },
        )
        cleanup_fhvhv = BashOperator(
            task_id="cleanup_fhvhv",
            bash_command="rm -rf " + TEMP_PATH,
            trigger_rule="all_success",
        )
        clean_hvfhv >> enrich_fhvhv >> validate_fhvhv >> cleanup_fhvhv

    # ── Sequential group execution ─────────────────────────────────────────────
    tg_yellow >> tg_green >> tg_fhv >> tg_fhvhv
