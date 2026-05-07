import logging
import os
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.task_group import TaskGroup

from airflow import DAG

SCRIPT_PATH = "/opt/airflow/spark_jobs"
INPUT_PATH = "/opt/spark/data/raw"
TEMP_PATH = "/opt/spark/data/tmp"
OUTPUT_PATH = "/opt/spark/data/processed"
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
    "retries": 1,
}


# Function to check if files were actually downloaded using Redis
def check_redis_download(trip_type: str, **context) -> bool:
    redis_date = context["logical_date"]
    redis_year = redis_date.year
    redis_month = redis_date.strftime("%m")

    hook = RedisHook(redis_conn_id="redis_default")
    r = hook.get_conn()

    redis_hash = "tlc:file_status"
    redis_key = f"{trip_type}:{redis_year}:{redis_month}"
    redis_value = r.hget(redis_hash, redis_key)

    return redis_value is not None and redis_value.decode("utf8") == "downloaded"


# Function to check if there are already processed files
# If processed files already present, whole task group of that type is skipped
def processing_query(trip_type: str, output_path: str, **context) -> bool:
    working_date = context["logical_date"].strftime("%Y-%m")
    success_path = os.path.join(output_path, trip_type, working_date, "_SUCCESS")
    if os.path.exists(success_path):
        logger.info(
            f"{success_path} already exists: Skipping {trip_type}/{working_date}."
        )
        return False
    logger.info(f"{success_path} does not exist: Processing {trip_type}/{working_date}")
    return True


with DAG(
    dag_id="process_tlc_files",
    default_args=default_args,
    schedule_interval="@monthly",
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
) as dag:
    # Yellow Task Group
    with TaskGroup("yellow") as tg_yellow:
        download_check = ShortCircuitOperator(
            task_id="download_check",
            python_callable=check_redis_download,
            op_kwargs={"trip_type": "yellow"},
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
        )
        check_yellow = ShortCircuitOperator(
            task_id="check_yellow",
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
            python_callable=processing_query,
            op_kwargs={"trip_type": "yellow", "output_path": OUTPUT_PATH},
        )
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
        (
            download_check
            >> check_yellow
            >> clean_yellow
            >> enrich_yellow
            >> validate_yellow
            >> cleanup_yellow
        )

    # Green Task Group
    with TaskGroup("green") as tg_green:
        download_check = ShortCircuitOperator(
            task_id="download_check",
            python_callable=check_redis_download,
            op_kwargs={"trip_type": "green"},
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
        )
        check_green = ShortCircuitOperator(
            task_id="check_green",
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
            python_callable=processing_query,
            op_kwargs={"trip_type": "green", "output_path": OUTPUT_PATH},
        )
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
        (
            download_check
            >> check_green
            >> clean_green
            >> enrich_green
            >> validate_green
            >> cleanup_green
        )

    # For Hire Vehicles Task Group
    with TaskGroup("fhv") as tg_fhv:
        download_check = ShortCircuitOperator(
            task_id="download_check",
            python_callable=check_redis_download,
            op_kwargs={"trip_type": "fhv"},
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
        )
        check_fhv = ShortCircuitOperator(
            task_id="check_fhv",
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
            python_callable=processing_query,
            op_kwargs={"trip_type": "fhv", "output_path": OUTPUT_PATH},
        )
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
        (
            download_check
            >> check_fhv
            >> clean_fhv
            >> enrich_fhv
            >> validate_fhv
            >> cleanup_fhv
        )

    # High Volume For Hire Vehicles Task Group
    with TaskGroup("fhvhv") as tg_fhvhv:
        download_check = ShortCircuitOperator(
            task_id="download_check",
            python_callable=check_redis_download,
            op_kwargs={"trip_type": "fhvhv"},
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
        )
        check_fhvhv = ShortCircuitOperator(
            task_id="check_fhvhv",
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
            python_callable=processing_query,
            op_kwargs={"trip_type": "fhvhv", "output_path": OUTPUT_PATH},
        )
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
        (
            download_check
            >> check_fhvhv
            >> clean_hvfhv
            >> enrich_fhvhv
            >> validate_fhvhv
            >> cleanup_fhvhv
        )

    # Sequential Group Execution for File Processing
    tg_yellow >> tg_green >> tg_fhv >> tg_fhvhv
