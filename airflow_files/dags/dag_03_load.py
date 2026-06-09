import logging
import os.path

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, TaskGroup

logger = logging.getLogger(__name__)

SCRIPT_PATH = "/opt/airflow/spark/jobs"
FILES_PATH = "/opt/spark/data/processed"

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2022, 1, 1, tz="America/New_York"),
    "end_date": pendulum.datetime(2023, 12, 31, tz="America/New_York"),
    "retries": 1,
}


# Function to check if yellow and green processed files are present
def success_yellow_green(output_path: str, **context) -> bool:
    working_date = context["logical_date"].strftime("%Y-%m")
    success_path_yellow = os.path.join(output_path, "yellow", working_date, "_SUCCESS")
    success_path_green = os.path.join(output_path, "green", working_date, "_SUCCESS")
    if not (os.path.exists(success_path_yellow) and os.path.exists(success_path_green)):
        logger.info(
            f"Processed files does not exist for {working_date} in yellow or green taxi types. Please check."
        )
        return False
    logger.info(
        f"Processed files exist for {working_date}. Moving ahead with loading process."
    )
    return True


# Function to check if rest processed files are present
def success_query(trip_type: str, output_path: str, **context) -> bool:
    working_date = context["logical_date"].strftime("%Y-%m")
    success_path = os.path.join(output_path, trip_type, working_date, "_SUCCESS")
    if not os.path.exists(success_path):
        logger.info(
            f"Processed files does not exist for {working_date} in {trip_type} taxi types. Please check."
        )
        return False
    logger.info(
        f"Processed files exist for {working_date} for {trip_type} taxi type. Moving ahead with loading process."
    )
    return True


with DAG(
    dag_id="load_tlc_files",
    default_args=default_args,
    schedule="@monthly",
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
) as dag:
    with TaskGroup("dim_base_load") as tg_base:
        load_base_table = SparkSubmitOperator(
            task_id="load_base_table",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/load_base_table.py",
            jars="/opt/spark/jars/postgresql-42.7.11.jar",
            conf={
                "spark.hadoop.fs.permissions.umask-mode": "000",
            },
        )
        load_base_table
    with TaskGroup("yellow_green_load") as tg_yellow_green:
        green_yellow_success_check = ShortCircuitOperator(
            task_id="green_yellow_success_check",
            python_callable=success_yellow_green,
            op_kwargs={"output_path": FILES_PATH},
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
        )
        load_green_yellow = SparkSubmitOperator(
            task_id="load_green_yellow",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/load_taxi_fact_table.py",
            application_args=["{{ logical_date.strftime('%Y-%m') }}"],
            jars="/opt/spark/jars/postgresql-42.7.11.jar",
            conf={
                "spark.hadoop.fs.permissions.umask-mode": "000",
            },
        )
        green_yellow_success_check >> load_green_yellow
    with TaskGroup("fhv_load") as tg_fhv:
        fhv_success_check = ShortCircuitOperator(
            task_id="fhv_success_check",
            python_callable=success_query,
            op_kwargs={"trip_type": "fhv", "output_path": FILES_PATH},
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
        )
        load_fhv = SparkSubmitOperator(
            task_id="load_fhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/load_fhv_fact_table.py",
            application_args=["{{ logical_date.strftime('%Y-%m') }}"],
            jars="/opt/spark/jars/postgresql-42.7.11.jar",
            conf={
                "spark.hadoop.fs.permissions.umask-mode": "000",
            },
        )
        fhv_success_check >> load_fhv
    with TaskGroup("fhvhv_load") as tg_fhvhv:
        fhvhv_success_check = ShortCircuitOperator(
            task_id="fhvhv_success_check",
            python_callable=success_query,
            op_kwargs={"trip_type": "fhvhv", "output_path": FILES_PATH},
            trigger_rule="all_done",
            ignore_downstream_trigger_rules=False,
        )
        load_fhvhv = SparkSubmitOperator(
            task_id="load_fhvhv",
            conn_id="spark_default",
            application=f"{SCRIPT_PATH}/load_fhvhv_fact_table.py",
            application_args=["{{ logical_date.strftime('%Y-%m') }}"],
            jars="/opt/spark/jars/postgresql-42.7.11.jar",
            conf={
                "spark.hadoop.fs.permissions.umask-mode": "000",
                "spark.executor.memory": "5g",
                "spark.executor.memoryOverhead": "512m",
                "spark.driver.memory": "2g",
                "spark.sql.shuffle.partitions": "8",
            },
        )
        fhvhv_success_check >> load_fhvhv

    tg_base >> tg_yellow_green >> tg_fhv >> tg_fhvhv
