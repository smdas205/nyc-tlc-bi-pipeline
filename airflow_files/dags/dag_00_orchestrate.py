import logging

import pendulum
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)

REDIS_DB = 2

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2022, 1, 1, tz="America/New_York"),
    "end_date": pendulum.datetime(2023, 12, 31, tz="America/New_York"),
    "retries": 1,
}


# Function to setup and update Redis on Airflow Run Status
def update_redis(**context):
    logical_date = context["logical_date"]
    year = logical_date.year
    month = logical_date.strftime("%m")

    hook = RedisHook(redis_conn_id="redis_db2")
    r = hook.get_conn()
    key = f"tlc:power_bi_ready:{year}:{month}"
    r.set(key, "1")
    logger.info(f"[notify_pbi_ready] Set Redis key: {key}")
    return None


with DAG(
    dag_id="orchestrate_tlc_files",
    default_args=default_args,
    schedule="@monthly",
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    tags=["orchestration", "master"],
) as dag:
    trigger_ingest_datasets = TriggerDagRunOperator(
        task_id="trigger_ingest_datasets",
        trigger_dag_id="ingest_tlc_files",
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    trigger_process_datasets = TriggerDagRunOperator(
        task_id="trigger_process_datasets",
        trigger_dag_id="process_tlc_files",
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    trigger_load_database = TriggerDagRunOperator(
        task_id="trigger_load_database",
        trigger_dag_id="load_tlc_files",
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    update_redis_pbi = PythonOperator(
        task_id="update_redis", python_callable=update_redis
    )

    (
        trigger_ingest_datasets
        >> trigger_process_datasets
        >> trigger_load_database
        >> update_redis_pbi
    )
