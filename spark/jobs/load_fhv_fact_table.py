import os
import sys

from dotenv.main import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

load_dotenv()


# Function to ensure proper loading of env variables
def ensure_env(env_var):
    value = os.getenv(env_var)
    if not value:
        raise EnvironmentError(
            f"Empty or missing environment variable: {env_var}. Please check .env file."
        )
    else:
        return value


# Function for Spark Initialization
def get_spark():
    return (
        SparkSession.builder.appName("load_fhv_fact_table")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


# Function to get processed PARQUET files
def get_files(trip_type: str, yyyy_mm: str, spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(f"/opt/spark/data/processed/{trip_type}/{yyyy_mm}/")
    return df


# Main Function, Arguments from Airflow received here
if __name__ == "__main__":
    yr_month = sys.argv[1]

    fhv_trips_cols = [
        "dispatching_base_num",
        "pickup_datetime",
        "dropoff_datetime",
        "time_of_day",
        "day_of_week",
        "month_name",
        "is_weekend",
        "week_of_year",
        "pu_location_id",
        "do_location_id",
        "trip_time",
        "trip_duration_min",
        "sr_flag",
        "affiliated_base_num",
    ]

    spark_session = get_spark()
    fhv_processed = get_files("fhv", yr_month, spark_session).select(fhv_trips_cols)

    # trip_id Guard
    assert "trip_id" not in fhv_processed.columns, (
        "trip_id found in DataFrame. It is DB-generated (BIGSERIAL) "
        "and must not be written by PySpark."
    )

    # Get DB Details
    jdbc_url = ensure_env("JDBC_URL")

    # Props for declaring DB and managing intake/outtake data
    jdbc_read_props = {
        "user": ensure_env("DB_USER"),
        "password": ensure_env("DB_PASSWD"),
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }
    jdbc_write_props = {
        "user": ensure_env("DB_USER"),
        "password": ensure_env("DB_PASSWD"),
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
    }

    # Get Existing Data from DB
    existing = spark_session.read.jdbc(
        url=jdbc_url,
        table=f"""
        (SELECT
        pickup_datetime, pu_location_id, affiliated_base_num
        FROM fhv_trips
        WHERE
        pickup_datetime >= '{yr_month}-01' AND pickup_datetime < '{yr_month}-01'::date + interval '1 month') AS t""",
        properties=jdbc_read_props,
    )

    # Dedup Operation
    df_new = fhv_processed.select(fhv_trips_cols).join(
        existing.select("pickup_datetime", "pu_location_id", "affiliated_base_num"),
        on=["pickup_datetime", "pu_location_id", "affiliated_base_num"],
        how="left_anti",
    )

    df_new = df_new.repartition(8)

    df_new.write.jdbc(
        url=jdbc_url, table="fhv_trips", mode="append", properties=jdbc_write_props
    )

    spark_session.stop()
