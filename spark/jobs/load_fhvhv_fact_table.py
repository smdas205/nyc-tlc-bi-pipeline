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
        SparkSession.builder.appName("load_high_volume_fhv_fact_table")
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
    high_volume_fhv_trips_cols = [
        "hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        "request_datetime",
        "on_scene_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "time_of_day",
        "day_of_week",
        "month_name",
        "is_weekend",
        "week_of_year",
        "pu_location_id",
        "do_location_id",
        "trip_distance",
        "trip_time",
        "avg_speed_mph",
        "trip_duration_min",
        "core_fare",
        "tolls_amount",
        "extra",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        "platform_take",
        "driver_pay_per_mile",
        "fare_per_mile",
        "tip_percent",
        "cbd_congestion_fee",
        "calculated_total_amount",
        "shared_request_flag",
        "shared_match_flag",
        "access_a_ride_flag",
        "wav_request_flag",
        "wav_match_flag",
    ]

    spark_session = get_spark()
    fhvhv_processed = get_files("fhvhv", yr_month, spark_session).select(
        high_volume_fhv_trips_cols
    )

    # trip_id Guard
    assert "trip_id" not in fhvhv_processed.columns, (
        "trip_id found in DataFrame. It is DB-generated (BIGSERIAL) "
        "and must not be written by PySpark."
    )

    # Get DB Details
    jdbc_url = ensure_env("JDBC_URL")

    # Props for declaring DB and managing memory size
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
        "numPartitions": "8",
    }

    # Get Existing Data from DB
    existing = spark_session.read.jdbc(
        url=jdbc_url,
        table=f"""
        (SELECT
        pickup_datetime, pu_location_id, core_fare
        FROM high_volume_fhv_trips
        WHERE
        pickup_datetime >= '{yr_month}-01' AND pickup_datetime < '{yr_month}-01'::date + interval '1 month') AS t""",
        properties=jdbc_read_props,
    )

    # Dedup Operation
    df_new = fhvhv_processed.join(
        existing.select("pickup_datetime", "pu_location_id", "core_fare"),
        on=["pickup_datetime", "pu_location_id", "core_fare"],
        how="left_anti",
    )

    df_new = df_new.repartition(8)

    df_new.write.jdbc(
        url=jdbc_url,
        table="high_volume_fhv_trips",
        mode="append",
        properties=jdbc_write_props,
    )

    spark_session.stop()
