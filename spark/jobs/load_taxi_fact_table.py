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
        SparkSession.builder.appName("load_metered_fact_tables")
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
    taxi_trips_cols = [
        "taxi_type",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "time_of_day",
        "day_of_week",
        "month_name",
        "is_weekend",
        "week_of_year",
        "pu_location_id",
        "do_location_id",
        "store_and_fwd_flag",
        "ratecode_id",
        "trip_type",
        "passenger_count",
        "trip_distance",
        "trip_time",
        "avg_speed_mph",
        "trip_duration_min",
        "payment_type",
        "core_fare",
        "extra",
        "mta_tax",
        "tips",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "logged_total_amount",
        "calculated_total_amount",
        "fare_per_mile",
        "tip_percent",
    ]

    spark_session = get_spark()
    yellow_processed = get_files("yellow", yr_month, spark_session)
    green_processed = get_files("green", yr_month, spark_session)

    df_union = yellow_processed.select(taxi_trips_cols).unionByName(
        green_processed.select(taxi_trips_cols)
    )

    # trip_id Guard
    assert "trip_id" not in df_union.columns, (
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
        pickup_datetime, pu_location_id, core_fare
        FROM taxi_trips
        WHERE
        pickup_datetime >= '{yr_month}-01' AND pickup_datetime < '{yr_month}-01'::date + interval '1 month') AS t""",
        properties=jdbc_read_props,
    )

    # Dedup Operation
    df_new = df_union.select(taxi_trips_cols).join(
        existing.select("pickup_datetime", "pu_location_id", "core_fare"),
        on=["pickup_datetime", "pu_location_id", "core_fare"],
        how="left_anti",
    )

    df_new = df_new.repartition(8)

    df_new.write.jdbc(
        url=jdbc_url, table="taxi_trips", mode="append", properties=jdbc_write_props
    )

    spark_session.stop()
