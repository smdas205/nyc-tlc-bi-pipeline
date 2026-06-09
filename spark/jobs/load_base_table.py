import logging
import os
import sys

import requests
from dotenv.main import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)

load_dotenv()


# Function to Download Latest Base File from API
def get_file_from_api(dataset_id: str, output_filename: str):
    url = f"https://data.cityofnewyork.us/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"
    # Stream the request in case the file is large
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(output_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info(f"Success! Saved {output_filename}")
    else:
        logger.error(f"Error {response.status_code}: Failed to download data.")
        logger.error(response.text)
        response.raise_for_status()


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
        SparkSession.builder.appName("dim_load_base_table")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def get_files(spark: SparkSession, file_path: str, dataset_id: str) -> DataFrame:
    get_file_from_api(dataset_id, file_path)
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


# Main Function, Arguments from Airflow received here
if __name__ == "__main__":
    spark_session = get_spark()

    file_location = "/opt/spark/data/base"
    os.makedirs(file_location, exist_ok=True)

    historical_base_df = get_files(
        spark_session, f"{file_location}/historical_bases_data.csv", "2bmr-jdsv"
    )
    current_base_df = get_files(
        spark_session, f"{file_location}/current_bases_data.csv", "8wbx-tsch"
    )

    historical_cleaned = historical_base_df.selectExpr(
        "Base_Number AS base_num", "Base_Name AS base_name"
    ).dropDuplicates(["base_num"])

    historical_cleaned.createOrReplaceTempView("historical_df")

    current_cleaned = current_base_df.selectExpr(
        "`Base Number` AS base_num",
        "`Base Name` AS base_name",
        "`Base Type` AS base_type",
    ).dropDuplicates(["base_num"])

    current_cleaned.createOrReplaceTempView("current_df")

    joined_base_df: DataFrame = spark_session.sql("""
        SELECT
        COALESCE (c.base_num, h.base_num) AS base_num,
        c.base_name AS current_base_name,
        h.base_name AS historical_base_name,
        c.base_type AS base_type
        FROM current_df c FULL OUTER JOIN historical_df h
        ON c.base_num = h.base_num
        """)

    joined_base_df = joined_base_df.fillna({"base_type": "Inactive Base"})

    # Get DB Details
    jdbc_url = ensure_env("JDBC_URL")
    jdbc_props = {
        "user": ensure_env("DB_USER"),
        "password": ensure_env("DB_PASSWD"),
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
        "numPartitions": "8",
    }

    base_table_cols = [
        "base_num",
        "current_base_name",
        "historical_base_name",
        "base_type",
    ]

    # Get Existing Data from DB
    existing = spark_session.read.jdbc(
        url=jdbc_url,
        table="(SELECT base_num FROM base_table) AS b",
        properties=jdbc_props,
    )

    # Dedup Operation
    df_new = joined_base_df.select(base_table_cols).join(
        existing.select("base_num"),
        on=["base_num"],
        how="left_anti",
    )

    df_new.write.jdbc(
        url=jdbc_url, table="base_table", mode="append", properties=jdbc_props
    )

    spark_session.stop()
