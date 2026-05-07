# io_utils.py is not used by DAG file.
# This script is used only for easy access to Pyspark.
import os
import shutil

from pyspark.sql import DataFrame, SparkSession


# PySpark Declaration
def get_spark(trip_type: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"nyc_tlc_{trip_type}")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )


# Function to read downloaded PARQUET files
def read_raw_parquet(
    spark: SparkSession, trip_type: str, path: str, year_month: str
) -> DataFrame:
    pattern = f"{path}/{trip_type}_tripdata_{year_month}.parquet"
    return spark.read.parquet(pattern)


# Function to read PARQUET files from temp path
def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


# Function to write PARQUET files in overwrite mode
def write_parquet(df: DataFrame, path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    df.write.mode("overwrite").parquet(path)
