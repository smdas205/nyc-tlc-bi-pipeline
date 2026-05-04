# spark/jobs/io_utils.py
import os
import shutil

from pyspark.sql import DataFrame, SparkSession


def get_spark(trip_type: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"nyc_tlc_{trip_type}")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )


def read_raw_parquet(
    spark: SparkSession, trip_type: str, path: str, year_month: str
) -> DataFrame:
    pattern = f"{path}/{trip_type}_tripdata_{year_month}.parquet"
    return spark.read.parquet(pattern)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def write_parquet(df: DataFrame, path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    df.write.mode("overwrite").parquet(path)
