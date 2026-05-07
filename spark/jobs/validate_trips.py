# validate_trips.py is used to validate the enriched PARQUET file
import sys

import pyspark.sql.functions as F
from io_utils import get_spark, read_parquet
from pyspark.sql import DataFrame


# Function to check for no nulls in enriched file for specific rows
def _assert_no_nulls(df: DataFrame, cols: list[str], taxi_type: str) -> None:
    for col in cols:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise AssertionError(
                f"[{taxi_type}] Column '{col}' has {null_count} null(s)"
            )


# Function to check if values are between a certain min value and max value
def _assert_between(
    df: DataFrame, col: str, min_val: float, max_val: float, taxi_type: str
) -> None:
    violations = df.filter(
        F.col(col).isNotNull() & ~F.col(col).between(min_val, max_val)
    ).count()
    if violations > 0:
        raise AssertionError(
            f"[{taxi_type}] Column '{col}' has {violations} value(s) outside [{min_val}, {max_val}]"
        )


# Function to check for row count to match min rows standard
def _assert_min_rows(df: DataFrame, threshold: int, taxi_type: str) -> None:
    row_count = df.count()
    if row_count < threshold:
        raise AssertionError(
            f"[{taxi_type}] Row count {row_count} is below threshold {threshold}"
        )


# Function to check if enriched yellow, green and HVFHV datasets are valid
def expect_metered(df: DataFrame, taxi_type: str) -> DataFrame:
    _assert_no_nulls(
        df,
        ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"],
        taxi_type,
    )
    _assert_between(df, "trip_duration_min", 0.5, 600, taxi_type)
    _assert_between(df, "core_fare", 0.0, 1000, taxi_type)
    _assert_min_rows(df, 1000, taxi_type)
    return df


# Function to check if enriched FHV dataset is valid
def expect_fhv(df: DataFrame) -> DataFrame:
    _assert_no_nulls(df, ["pickup_datetime"], "fhv")
    _assert_between(df, "trip_duration_min", 0.5, 600, "fhv")
    _assert_min_rows(df, 1000, "fhv")
    return df


# Main Function, DAG arguments arrive here.
if __name__ == "__main__":
    trip_type = sys.argv[1]
    input_path = sys.argv[2]

    spark = get_spark(trip_type)
    df = read_parquet(spark, input_path)
    # Try-Except Block to Shift Bad Files to Quarantine
    try:
        if trip_type == "fhv":
            expect_fhv(df)
        else:
            expect_metered(df, trip_type)
    except AssertionError as e:
        import os
        import shutil

        spark.stop()
        quarantine_path = input_path.replace("/processed/", "/quarantine/")
        os.makedirs(quarantine_path, exist_ok=True)
        shutil.move(input_path, quarantine_path)
        raise e
    finally:
        spark.stop()
