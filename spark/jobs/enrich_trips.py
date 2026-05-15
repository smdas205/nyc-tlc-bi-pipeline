# enrich_trips.py is used to add analytical rows using existing rows.
import sys

from io_utils import get_spark, read_parquet, write_parquet
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f


# Function to Add Rate Metrics to Yellow/Green Datasets
def add_rate_metrics(df: DataFrame) -> DataFrame:
    df = df.filter((f.col("core_fare") > 0) & (f.col("core_fare") <= 500))
    df = df.withColumn(
        "fare_per_mile",
        f.when(
            f.col("trip_distance") > 0, f.col("core_fare") / f.col("trip_distance")
        ).otherwise(None),
    )
    df = df.withColumn(
        "tip_percent",
        f.when(
            f.col("core_fare") > 0, f.col("tips") / f.col("core_fare") * 100
        ).otherwise(None),
    )
    return df


# Function to add Time Metrics to All Datasets
def time_metrics(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "trip_duration_min",
        (
            (
                f.col("dropoff_datetime").cast("long")
                - f.col("pickup_datetime").cast("long")
            )
            / 60
        ),
    )
    # Additional Filter Added to Filter out Bad Rows
    df = df.filter(
        (f.col("trip_duration_min") >= 0.5)
        & (f.col("trip_duration_min") <= 600)
        & ((f.col("core_fare") <= 1000) | (f.col("core_fare").isNull()))
    )
    df = df.withColumn(
        "avg_speed_mph",
        f.when(
            (f.col("trip_distance") > 0) & (f.col("trip_duration_min") > 0),
            (f.col("trip_distance") / (f.col("trip_duration_min") / 60)),
        ).otherwise(None),
    )
    df = df.withColumn(
        "time_of_day",
        f.when(
            (f.hour(f.col("pickup_datetime")) >= 0)
            & (f.hour(f.col("pickup_datetime")) < 6),
            "Overnight",
        )
        .when(
            (f.hour(f.col("pickup_datetime")) >= 6)
            & (f.hour(f.col("pickup_datetime")) < 11),
            "Morning Rush",
        )
        .when(
            (f.hour(f.col("pickup_datetime")) >= 11)
            & (f.hour(f.col("pickup_datetime")) < 16),
            "Mid Day",
        )
        .when(
            (f.hour(f.col("pickup_datetime")) >= 16)
            & (f.hour(f.col("pickup_datetime")) < 20),
            "Evening Rush",
        )
        .when(
            (f.hour(f.col("pickup_datetime")) >= 20)
            & (f.hour(f.col("pickup_datetime")) <= 23),
            "Night",
        )
        .otherwise(None),
    )
    df = df.withColumn("day_of_week", f.date_format(f.col("pickup_datetime"), "EEEE"))
    df = df.withColumn("month_name", f.date_format(f.col("pickup_datetime"), "MMMM"))
    df = df.withColumn(
        "is_weekend",
        f.when(f.dayofweek(f.col("pickup_datetime")).isin(1, 7), True).otherwise(False),
    )
    df = df.withColumn("week_of_year", f.weekofyear(f.col("pickup_datetime")))
    return df


# Function to Add Driver Pay Metrics for High Volume For Hire Vehicles Dataset
def add_pay_metrics(df: DataFrame) -> DataFrame:
    df = df.withColumn("platform_take", f.col("core_fare") - f.col("driver_pay"))
    df = df.withColumn(
        "driver_pay_per_mile",
        f.when(
            f.col("trip_distance") > 0, f.col("driver_pay") / f.col("trip_distance")
        ).otherwise(None),
    )
    return df


# Function to Enrich Trips
def enrich_trips(spark: SparkSession, df: DataFrame, taxi_type: str) -> DataFrame:
    if taxi_type in ("yellow", "green"):
        df = add_rate_metrics(df)
    elif taxi_type == "fhvhv":
        df = add_pay_metrics(df)
    df = time_metrics(df)
    return df


# Main Function, DAG arguments received here.
if __name__ == "__main__":
    trip_type = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = get_spark(trip_type)
    df = read_parquet(spark, input_path)
    df = enrich_trips(spark, df, trip_type)
    write_parquet(df, output_path)
    spark.stop()
