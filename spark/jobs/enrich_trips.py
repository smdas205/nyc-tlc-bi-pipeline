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


# Function to Add Zone Based Metrics to All Datasets using NYC Taxi Zone Lookup
def zone_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    df = df.drop("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone")
    pickup_csv = (
        spark.read.csv(
            "/opt/spark/data/raw/taxi_zone_lookup.csv",
            inferSchema=True,
            header=True,
        )
        .withColumnRenamed("Borough", "pickup_borough")
        .withColumnRenamed("Zone", "pickup_zone")
    )

    df_join_pickup = df.join(
        pickup_csv.select("LocationID", "pickup_borough", "pickup_zone"),
        on=df["pu_location_id"] == pickup_csv["LocationID"],
        how="left",
    ).drop("LocationID")

    dropoff_csv = (
        spark.read.csv(
            "/opt/spark/data/raw/taxi_zone_lookup.csv",
            inferSchema=True,
            header=True,
        )
        .withColumnRenamed("Borough", "dropoff_borough")
        .withColumnRenamed("Zone", "dropoff_zone")
    )

    df_join_dropoff = df_join_pickup.join(
        dropoff_csv.select("LocationID", "dropoff_borough", "dropoff_zone"),
        on=df_join_pickup["do_location_id"] == dropoff_csv["LocationID"],
        how="left",
    ).drop("LocationID")

    return df_join_dropoff


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
    df = zone_metrics(spark, df)
    return df


if __name__ == "__main__":
    trip_type = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = get_spark(trip_type)
    df = read_parquet(spark, input_path)
    df = enrich_trips(spark, df, trip_type)
    write_parquet(df, output_path)
    spark.stop()
