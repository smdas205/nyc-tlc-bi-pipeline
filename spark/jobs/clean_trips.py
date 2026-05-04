import sys

from io_utils import get_spark, read_raw_parquet, write_parquet
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from schemas import CANONICAL_SCHEMA


# Function for tables to conform to canonical schema
# If canonical column found in df, column will be kept as it is
# Else the column will be appended with all row values as None/NULL
def conform_to_canon(df: DataFrame) -> DataFrame:
    current_columns = []
    for field in CANONICAL_SCHEMA.fields:
        if field.name in df.columns:
            current_columns.append(F.col(field.name).cast(field.dataType))
        else:
            current_columns.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(current_columns)


# Function to clean Yellow Taxis Datasets
def clean_yellow(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed("VendorID", "vendor_id")
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    df = df.withColumnRenamed("RatecodeID", "ratecode_id")
    df = df.withColumnRenamed("PULocationID", "pu_location_id")
    df = df.withColumnRenamed("DOLocationID", "do_location_id")
    df = df.withColumnRenamed("fare_amount", "core_fare")
    df = df.withColumnRenamed("tip_amount", "tips")
    df = df.filter(
        (F.col("payment_type") >= 0)
        & (F.col("core_fare") >= 0)
        & (F.col("extra") >= 0)
        & (F.col("mta_tax") >= 0)
        & (F.col("tips") >= 0)
        & (F.col("tolls_amount") >= 0)
        & (F.col("improvement_surcharge") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("congestion_surcharge") >= 0)
        & (F.col("passenger_count") >= 1)
        & (F.col("trip_distance") > 0)
    )
    if "Airport_fee" in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")
        df = df.filter(F.col("airport_fee") >= 0)
    elif "airport_fee" in df.columns:
        df = df.filter(F.col("airport_fee") >= 0)
    df = df.withColumn("taxi_type", F.lit("yellow"))
    return conform_to_canon(df)


# Function to clean Green Taxi Datasets
def clean_green(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed("VendorID", "vendor_id")
    df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    df = df.withColumnRenamed("RatecodeID", "ratecode_id")
    df = df.withColumnRenamed("PULocationID", "pu_location_id")
    df = df.withColumnRenamed("DOLocationID", "do_location_id")
    df = df.withColumnRenamed("fare_amount", "core_fare")
    df = df.withColumnRenamed("tip_amount", "tips")
    df = df.filter(
        (F.col("payment_type") >= 0)
        & (F.col("core_fare") >= 0)
        & (F.col("extra") >= 0)
        & (F.col("mta_tax") >= 0)
        & (F.col("tips") >= 0)
        & (F.col("tolls_amount") >= 0)
        & (F.col("improvement_surcharge") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("congestion_surcharge") >= 0)
        & (F.col("passenger_count") >= 1)
        & (F.col("trip_distance") > 0)
    )
    df = df.withColumn("taxi_type", F.lit("green"))
    return conform_to_canon(df)


# Function to clean FHV Taxi Datasets
def clean_fhv(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed("dropOff_datetime", "dropoff_datetime")
    df = df.withColumnRenamed("PUlocationID", "pu_location_id")
    df = df.withColumnRenamed("DOlocationID", "do_location_id")
    df = df.withColumnRenamed("SR_Flag", "sr_flag")
    df = df.withColumnRenamed("Affiliated_base_number", "affiliated_base_number")
    df = df.withColumn("taxi_type", F.lit("fhv"))
    return conform_to_canon(df)


# Function to clean High Volume FHV Taxi Datasets
def clean_fhvhv(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed("PULocationID", "pu_location_id")
    df = df.withColumnRenamed("DOLocationID", "do_location_id")
    df = df.withColumnRenamed("trip_miles", "trip_distance")
    df = df.withColumnRenamed("base_passenger_fare", "core_fare")
    df = df.withColumnRenamed("tolls", "tolls_amount")
    df = df.filter(
        (F.col("core_fare") >= 0)
        & (F.col("bcf") >= 0)
        & (F.col("sales_tax") >= 0)
        & (F.col("tips") >= 0)
        & (F.col("tolls_amount") >= 0)
        & (F.col("airport_fee") >= 0)
        & (F.col("congestion_surcharge") >= 0)
        & (F.col("driver_pay") >= 0)
    )
    df = df.withColumn("taxi_type", F.lit("fhvhv"))
    return conform_to_canon(df)


if __name__ == "__main__":
    trip_type = sys.argv[1]
    input_path = sys.argv[2]
    year_month = sys.argv[3]
    output_path = sys.argv[4]

    spark = get_spark(trip_type)
    df = read_raw_parquet(spark, trip_type, input_path, year_month)

    if trip_type == "yellow":
        df = clean_yellow(df)
    elif trip_type == "green":
        df = clean_green(df)
    elif trip_type == "fhv":
        df = clean_fhv(df)
    elif trip_type == "fhvhv":
        df = clean_fhvhv(df)

    write_parquet(df, output_path)
    spark.stop()
