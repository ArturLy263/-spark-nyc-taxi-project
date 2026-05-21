from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def remove_invalid_distances(df: DataFrame) -> DataFrame:
    return df.filter(col("trip_distance") > 0)


def remove_invalid_fares(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("fare_amount") > 0) &
        (col("fare_amount") < 500)
    )


def remove_invalid_passengers(df: DataFrame) -> DataFrame:
    return df.filter(
        col("passenger_count").between(1, 6)
    )


def remove_invalid_datetimes(df: DataFrame) -> DataFrame:
    return df.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("tpep_dropoff_datetime").isNotNull()
    )


def remove_negative_trip_duration(df: DataFrame) -> DataFrame:
    return df.filter(
        col("tpep_dropoff_datetime") >
        col("tpep_pickup_datetime")
    )