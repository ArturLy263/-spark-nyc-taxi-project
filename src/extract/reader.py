from pyspark.sql import DataFrame


def read_taxi_data(spark, path: str) -> DataFrame:
    return spark.read.csv(
        path,
        header=True,
        inferSchema=True
    )