from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "TaxiETL") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark