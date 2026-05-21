from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_hourly_summary(df: DataFrame) -> DataFrame:
    return df.groupBy("pickup_hour").agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.avg("tip_pct").alias("avg_tip_pct"),
        F.avg("trip_duration_minutes").alias("avg_duration_minutes")
    )


def calculate_payment_summary(df: DataFrame) -> DataFrame:
    return df.groupBy("payment_type_label").agg(
        F.count("*").alias("total_trips"),
        F.avg("tip_pct").alias("avg_tip_pct"),
        F.percentile_approx("tip_pct", 0.5).alias("median_tip_pct")
    )


def calculate_top_revenue_locations(df: DataFrame) -> DataFrame:
    revenue_df = df.groupBy("PULocationID").agg(
        F.round(F.sum("fare_amount"), 2).alias("total_revenue")
    )

    return revenue_df.orderBy(
        F.col("total_revenue").desc()
    ).limit(10)