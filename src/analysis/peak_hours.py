from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_peak_hours(df: DataFrame) -> DataFrame:
    hourly_counts = df.groupBy(
        "pickup_date",
        "pickup_hour"
    ).count()

    window_spec = Window.partitionBy(
        "pickup_date"
    ).orderBy(
        F.desc("count")
    )

    ranked_hours = hourly_counts.withColumn(
        "daily_hour_rank",
        F.dense_rank().over(window_spec)
    )

    return ranked_hours.filter(
        "daily_hour_rank = 1"
    )