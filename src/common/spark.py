"""SparkSession factory used by every job."""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str, extra_packages: list[str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if extra_packages:
        builder = builder.config("spark.jars.packages", ",".join(extra_packages))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
