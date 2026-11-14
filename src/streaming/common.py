"""Shared helpers for the streaming part: Spark session and title normalisation.

The same normalisation is applied to IMDb names (watchlist) and to Wikipedia
page titles (stream), so that both sides of the join produce identical keys.
"""
import os

import pyspark
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
DEFAULT_TOPIC = "wiki-changes"
WATCHLIST_PATH = "data/streaming/watchlist"

# Wikipedia disambiguation suffixes, e.g. "Heat (1995 film)", "Tom Hardy (actor)".
TITLE_QUALIFIER = r"(film|movie|tv series|miniseries|tv film|documentary|animated series|web series)"
PERSON_QUALIFIER = r"(actor|actress|director|filmmaker|screenwriter|producer|writer|comedian|cinematographer|composer)"


def kafka_package() -> str:
    """Maven coordinates of the Kafka connector matching the installed PySpark."""
    version = pyspark.__version__.split("+")[0]
    scala = "2.13" if version.startswith("4") else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"


def get_spark(app_name: str, with_kafka: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if with_kafka:
        builder = builder.config("spark.jars.packages", kafka_package())
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalise_key(col: Column) -> Column:
    """Lower-case, underscores to spaces, drop a trailing "(...)" qualifier."""
    c = F.lower(F.regexp_replace(col, "_", " "))
    c = F.regexp_replace(c, r"\s*\([^)]*\)\s*$", "")
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)


def qualifier_kind(col: Column) -> Column:
    """Kind of entity announced by the Wikipedia qualifier.

    'title'  -> "(film)", "(2010 film)", "(TV series)" ...
    'person' -> "(actor)", "(director)" ...
    'none'   -> no qualifier, may be either a title or a person
    'other'  -> unrelated qualifier such as "(planet)" or "(band)": never matched
    """
    qual = F.lower(F.regexp_extract(col, r"\(([^)]*)\)\s*$", 1))
    return (
        F.when(qual == "", F.lit("none"))
        .when(qual.rlike(TITLE_QUALIFIER), F.lit("title"))
        .when(qual.rlike(PERSON_QUALIFIER), F.lit("person"))
        .otherwise(F.lit("other"))
    )
