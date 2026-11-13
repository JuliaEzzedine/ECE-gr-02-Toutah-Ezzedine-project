"""Convert the raw IMDb TSV files into a Parquet "bronze" layer.

Files are read as plain text and split on tabs, so values are kept
exactly as in the source:
  - the IMDb null marker \\N is kept as the string "\\N"
  - empty fields stay "" (Spark's CSV reader would turn them into NULL)
  - a field is NULL only when the line has too few fields
Lines that do not have the expected number of fields are flagged with
`_malformed = true` and keep the original line in `_raw_line`.

Usage: python -m src.prep.ingest [--tables title.basics title.ratings]
"""
import argparse

from pyspark.sql import functions as F

from src.common.config import RAW_DIR, TABLES, bronze_path
from src.common.spark import get_spark


def ingest_table(spark, table: str) -> None:
    columns = TABLES[table]
    header = "\t".join(columns)
    src = str(RAW_DIR / f"{table}.tsv.gz")

    lines = spark.read.text(src).filter(F.col("value") != header)
    parts = F.split(F.col("value"), "\t", -1)

    df = lines.select(
        *[parts.getItem(i).alias(c) for i, c in enumerate(columns)],
        (F.size(parts) != len(columns)).alias("_malformed"),
        F.when(F.size(parts) != len(columns), F.col("value")).alias("_raw_line"),
    )
    # .gz is not splittable: spread rows before writing so later jobs run in parallel
    df = df.withColumn("_ingested_at", F.current_timestamp()).repartition(16)
    df.write.mode("overwrite").parquet(bronze_path(table))

    written = spark.read.parquet(bronze_path(table))
    total = written.count()
    malformed = written.filter("_malformed").count()
    print(f"[ok] {table}: {total:,} rows, {malformed:,} malformed rows -> {bronze_path(table)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", nargs="*", default=list(TABLES))
    args = parser.parse_args()

    spark = get_spark("imdb-ingest")
    for table in args.tables:
        ingest_table(spark, table)
    spark.stop()


if __name__ == "__main__":
    main()
