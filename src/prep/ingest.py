"""Convert the raw IMDb TSV files into a Parquet "bronze" layer.

Files are read as plain text and split on tabs, so values are kept
exactly as in the source:
  - the IMDb null marker \\N is kept as the string "\\N"
  - empty fields stay "" (Spark's CSV reader would turn them into NULL)
  - a field is NULL only when the line has too few fields
Lines that do not have the expected number of fields are flagged with
`_malformed = true` and keep the original line in `_raw_line`.

A .gz file cannot be split, so Spark would read it with a single task and a
single core. Each file is therefore first decompressed to a scratch folder
(data/scratch/), which Spark can read in parallel, then the scratch copy is
deleted once the table is written.

Usage: python -m src.prep.ingest [--tables title.basics title.ratings] [--keep-scratch]
"""
import argparse
import gzip
import os
import shutil
import time
from pathlib import Path

from pyspark.sql import functions as F

from src.common.config import RAW_DIR, TABLES, bronze_path
from src.common.spark import get_spark

SCRATCH_DIR = Path(RAW_DIR).parent / "scratch"


def decompress(table: str) -> Path:
    """Gunzip data/raw/<table>.tsv.gz to data/scratch/<table>.tsv (skipped if up to date)."""
    src = Path(RAW_DIR) / f"{table}.tsv.gz"
    dst = SCRATCH_DIR / f"{table}.tsv"
    if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
        return dst
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    start = time.time()
    tmp = dst.with_suffix(".tsv.part")
    with gzip.open(src, "rb") as fin, open(tmp, "wb") as fout:
        shutil.copyfileobj(fin, fout, length=8 << 20)
    os.replace(tmp, dst)
    print(f"[gunzip] {table}: {dst.stat().st_size / 1e9:.2f} GB in {time.time() - start:.0f}s", flush=True)
    return dst


def ingest_table(spark, table: str, keep_scratch: bool) -> None:
    columns = TABLES[table]
    header = "\t".join(columns)
    src = decompress(table)

    lines = spark.read.text(str(src)).filter(F.col("value") != header)
    parts = F.split(F.col("value"), "\t", -1)

    df = lines.select(
        *[parts.getItem(i).alias(c) for i, c in enumerate(columns)],
        (F.size(parts) != len(columns)).alias("_malformed"),
        F.when(F.size(parts) != len(columns), F.col("value")).alias("_raw_line"),
        F.current_timestamp().alias("_ingested_at"),
    )
    df.write.mode("overwrite").parquet(bronze_path(table))

    written = spark.read.parquet(bronze_path(table))
    total = written.count()
    malformed = written.filter("_malformed").count()
    print(f"[ok] {table}: {total:,} rows, {malformed:,} malformed rows -> {bronze_path(table)}", flush=True)

    if not keep_scratch:
        src.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", nargs="*", default=list(TABLES))
    parser.add_argument("--keep-scratch", action="store_true",
                        help="keep the decompressed copies in data/scratch/")
    args = parser.parse_args()

    spark = get_spark("imdb-ingest")
    for table in args.tables:
        ingest_table(spark, table, args.keep_scratch)
    spark.stop()


if __name__ == "__main__":
    main()
