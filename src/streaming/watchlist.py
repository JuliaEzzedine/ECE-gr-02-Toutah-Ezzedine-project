"""Build the static watchlist joined with the Wikipedia stream.

Only three IMDb files are needed (title.basics, title.ratings, name.basics), so
this step runs without the full preparation pipeline. Files already present in
data/raw/ are reused.

    python -m src.streaming.watchlist --min-votes 5000

Output (Parquet, data/streaming/watchlist):
    key          normalised name used for the join
    entity_type  'title' or 'person'
    imdb_ids     tconst / nconst values sharing this key
    names        original IMDb names
    weight       numVotes of the most popular title (titles) or of the best
                 known-for title (persons)
"""
import argparse
import os
import shutil
import urllib.request

from pyspark.sql import functions as F

from src.streaming.common import WATCHLIST_PATH, get_spark, normalise_key

IMDB_BASE_URL = "https://datasets.imdbws.com/"
RAW_DIR = "data/raw"
FILES = ["title.basics.tsv.gz", "title.ratings.tsv.gz", "name.basics.tsv.gz"]
TITLE_TYPES = ["movie", "tvMovie", "tvSeries", "tvMiniSeries"]
PROFESSIONS = r"(actor|actress|director|writer|producer)"


def download_missing() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    for name in FILES:
        path = os.path.join(RAW_DIR, name)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            print(f"[skip] {name} already in {RAW_DIR}")
            continue
        print(f"[download] {name} ...", flush=True)
        req = urllib.request.Request(
            IMDB_BASE_URL + name, headers={"User-Agent": "ECE-student-project/1.0"}
        )
        tmp = path + ".part"
        with urllib.request.urlopen(req) as resp, open(tmp, "wb") as out:
            shutil.copyfileobj(resp, out, length=1 << 20)
        os.replace(tmp, path)
        print(f"[ok] {name}: {os.path.getsize(path) / 1e6:.0f} MB")


def read_tsv(spark, name):
    return (
        spark.read.option("sep", "\t")
        .option("header", True)
        .option("quote", "")
        .option("nullValue", "\\N")
        .csv(os.path.join(RAW_DIR, name))
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-votes", type=int, default=5000,
                        help="keep titles with at least this many IMDb votes")
    parser.add_argument("--min-key-length", type=int, default=4,
                        help="ignore very short names such as 'Up' or 'It'")
    parser.add_argument("--output", default=WATCHLIST_PATH)
    args = parser.parse_args()

    download_missing()
    spark = get_spark("imdb-watchlist", with_kafka=False)

    ratings = read_tsv(spark, "title.ratings.tsv.gz").select(
        "tconst", F.col("numVotes").cast("int").alias("numVotes")
    )
    titles = (
        read_tsv(spark, "title.basics.tsv.gz")
        .where(F.col("titleType").isin(TITLE_TYPES))
        .select("tconst", "primaryTitle")
        .join(ratings, "tconst")
        .where(F.col("numVotes") >= args.min_votes)
        .cache()
    )

    title_keys = (
        titles.withColumn("key", normalise_key(F.col("primaryTitle")))
        .groupBy("key")
        .agg(
            F.collect_set("tconst").alias("imdb_ids"),
            F.collect_set("primaryTitle").alias("names"),
            F.max("numVotes").alias("weight"),
        )
        .withColumn("entity_type", F.lit("title"))
    )

    # People known for at least one watched title, in a film profession.
    people = (
        read_tsv(spark, "name.basics.tsv.gz")
        .where(F.col("primaryProfession").rlike(PROFESSIONS))
        .select("nconst", "primaryName",
                F.explode(F.split("knownForTitles", ",")).alias("tconst"))
        .join(F.broadcast(titles.select("tconst", "numVotes")), "tconst")
    )
    person_keys = (
        people.withColumn("key", normalise_key(F.col("primaryName")))
        .groupBy("key")
        .agg(
            F.collect_set("nconst").alias("imdb_ids"),
            F.collect_set("primaryName").alias("names"),
            F.max("numVotes").alias("weight"),
        )
        .withColumn("entity_type", F.lit("person"))
    )

    watchlist = (
        title_keys.unionByName(person_keys)
        .where(F.length("key") >= args.min_key_length)
        .select("key", "entity_type", "imdb_ids", "names", "weight")
    )
    watchlist.coalesce(1).write.mode("overwrite").parquet(args.output)

    saved = spark.read.parquet(args.output)
    print(f"[ok] watchlist written to {args.output}")
    saved.groupBy("entity_type").count().show()
    spark.stop()


if __name__ == "__main__":
    main()
