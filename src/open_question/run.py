"""Open question: are older films rated higher than recent ones, and is it a
real quality effect or a survivorship bias?

Survivorship bias: old films that are still watched (and rated) today are mostly
the ones that were remembered, while recent films are rated by everybody, good
or bad. If the bias explains the gap, then:
  - old films should have far fewer votes (only a selected few are watched);
  - the gap should shrink when films of the same "visibility" are compared,
    e.g. the 100 most-voted films of each decade.

Scope: feature films (titleType = movie, runtime between 40 min and 24 h)
with a rating, released from the 1920s to the last complete year.

Outputs: outputs/open_question.md, outputs/open_question_by_year.csv

Usage: python -m src.open_question.run [--top-n 100]
"""
import argparse
import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.config import REPORTS_DIR, silver_path
from src.common.spark import get_spark

FIRST_YEAR = 1920
LAST_YEAR = datetime.date.today().year - 1   # last complete year


def to_md(df) -> str:
    return df.toPandas().to_markdown(index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=100,
                        help="films per decade for the equal-visibility comparison")
    args = parser.parse_args()

    spark = get_spark("imdb-open-question")
    films = (
        spark.read.parquet(silver_path("title.basics"))
        .filter((F.col("titleType") == "movie")
                & F.col("runtimeMinutes").between(40, 24 * 60)
                & F.col("startYear").between(FIRST_YEAR, LAST_YEAR))
        .select("tconst", "primaryTitle", "startYear")
        .join(spark.read.parquet(silver_path("title.ratings")), "tconst")
        .withColumn("decade", (F.floor(F.col("startYear") / 10) * 10).cast("int"))
        .withColumn("log_votes", F.log10("numVotes"))
        .cache()
    )

    # 1. All rated films, by decade
    by_decade = (
        films.groupBy("decade").agg(
            F.count(F.lit(1)).alias("rated_films"),
            F.round(F.avg("averageRating"), 2).alias("mean_rating"),
            F.round(F.sum(F.col("averageRating") * F.col("numVotes")) / F.sum("numVotes"), 2)
            .alias("vote_weighted_rating"),
            F.percentile_approx("numVotes", 0.5).alias("median_votes"),
            F.round(100 * F.avg((F.col("numVotes") >= 1000).cast("int")), 1)
            .alias("pct_films_1000_votes"),
            F.round(F.corr("log_votes", "averageRating"), 3).alias("corr_logvotes_rating"),
        )
        .orderBy("decade")
    )

    # 2. Equal visibility: the N most-voted films of each decade
    rank = Window.partitionBy("decade").orderBy(F.desc("numVotes"), "tconst")
    top = films.withColumn("vote_rank", F.row_number().over(rank)).filter(
        F.col("vote_rank") <= args.top_n)
    top_by_decade = (
        top.groupBy("decade").agg(
            F.count(F.lit(1)).alias("films"),
            F.round(F.avg("averageRating"), 2).alias("mean_rating_top"),
            F.min("numVotes").alias("min_votes_in_top"),
        )
        .orderBy("decade")
    )
    comparison = (
        by_decade.select("decade", "mean_rating", "median_votes")
        .join(top_by_decade.select("decade", "mean_rating_top", "min_votes_in_top"), "decade")
        .withColumn("gap_top_minus_all", F.round(F.col("mean_rating_top") - F.col("mean_rating"), 2))
        .orderBy("decade")
    )

    # 3. Same visibility threshold for every decade
    thresholds = [1_000, 10_000, 100_000]
    threshold_rows = []
    for t in thresholds:
        threshold_rows.append(
            films.filter(F.col("numVotes") >= t).groupBy("decade").agg(
                F.lit(t).alias("min_votes"),
                F.count(F.lit(1)).alias("films"),
                F.round(F.avg("averageRating"), 2).alias("mean_rating"),
            )
        )
    by_threshold = threshold_rows[0]
    for df in threshold_rows[1:]:
        by_threshold = by_threshold.unionByName(df)
    pivot = (
        by_threshold.groupBy("decade").pivot("min_votes", thresholds)
        .agg(F.first("mean_rating"))
        .orderBy("decade")
    )
    for t in thresholds:
        pivot = pivot.withColumnRenamed(str(t), f"mean_rating_{t}_votes")

    # 4. Yearly series (for a chart in the report)
    by_year = (
        films.groupBy("startYear").agg(
            F.count(F.lit(1)).alias("rated_films"),
            F.round(F.avg("averageRating"), 3).alias("mean_rating"),
            F.percentile_approx("numVotes", 0.5).alias("median_votes"),
        )
        .orderBy("startYear")
    )

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    by_year.toPandas().to_csv(REPORTS_DIR / "open_question_by_year.csv", index=False)

    total = films.count()
    text = "\n\n".join([
        "# Open question: are older films rated higher, or is it survivorship bias?",
        f"Scope: {total:,} rated feature films (movie, 40 min to 24 h), "
        f"released {FIRST_YEAR}-{LAST_YEAR}.",
        "## 1. All rated films by decade",
        "`corr_logvotes_rating` = correlation between log10(number of votes) and rating.",
        to_md(by_decade),
        f"## 2. Equal visibility: the {args.top_n} most-voted films of each decade",
        "`gap_top_minus_all` = mean rating of the most-voted films minus mean rating of all films.",
        to_md(comparison),
        "## 3. Mean rating by decade above a fixed number of votes",
        to_md(pivot),
        "Yearly series: `outputs/open_question_by_year.csv`.",
    ]) + "\n"
    (REPORTS_DIR / "open_question.md").write_text(text, encoding="utf-8")
    print(text)
    print(f"[ok] written to {REPORTS_DIR / 'open_question.md'}")
    spark.stop()


if __name__ == "__main__":
    main()
