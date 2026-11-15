"""Select the films and principals to enrich (Spark).

Films  : feature-length movies (titleType = movie, runtime >= --min-runtime)
         released between --year-min and --year-max, top --top-n per
         (genre, year) ranked by number of votes then average rating.
People : the first --principals principals of each selected film
         (lowest `ordering` in title.principals).

Nothing here depends on the size of the selection: removing the filters
(or raising --top-n) runs the same code on the full dataset.

Outputs (JSON lines, read by the scraper):
  data/enrich/targets/films/       one row per film
  data/enrich/targets/film_genre/  one row per (film, genre, year, rank)
  data/enrich/targets/people/      one row per person
  data/enrich/targets/film_people/ one row per (film, person)

Usage: python -m src.enrich.targets [--year-min 1980 --year-max 2025 --top-n 10]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.config import ENRICH_TARGETS_DIR, silver_path
from src.common.spark import get_spark


def select_films(spark, year_min, year_max, top_n, min_runtime):
    basics = spark.read.parquet(silver_path("title.basics"))
    ratings = spark.read.parquet(silver_path("title.ratings"))

    movies = (
        basics
        .filter((F.col("titleType") == "movie")
                & (F.col("runtimeMinutes") >= min_runtime)
                & F.col("startYear").between(year_min, year_max)
                & F.col("genres").isNotNull())
        .join(ratings, "tconst")
        .select("tconst", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes",
                F.explode("genres").alias("genre"), "averageRating", "numVotes")
    )
    rank_window = Window.partitionBy("genre", "startYear").orderBy(
        F.desc("numVotes"), F.desc("averageRating"), F.asc("tconst")
    )
    return (
        movies.withColumn("rank_in_genre_year", F.row_number().over(rank_window))
        .filter(F.col("rank_in_genre_year") <= top_n)
    )


def select_people(spark, films, n_principals):
    principals = spark.read.parquet(silver_path("title.principals"))
    names = spark.read.parquet(silver_path("name.basics"))
    order = Window.partitionBy("tconst").orderBy("ordering")
    film_people = (
        principals.join(films.select("tconst"), "tconst", "left_semi")
        .withColumn("principal_rank", F.row_number().over(order))
        .filter(F.col("principal_rank") <= n_principals)
        .select("tconst", "nconst", "category", "principal_rank")
    )
    people = (
        film_people.select("nconst").distinct()
        .join(names.select("nconst", "primaryName", "birthYear", "deathYear"), "nconst", "left")
    )
    return film_people, people


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year-min", type=int, default=1980)
    parser.add_argument("--year-max", type=int, default=2025)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--principals", type=int, default=3)
    parser.add_argument("--min-runtime", type=int, default=40,
                        help="feature-length threshold in minutes")
    args = parser.parse_args()

    spark = get_spark("imdb-enrich-targets")
    film_genre = select_films(spark, args.year_min, args.year_max, args.top_n, args.min_runtime)
    film_genre.write.mode("overwrite").json(str(ENRICH_TARGETS_DIR / "film_genre"))
    film_genre = spark.read.json(str(ENRICH_TARGETS_DIR / "film_genre"))

    films = film_genre.select(
        "tconst", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes",
        "averageRating", "numVotes",
    ).dropDuplicates(["tconst"])
    films.write.mode("overwrite").json(str(ENRICH_TARGETS_DIR / "films"))

    film_people, people = select_people(spark, films, args.principals)
    film_people.write.mode("overwrite").json(str(ENRICH_TARGETS_DIR / "film_people"))
    people.write.mode("overwrite").json(str(ENRICH_TARGETS_DIR / "people"))

    n_slots = film_genre.count()
    n_films = spark.read.json(str(ENRICH_TARGETS_DIR / "films")).count()
    n_people = spark.read.json(str(ENRICH_TARGETS_DIR / "people")).count()
    print(f"[ok] {n_slots:,} (genre, year, rank) slots -> {n_films:,} distinct films, "
          f"{n_people:,} distinct people")
    spark.stop()


if __name__ == "__main__":
    main()
