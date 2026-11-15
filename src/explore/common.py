"""Shared helpers for the exploration queries."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import REPORTS_DIR, silver_path

OUT_DIR = REPORTS_DIR / "exploration"

# Shared definitions (also documented in REPORT.md)
FEATURE_MIN_RUNTIME = 40      # minutes: feature-length film (AMPAS / AFI / BFI definition)
YEAR_MIN, YEAR_MAX = 1980, 2025
MIN_VOTES = 1000              # a rating is only trusted above this number of votes
ACTING = ["actor", "actress"]


def table(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.parquet(silver_path(name))


def feature_movies(spark: SparkSession) -> DataFrame:
    """Feature-length movies with their rating (left join: unrated movies kept)."""
    basics = table(spark, "title.basics")
    ratings = table(spark, "title.ratings")
    return (
        basics.filter(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= FEATURE_MIN_RUNTIME)
            & ~F.col("dq_runtime_extreme")
            & (F.coalesce(F.col("isAdult"), F.lit(0)) == 0)
        )
        .join(ratings, "tconst", "left")
        .select("tconst", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes",
                "genres", "averageRating", "numVotes")
    )


def rated(df: DataFrame, min_votes: int = MIN_VOTES) -> DataFrame:
    return df.filter(F.col("averageRating").isNotNull() & (F.col("numVotes") >= min_votes))


def save(name: str, sections: list[tuple[str, object]], full: DataFrame | None = None,
         intro: str = "") -> None:
    """Write outputs/exploration/<name>.md (pandas tables) and optionally a full CSV."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [f"## {name}", ""]
    if intro:
        lines += [intro, ""]
    for title, pdf in sections:
        lines += [f"### {title}", ""]
        if hasattr(pdf, "to_markdown"):
            lines += [pdf.to_markdown(index=False) if len(pdf) else "_No rows._", ""]
        else:
            lines += [str(pdf), ""]
    if full is not None:
        csv_dir = OUT_DIR / f"{name}_full"
        full.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv_dir))
        lines += [f"Full result: `outputs/exploration/{name}_full/` (CSV)", ""]
    (OUT_DIR / f"{name}.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"[ok] {name} -> {OUT_DIR / (name + '.md')}")
