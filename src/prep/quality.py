"""Detect and handle data quality issues, producing the "silver" layer.

For every table the job:
  1. casts the raw strings to proper types (\\N and "" become NULL,
     comma separated lists become arrays, invalid numbers become NULL)
  2. adds one boolean `dq_*` column per data quality rule
  3. applies the rule's handling:
       - quarantine : the row is moved to data/quarantine/<table>
       - nullify    : the faulty value(s) are set to NULL, the row is kept
       - flag       : the row is kept unchanged, the flag column stays
                      available so analyses can filter on it
  4. writes the cleaned table to data/silver/<table>

Lists of ids (knownForTitles, directors, writers) are cleaned by removing
ids that do not exist in the parent table.

Outputs:
  data/silver/<table>, data/quarantine/<table>
  outputs/quality_report.csv / outputs/quality_report.md

Usage: python -m src.prep.quality
"""
from dataclasses import dataclass, field

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from src.common.config import (
    IMDB_NULL, QUARANTINE_DIR, REPORTS_DIR, STAGING_DIR, bronze_path, silver_path,
)
from src.common.spark import get_spark

import datetime

CURRENT_YEAR = datetime.date.today().year
MAX_LIFESPAN = 120
MAX_RUNTIME_MINUTES = 24 * 60
ARCHIVE_CATEGORIES = ["archive_footage", "archive_sound"]


# --------------------------------------------------------------------------- #
# Typing helpers
# --------------------------------------------------------------------------- #
def s(name: str) -> Column:
    """Raw string column with IMDb nulls and blanks converted to NULL."""
    c = F.col(name)
    return F.when(c.isNull() | (c == IMDB_NULL) | (F.trim(c) == ""), None).otherwise(c)


def i(name: str) -> Column:
    return s(name).cast("int")


def d(name: str) -> Column:
    return s(name).cast("double")


def arr(name: str) -> Column:
    return F.split(s(name), ",")


def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.parquet(bronze_path(table)).filter(~F.col("_malformed"))


# --------------------------------------------------------------------------- #
# Generic rule engine
# --------------------------------------------------------------------------- #
@dataclass
class Rule:
    name: str                 # becomes column dq_<name>
    condition: Column         # True when the row breaks the rule
    action: str               # quarantine | nullify | flag
    description: str
    nullify: list[str] = field(default_factory=list)

    @property
    def col(self) -> str:
        return f"dq_{self.name}"


def ids_of(df: DataFrame, key: str, alias: str) -> DataFrame:
    """Distinct ids of a parent table, with a marker column for left joins."""
    return df.select(F.col(key).alias(alias)).distinct().withColumn(f"{alias}_exists", F.lit(True))


def clean_id_list(df: DataFrame, key: str, list_col: str, valid_ids: DataFrame) -> DataFrame:
    """Remove ids from an array column when they are missing from `valid_ids`.

    Adds `<list_col>_orphans` = number of ids removed.
    """
    ref = f"__{list_col}_ref"
    kept = (
        df.select(key, F.explode(list_col).alias(ref))
        .join(valid_ids.select(F.col(valid_ids.columns[0]).alias(ref)), ref, "left_semi")
        .groupBy(key)
        .agg(F.collect_list(ref).alias(f"__{list_col}_kept"))
    )
    original_size = F.coalesce(F.size(list_col), F.lit(0))
    return (
        df.join(kept, key, "left")
        .withColumn(
            f"{list_col}_orphans",
            F.when(F.col(list_col).isNull(), 0)
            .otherwise(original_size - F.coalesce(F.size(f"__{list_col}_kept"), F.lit(0))),
        )
        .withColumn(
            list_col,
            F.when(F.col(list_col).isNull(), None)
            .when(F.size(f"__{list_col}_kept") > 0, F.col(f"__{list_col}_kept")),
        )
        .drop(f"__{list_col}_kept")
    )


def apply_rules(
    spark: SparkSession, table: str, df: DataFrame, key: list[str], rules: list[Rule],
    output_cols: list[str],
) -> list[dict]:
    """Flag, report and handle every rule, then write silver + quarantine."""
    dup = Rule(
        "duplicate_key",
        F.row_number().over(Window.partitionBy(*key).orderBy(F.lit(1))) > 1,
        "quarantine",
        f"Duplicate primary key ({', '.join(key)})",
    )
    rules = [dup] + rules
    for r in rules:
        df = df.withColumn(r.col, F.coalesce(r.condition, F.lit(False)))

    keep = output_cols + [r.col for r in rules]
    staging = str(STAGING_DIR / table.replace(".", "_"))
    df.select(*keep).write.mode("overwrite").parquet(staging)
    staged = spark.read.parquet(staging)

    counts = staged.agg(
        F.count(F.lit(1)).alias("__total"),
        *[F.sum(F.col(r.col).cast("int")).alias(r.col) for r in rules],
    ).collect()[0].asDict()
    total = counts["__total"]

    hard = [r.col for r in rules if r.action == "quarantine"]
    is_hard = F.lit(False)
    for c in hard:
        is_hard = is_hard | F.col(c)

    staged.filter(is_hard).write.mode("overwrite").parquet(
        str(QUARANTINE_DIR / table.replace(".", "_"))
    )

    silver = staged.filter(~is_hard)
    for r in rules:
        for c in r.nullify:
            silver = silver.withColumn(c, F.when(F.col(r.col), None).otherwise(F.col(c)))
    silver = silver.drop(*hard)  # always False in silver
    silver.write.mode("overwrite").parquet(silver_path(table))

    report = []
    for r in rules:
        n = counts[r.col] or 0
        handling = r.action if not r.nullify else f"nullify {', '.join(r.nullify)}"
        report.append({
            "table": table, "rule": r.col, "description": r.description,
            "rows_affected": n, "pct_rows": round(100 * n / total, 4) if total else 0,
            "handling": handling,
        })
    print(f"[ok] {table}: {total:,} rows checked, "
          f"{sum(counts[c] or 0 for c in hard):,} quarantine flags")
    return report


# --------------------------------------------------------------------------- #
# Tables
# --------------------------------------------------------------------------- #
def title_basics(spark):
    df = read_bronze(spark, "title.basics").select(
        s("tconst").alias("tconst"), s("titleType").alias("titleType"),
        s("primaryTitle").alias("primaryTitle"), s("originalTitle").alias("originalTitle"),
        i("isAdult").alias("isAdult"), i("startYear").alias("startYear"),
        i("endYear").alias("endYear"), i("runtimeMinutes").alias("runtimeMinutes"),
        arr("genres").alias("genres"),
    )
    series = F.col("titleType").isin("tvSeries", "tvMiniSeries")
    rules = [
        Rule("missing_title", F.col("primaryTitle").isNull(), "flag", "No primary title"),
        Rule("invalid_is_adult", ~F.col("isAdult").isin(0, 1), "nullify",
             "isAdult is not 0 or 1", ["isAdult"]),
        Rule("end_before_start", F.col("endYear") < F.col("startYear"), "nullify",
             "endYear earlier than startYear", ["endYear"]),
        Rule("end_year_not_series", F.col("endYear").isNotNull() & ~series, "nullify",
             "endYear set on a title that is not a series", ["endYear"]),
        Rule("start_in_future", F.col("startYear") > CURRENT_YEAR, "flag",
             "startYear in the future (announced / in production)"),
        Rule("start_too_old", F.col("startYear") < 1870, "flag",
             "startYear before the first motion pictures (1870)"),
        Rule("runtime_not_positive", F.col("runtimeMinutes") <= 0, "nullify",
             "runtimeMinutes <= 0", ["runtimeMinutes"]),
        Rule("runtime_extreme", F.col("runtimeMinutes") > MAX_RUNTIME_MINUTES, "flag",
             f"runtimeMinutes above {MAX_RUNTIME_MINUTES} (24 h)"),
        Rule("missing_genres", F.col("genres").isNull(), "flag", "No genre"),
    ]
    return apply_rules(spark, "title.basics", df, ["tconst"], rules, df.columns)


def name_basics(spark, title_ids):
    df = read_bronze(spark, "name.basics").select(
        s("nconst").alias("nconst"), s("primaryName").alias("primaryName"),
        i("birthYear").alias("birthYear"), i("deathYear").alias("deathYear"),
        arr("primaryProfession").alias("primaryProfession"),
        arr("knownForTitles").alias("knownForTitles"),
    )
    df = clean_id_list(df, "nconst", "knownForTitles", title_ids)
    rules = [
        Rule("missing_name", F.col("primaryName").isNull(), "flag", "No name"),
        Rule("death_before_birth", F.col("deathYear") < F.col("birthYear"), "nullify",
             "deathYear earlier than birthYear (impossible to know which one is wrong)",
             ["birthYear", "deathYear"]),
        Rule("birth_in_future", F.col("birthYear") > CURRENT_YEAR, "nullify",
             "birthYear in the future", ["birthYear"]),
        Rule("death_in_future", F.col("deathYear") > CURRENT_YEAR, "nullify",
             "deathYear in the future", ["deathYear"]),
        Rule("lifespan_too_long", (F.col("deathYear") - F.col("birthYear")) > MAX_LIFESPAN,
             "flag", f"Lifespan above {MAX_LIFESPAN} years"),
        Rule("presumed_dead_no_death_year",
             F.col("deathYear").isNull() & (F.lit(CURRENT_YEAR) - F.col("birthYear") > MAX_LIFESPAN),
             "flag", f"No deathYear but born more than {MAX_LIFESPAN} years ago"),
        Rule("known_for_orphans", F.col("knownForTitles_orphans") > 0, "flag",
             "knownForTitles referenced titles missing from title.basics (ids removed)"),
    ]
    cols = [c for c in df.columns if c != "knownForTitles_orphans"]
    return apply_rules(spark, "name.basics", df, ["nconst"], rules, cols)


def title_ratings(spark, title_ids):
    df = (
        read_bronze(spark, "title.ratings")
        .select(s("tconst").alias("tconst"), d("averageRating").alias("averageRating"),
                i("numVotes").alias("numVotes"))
        .join(title_ids, F.col("tconst") == F.col("t_id"), "left")
    )
    rules = [
        Rule("orphan_title", F.col("t_id_exists").isNull(), "quarantine",
             "tconst missing from title.basics"),
        Rule("rating_out_of_range", ~F.col("averageRating").between(1, 10), "quarantine",
             "averageRating outside [1, 10]"),
        Rule("votes_not_positive", F.col("numVotes") <= 0, "quarantine", "numVotes <= 0"),
    ]
    return apply_rules(spark, "title.ratings", df, ["tconst"], rules,
                       ["tconst", "averageRating", "numVotes"])


def title_episode(spark, title_ids):
    parents = title_ids.select(F.col("t_id").alias("p_id"),
                               F.col("t_id_exists").alias("p_id_exists"))
    df = (
        read_bronze(spark, "title.episode")
        .select(s("tconst").alias("tconst"), s("parentTconst").alias("parentTconst"),
                i("seasonNumber").alias("seasonNumber"),
                i("episodeNumber").alias("episodeNumber"))
        .join(title_ids, F.col("tconst") == F.col("t_id"), "left")
        .join(parents, F.col("parentTconst") == F.col("p_id"), "left")
    )
    rules = [
        Rule("orphan_episode", F.col("t_id_exists").isNull(), "quarantine",
             "Episode tconst missing from title.basics"),
        Rule("orphan_parent", F.col("p_id_exists").isNull(), "quarantine",
             "parentTconst missing from title.basics"),
        Rule("self_parent", F.col("tconst") == F.col("parentTconst"), "quarantine",
             "Episode is its own parent"),
        Rule("negative_numbers", (F.col("seasonNumber") < 0) | (F.col("episodeNumber") < 0),
             "nullify", "Negative season or episode number",
             ["seasonNumber", "episodeNumber"]),
        Rule("missing_season", F.col("seasonNumber").isNull(), "flag", "No seasonNumber"),
    ]
    return apply_rules(spark, "title.episode", df, ["tconst"], rules,
                       ["tconst", "parentTconst", "seasonNumber", "episodeNumber"])


def title_crew(spark, title_ids, name_ids):
    df = (
        read_bronze(spark, "title.crew")
        .select(s("tconst").alias("tconst"), arr("directors").alias("directors"),
                arr("writers").alias("writers"))
        .join(title_ids, F.col("tconst") == F.col("t_id"), "left")
    )
    df = clean_id_list(df, "tconst", "directors", name_ids)
    df = clean_id_list(df, "tconst", "writers", name_ids)
    rules = [
        Rule("orphan_title", F.col("t_id_exists").isNull(), "quarantine",
             "tconst missing from title.basics"),
        Rule("director_orphans", F.col("directors_orphans") > 0, "flag",
             "directors referenced people missing from name.basics (ids removed)"),
        Rule("writer_orphans", F.col("writers_orphans") > 0, "flag",
             "writers referenced people missing from name.basics (ids removed)"),
    ]
    return apply_rules(spark, "title.crew", df, ["tconst"], rules,
                       ["tconst", "directors", "writers"])


def title_principals(spark):
    titles = spark.read.parquet(silver_path("title.basics")).select(
        F.col("tconst").alias("t_id"), F.col("startYear").alias("t_startYear"),
        F.lit(True).alias("t_id_exists"),
    )
    names = spark.read.parquet(silver_path("name.basics")).select(
        F.col("nconst").alias("n_id"), F.col("birthYear").alias("n_birthYear"),
        F.col("deathYear").alias("n_deathYear"), F.lit(True).alias("n_id_exists"),
    )
    df = (
        read_bronze(spark, "title.principals")
        .select(s("tconst").alias("tconst"), i("ordering").alias("ordering"),
                s("nconst").alias("nconst"), s("category").alias("category"),
                s("job").alias("job"),
                F.from_json(s("characters"), ArrayType(StringType())).alias("characters"))
        .join(titles, F.col("tconst") == F.col("t_id"), "left")
        .join(names, F.col("nconst") == F.col("n_id"), "left")
    )
    archive = F.col("category").isin(*ARCHIVE_CATEGORIES)
    rules = [
        Rule("orphan_title", F.col("t_id_exists").isNull(), "quarantine",
             "tconst missing from title.basics"),
        Rule("orphan_person", F.col("n_id_exists").isNull(), "quarantine",
             "nconst missing from name.basics"),
        Rule("born_after_release", F.col("n_birthYear") > F.col("t_startYear"), "flag",
             "Principal born after the title was released"),
        Rule("died_before_release",
             (F.col("n_deathYear") < F.col("t_startYear")) & ~archive, "flag",
             "Principal died before the release (not archive footage)"),
    ]
    return apply_rules(spark, "title.principals", df, ["tconst", "ordering"], rules,
                       ["tconst", "ordering", "nconst", "category", "job", "characters"])


def title_akas(spark, title_ids):
    df = (
        read_bronze(spark, "title.akas")
        .select(s("titleId").alias("titleId"), i("ordering").alias("ordering"),
                s("title").alias("title"), s("region").alias("region"),
                s("language").alias("language"),
                F.split(s("types"), "\u0002").alias("types"),
                F.split(s("attributes"), "\u0002").alias("attributes"),
                i("isOriginalTitle").alias("isOriginalTitle"))
        .join(title_ids, F.col("titleId") == F.col("t_id"), "left")
    )
    rules = [
        Rule("orphan_title", F.col("t_id_exists").isNull(), "quarantine",
             "titleId missing from title.basics"),
        Rule("invalid_is_original", ~F.col("isOriginalTitle").isin(0, 1), "nullify",
             "isOriginalTitle is not 0 or 1", ["isOriginalTitle"]),
    ]
    return apply_rules(spark, "title.akas", df, ["titleId", "ordering"], rules,
                       ["titleId", "ordering", "title", "region", "language", "types",
                        "attributes", "isOriginalTitle"])


# --------------------------------------------------------------------------- #
def main() -> None:
    spark = get_spark("imdb-quality")
    report = []

    report += title_basics(spark)
    title_ids = ids_of(spark.read.parquet(silver_path("title.basics")), "tconst", "t_id")

    report += name_basics(spark, title_ids.select("t_id"))
    name_ids = ids_of(spark.read.parquet(silver_path("name.basics")), "nconst", "n_id")

    report += title_ratings(spark, title_ids)
    report += title_episode(spark, title_ids)
    report += title_crew(spark, title_ids, name_ids.select("n_id"))
    report += title_principals(spark)
    report += title_akas(spark, title_ids)
    spark.stop()

    df = pd.DataFrame(report)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(REPORTS_DIR / "quality_report.csv", index=False)
    md = ["## Data quality rules", "", df.to_markdown(index=False), ""]
    (REPORTS_DIR / "quality_report.md").write_text("\n".join(md), encoding="utf-8")
    print(df.to_string(index=False))
    print(f"\nReport: {REPORTS_DIR / 'quality_report.md'}")


if __name__ == "__main__":
    main()
