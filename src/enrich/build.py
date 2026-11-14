"""Clean the scraped data and build the enriched tables (Spark).

  - reads the scraper's JSON lines with an explicit schema (schema enforcement:
    unexpected fields are ignored, malformed lines are counted and dropped)
  - keeps the most recent record per id
  - cleans the text (whitespace, leftover empty brackets, very short texts)
  - joins with the selected films / people and adds an explicit `note`
    whenever information is not available, plus lineage columns
    (source_url, wikidata_uri, revision_id, retrieved_at, license)

Outputs:
  data/enriched/films       one row per film: IMDb data + synopsis + lineage
  data/enriched/people      one row per person: IMDb data + biography + lineage
  data/enriched/film_genre  top-N ranking (genre, year, rank, tconst)
  data/enriched/film_people link table (tconst, nconst, category, principal_rank)
  outputs/enrichment_summary.md

Usage: python -m src.enrich.build
"""
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.common.config import ENRICH_RAW_DIR, ENRICH_TARGETS_DIR, ENRICHED_DIR, REPORTS_DIR
from src.common.spark import get_spark

MIN_TEXT_LENGTH = 40
WIKIDATA_LOOKUP = "https://query.wikidata.org/sparql (lookup on property P345, IMDb ID)"

LINEAGE_FIELDS = [
    StructField("wikidata_uri", StringType()),
    StructField("source_url", StringType()),
    StructField("revision_id", LongType()),
    StructField("license", StringType()),
    StructField("status", StringType()),
    StructField("retrieved_at", StringType()),
    StructField("_corrupt", StringType()),
]
SYNOPSIS_SCHEMA = StructType(
    [StructField("tconst", StringType()), StructField("section", StringType()),
     StructField("synopsis", StringType())] + LINEAGE_FIELDS
)
BIO_SCHEMA = StructType(
    [StructField("nconst", StringType()), StructField("biography", StringType())]
    + LINEAGE_FIELDS
)

NOTES = {
    "no_wikidata_item": "No Wikidata item references this IMDb id",
    "no_english_wikipedia_article": "No English Wikipedia article for this title/person",
    "wikipedia_page_missing": "Wikipedia article could not be retrieved",
    "no_plot_section": "Wikipedia article has no Plot/Synopsis section",
    "empty_introduction": "Wikipedia article has an empty introduction",
    "too_short": f"Text shorter than {MIN_TEXT_LENGTH} characters after cleaning",
    "not_fetched": "Not fetched yet (scraper not run for this id)",
}


def clean_text(col):
    text = F.regexp_replace(col, r"\(\s*[;,]?\s*\)", "")       # "( )" left by removed markup
    text = F.regexp_replace(text, r"[ \t]+", " ")
    text = F.regexp_replace(text, r"\s*\n\s*\n+\s*", "\n\n")
    return F.trim(text)


def read_raw(spark, name, schema, key):
    raw = (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .json(str(ENRICH_RAW_DIR / name))
    ).cache()
    corrupt = raw.filter(F.col("_corrupt").isNotNull()).count()
    latest = Window.partitionBy(key).orderBy(F.desc("retrieved_at"))
    clean = (
        raw.filter(F.col("_corrupt").isNull() & F.col(key).isNotNull())
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_corrupt")
    )
    return clean, corrupt


def with_note(df, text_col):
    """Final status + human readable note when the text is not available."""
    status = (
        F.when(F.col("status").isNull(), "not_fetched")
        .when((F.col("status") == "ok") & (F.length(text_col) < MIN_TEXT_LENGTH), "too_short")
        .otherwise(F.col("status"))
    )
    mapping = F.create_map(*[x for k, v in NOTES.items() for x in (F.lit(k), F.lit(v))])
    return (
        df.withColumn("status", status)
        .withColumn(text_col, F.when(F.col("status") == "ok", F.col(text_col)))
        .withColumn("note", F.when(F.col("status") != "ok", mapping[F.col("status")]))
        # lineage: most precise source consulted for this record
        .withColumn("lineage", F.when(
            F.col("status") != "not_fetched",
            F.coalesce("source_url", "wikidata_uri", F.lit(WIKIDATA_LOOKUP)),
        ))
    )


def main() -> None:
    spark = get_spark("imdb-enrich-build")
    targets = lambda name: spark.read.json(str(ENRICH_TARGETS_DIR / name))  # noqa: E731

    synopses, bad_syn = read_raw(spark, "film_synopsis.jsonl", SYNOPSIS_SCHEMA, "tconst")
    synopses = synopses.withColumn("synopsis", clean_text(F.col("synopsis")))
    films = with_note(
        targets("films").join(synopses, "tconst", "left"), "synopsis"
    ).withColumnRenamed("section", "source_section")

    bios, bad_bio = read_raw(spark, "person_bio.jsonl", BIO_SCHEMA, "nconst")
    bios = bios.withColumn("biography", clean_text(F.col("biography")))
    people = with_note(targets("people").join(bios, "nconst", "left"), "biography")

    films.write.mode("overwrite").parquet(str(ENRICHED_DIR / "films"))
    people.write.mode("overwrite").parquet(str(ENRICHED_DIR / "people"))
    targets("film_genre").write.mode("overwrite").parquet(str(ENRICHED_DIR / "film_genre"))
    targets("film_people").write.mode("overwrite").parquet(str(ENRICHED_DIR / "film_people"))

    films = spark.read.parquet(str(ENRICHED_DIR / "films"))
    people = spark.read.parquet(str(ENRICHED_DIR / "people"))
    film_status = films.groupBy("status").count().orderBy(F.desc("count")).toPandas()
    people_status = people.groupBy("status").count().orderBy(F.desc("count")).toPandas()

    # Each selected film should have at least 3 principals with a biography
    fp = spark.read.parquet(str(ENRICHED_DIR / "film_people"))
    bio_per_film = (
        fp.join(people.select("nconst", (F.col("status") == "ok").alias("has_bio")), "nconst")
        .groupBy("tconst").agg(F.sum(F.col("has_bio").cast("int")).alias("bios"))
        .groupBy("bios").count().orderBy("bios").toPandas()
    )
    sample = (
        films.filter("status = 'ok'")
        .select("tconst", "primaryTitle", "startYear",
                F.concat(F.regexp_replace(F.substring("synopsis", 1, 120), r"\s+", " "), F.lit("..."))
                .alias("synopsis_start"),
                "source_url")
        .orderBy(F.desc("numVotes")).limit(5).toPandas()
    )
    spark.stop()

    for df, total_col in ((film_status, "count"), (people_status, "count")):
        df["pct"] = (100 * df[total_col] / df[total_col].sum()).round(2)
    md = [
        "## Enrichment coverage",
        "",
        f"Malformed raw lines dropped: {bad_syn} (synopses), {bad_bio} (biographies)",
        "",
        "### Films (synopsis)", "", film_status.to_markdown(index=False), "",
        "### People (biography)", "", people_status.to_markdown(index=False), "",
        "### Number of principals with a biography per film", "",
        bio_per_film.to_markdown(index=False), "",
        "### Sample", "", sample.to_markdown(index=False), "",
    ]
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "enrichment_summary.md").write_text("\n".join(md), encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()
