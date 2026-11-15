"""Shared paths and dataset definitions."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"            # downloaded .tsv.gz files
BRONZE_DIR = DATA_DIR / "bronze"      # Parquet, all columns as strings
STAGING_DIR = DATA_DIR / "_staging"   # typed tables + data quality flags
SILVER_DIR = DATA_DIR / "silver"      # cleaned, typed tables used by all analyses
QUARANTINE_DIR = DATA_DIR / "quarantine"  # rows rejected by hard data quality rules
REPORTS_DIR = ROOT / "outputs"        # tables reused in REPORT.md

IMDB_BASE_URL = "https://datasets.imdbws.com"

# Column names of each IMDb table, in file order.
TABLES = {
    "name.basics": [
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles",
    ],
    "title.akas": [
        "titleId", "ordering", "title", "region", "language",
        "types", "attributes", "isOriginalTitle",
    ],
    "title.basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title.crew": ["tconst", "directors", "writers"],
    "title.episode": ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
    "title.principals": [
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ],
    "title.ratings": ["tconst", "averageRating", "numVotes"],
}

# Columns that are expected to hold numbers (used to detect bad values).
NUMERIC_COLUMNS = {
    "name.basics": ["birthYear", "deathYear"],
    "title.akas": ["ordering", "isOriginalTitle"],
    "title.basics": ["isAdult", "startYear", "endYear", "runtimeMinutes"],
    "title.episode": ["seasonNumber", "episodeNumber"],
    "title.principals": ["ordering"],
    "title.ratings": ["averageRating", "numVotes"],
}

IMDB_NULL = "\\N"


def bronze_path(table: str) -> str:
    return str(BRONZE_DIR / table.replace(".", "_"))


def silver_path(table: str) -> str:
    return str(SILVER_DIR / table.replace(".", "_"))

ENRICH_DIR = DATA_DIR / "enrich"
ENRICH_TARGETS_DIR = ENRICH_DIR / "targets"   # films / people selected by Spark
ENRICH_RAW_DIR = ENRICH_DIR / "raw"           # JSON lines returned by the scraper
ENRICHED_DIR = DATA_DIR / "enriched"          # final enriched tables (Spark)
