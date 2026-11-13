"""Shared paths and dataset definitions."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"            # downloaded .tsv.gz files
BRONZE_DIR = DATA_DIR / "bronze"      # Parquet, all columns as strings
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
