# IMDb Big Data Processing Project

## Contributors

| Name | Email | Git username |
| --- | --- | --- |
| Julia Ezzedine | julia.ezzedine@edu.ece.fr | JuliaEzzedine |
| Talia Toutah | talia.toutah@edu.ece.fr | taliaalg |

## Requirements

- Docker Desktop (or Docker Engine + Compose v2)
- At least 8 GB of RAM allocated to Docker and about 15 GB of free disk space
- Internet access (IMDb download, Wikipedia API, Wikimedia event stream)

## Environment setup

```bash
git clone <repo-url>
cd <repo-folder>
docker compose build
```

All commands below are run inside the `spark` container with
`docker compose run --rm spark <command>`. The repository is mounted at `/app`,
so data and outputs are written to `./data` and `./outputs` on the host.

The Spark driver memory defaults to 4 GB. To change it:

```bash
SPARK_DRIVER_MEMORY=6g docker compose run --rm spark <command>
```

## 1. Data preparation

```bash
# Download the 7 IMDb files into data/raw/ (about 1.5 GB, skipped if already present)
docker compose run --rm spark python -m src.prep.download

# Convert them to Parquet (data/bronze/), all columns kept as raw strings
docker compose run --rm spark python -m src.prep.ingest

# Percentage of null / missing / empty values per column -> outputs/null_report.md
docker compose run --rm spark python -m src.prep.null_report

# Data quality checks + handling -> data/silver/, data/quarantine/, outputs/quality_report.md
docker compose run --rm spark python -m src.prep.quality
```

All later batch steps read the cleaned tables from `data/silver/`.

## 2. Data exploration

TODO

## 3. Data enrichment

Requires the silver tables from step 1. Data is collected from Wikidata (to find the
English Wikipedia article of an IMDb id) and English Wikipedia (synopsis = "Plot"
section, biography = article introduction).

```bash
# Select the top 10 feature films per genre and year (1980-2025) and their first 3 principals
docker compose run --rm spark python -m src.enrich.targets

# Scrape Wikidata + Wikipedia (about 1 h, polite: one request at a time, >= 0.5 s apart).
# Set a contact email for the User-Agent required by the Wikimedia API policy.
# PowerShell:  $env:WIKI_CONTACT="you@school.fr"     bash:  export WIKI_CONTACT=you@school.fr
docker compose run --rm spark python -m src.enrich.fetch_wikipedia

# The scraper is resumable: if it is interrupted, run the same command again.
# Quick test on 20 films / people only:
#   docker compose run --rm spark python -m src.enrich.fetch_wikipedia --limit 20

# Clean, enforce the schema and build the enriched tables -> data/enriched/, outputs/enrichment_summary.md
docker compose run --rm spark python -m src.enrich.build
```

## 4. Streaming

```bash
# Start Kafka (only needed for this part)
docker compose --profile streaming up -d kafka

# Build the IMDb watchlist (titles with 5000+ votes and the people known for them)
# Downloads title.basics, title.ratings and name.basics into data/raw/ if missing
docker compose run --rm spark python -m src.streaming.watchlist

# Terminal 1: Spark Structured Streaming job (results in outputs/streaming/)
docker compose run --rm spark python -m src.streaming.stream_job --starting-offsets earliest --minutes 32

# Terminal 2: send live Wikipedia change events to Kafka for 30 minutes
docker compose run --rm spark python -m src.streaming.producer --minutes 30

# Summary of the run -> outputs/streaming/summary.md
docker compose run --rm spark python -m src.streaming.summary
```

Watermark test with replayed events, some of them late (run the job first, wait for
the watchlist to load, then send the events):

```bash
docker compose run --rm spark python -m src.streaming.stream_job --topic wiki-test --starting-offsets earliest --output outputs/streaming_test --trigger "10 seconds"
docker compose run --rm spark python -m src.streaming.producer --replay src/streaming/sample_events.jsonl --topic wiki-test
```

## 5. Open question

TODO

## Use of AI / LLM tools

| Tool | Used for |
| --- | --- |
| Claude (Anthropic) | Generating code for the Docker environment, the IMDb download script, the Parquet ingestion job and the null-value report (`Dockerfile`, `docker-compose.yml`, `src/common/`, `src/prep/download.py`, `src/prep/ingest.py`, `src/prep/null_report.py`) |
| Claude (Anthropic) | Generating the data quality job (`src/prep/quality.py`) and the enrichment pipeline (`src/enrich/`): target selection, Wikidata/Wikipedia scraper, Spark build of the enriched tables |
| Claude (Anthropic) | Generating the streaming code (`src/streaming/`): Kafka producer, watchlist builder, Spark Structured Streaming job, run summary and test events |

No AI tool was used to write the explanations or reasoning in `REPORT.md`.