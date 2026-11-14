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
```

## 2. Data exploration

TODO

## 3. Data enrichment

TODO

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
| Claude (Anthropic) | Generating the streaming code (`src/streaming/`): Kafka producer, watchlist builder, Spark Structured Streaming job, run summary and test events |

No AI tool was used to write the explanations or reasoning in `REPORT.md`.