# IMDb Big Data Processing Project

## Contributors

| Name | Email | Git username |
| --- | --- | --- |
| Julia Ezzedine | TODO@edu.ece.fr | JuliaEzzedine |
| Talia TODO | TODO@edu.ece.fr | TODO |

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

TODO — Kafka is started only for this part:

```bash
docker compose --profile streaming up -d kafka
```

## 5. Open question

TODO

## Use of AI / LLM tools

| Tool | Used for |
| --- | --- |
| Claude (Anthropic) | Generating code for the Docker environment, the IMDb download script, the Parquet ingestion job and the null-value report (`Dockerfile`, `docker-compose.yml`, `src/common/`, `src/prep/download.py`, `src/prep/ingest.py`, `src/prep/null_report.py`) |

No AI tool was used to write the explanations or reasoning in `REPORT.md`.
