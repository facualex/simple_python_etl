## Simple Python ETL — NYC Yellow Taxi

A **production-leaning Python ETL pipeline** built as a portfolio project for Data Engineering roles. It extracts the most recent NYC Yellow Taxi monthly dataset, validates and transforms it, and persists a cleaned, feature-enriched output locally — with schema validation at every boundary, structured logging per execution, and clear separation across pipeline stages.

The focus is not on scale, but on the practices that matter in real DE work: schema contracts, data quality enforcement, fault tolerance, operational observability, and traceable artifacts.

---

## What this demonstrates

### Schema-first data validation
Two Pandera schemas (`RAW_SCHEMA`, `PROCESSED_SCHEMA`) enforce contracts at the two critical boundaries: after ingestion and after feature engineering. Both apply before the next stage runs, so failures surface at their origin rather than propagating silently downstream. Bounded error summaries prevent megabyte-scale stack traces when validation fails on large datasets.

### Robust extraction with bounded retries
The NYC TLC dataset is published with an upstream lag — the current month is often not yet available. `get_latest_data_url()` resolves this by probing months backwards via HTTP `HEAD` (no download cost) until it finds a `200 OK`, bounded by a configurable threshold. Runs are deterministic and require no manual date updates.

### Streaming I/O
The download uses `requests.get(..., stream=True)` with chunked writes, keeping memory usage flat regardless of file size. Processed output is written as gzip-compressed Parquet for efficient storage.

### Data quality enforcement
Three explicit cleaning steps reject rows that would corrupt downstream metrics: non-positive passenger counts, zero/negative trip distances, and trips where dropoff precedes pickup. Each is an isolated, testable function.

### Separation of concerns
`extract`, `transform`, and `load` are independent modules with narrow responsibilities. `pipeline.py` is the only place that knows the full sequence — each step can be tested, replaced, or extended without touching the others.

### Traceable artifact lineage
`transform()` returns `(df, raw_path)` and `load()` derives the output filename from `raw_path.name`, producing names like `prcsd_yellow_tripdata_2026-03.parquet.gzip`. Every processed file can be traced back to its exact source without a metadata store.

### Operational logging
Every run writes to both console and a timestamped file under `logs/YYYYMMDD/`, with microsecond precision in the filename to avoid collisions. Log level is configurable via environment variable. Structured format includes module, function, and line number for fast debugging.

### Configuration-driven design
The extraction endpoint is driven by `BASE_URL` in a `.env` file. The pipeline fails fast with a clear error when configuration is missing — no silent defaults that cause confusing behavior in production.

---

## Architecture and data flow

```text
  .env (BASE_URL, LOG_LEVEL)
            |
            v
  src/pipeline.py (orchestration)
      |     |      |
      |     |      +--> src/logging_config.py  --> logs/YYYYMMDD/HHMMSS-ffffff.log
      |     |
      |     +--> src/extract.py
      |               HEAD probe (bounded retry) --> remote endpoint
      |               streaming download         --> data/raw/yellow_tripdata_YYYY-MM.parquet
      |
      +--> src/transform.py
      |         RAW_SCHEMA validation
      |         cleaning (passengers, distances, durations)
      |         RAW_SCHEMA re-validation (post-clean)
      |         feature engineering (duration, time features, payment labels)
      |         PROCESSED_SCHEMA validation
      |         |
      |         v
      +--> src/load.py --> data/processed/prcsd_<raw_filename>.gzip
```

---

## Tech stack

| Library | Role |
|---|---|
| **Python 3.13** | Runtime |
| **Pandas** | Transformations and Parquet I/O |
| **Pandera** | Schema validation at ingestion and post-transform boundaries |
| **PyArrow** | Parquet engine |
| **Requests** | HTTP probing + streaming download |
| **python-dateutil** | Month arithmetic for backward probing |
| **python-dotenv** | Environment-based configuration |
| **pytest** | Test runner |

Exact pinned versions in `requirements.txt`.

---

## Project structure

```text
simple_python_etl/
├── data/
│   ├── raw/                  # Downloaded Parquet source files
│   └── processed/            # Cleaned + enriched Parquet outputs (gzip)
├── src/
│   ├── pipeline.py           # Orchestration entrypoint (extract → transform → load)
│   ├── extract.py            # URL resolution, bounded retry, streaming download
│   ├── transform.py          # Data cleaning + feature engineering
│   ├── load.py               # Write transformed dataframe to processed zone
│   ├── schemas.py            # Pandera schemas for raw and processed data
│   ├── utils.py              # Schema error summarization utilities
│   └── logging_config.py     # Console + file logging setup
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Quickstart

**Prerequisites:** Python 3.13+

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

```bash
cp .env.example .env
# Set BASE_URL in .env:
# BASE_URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata
```

```bash
python src/pipeline.py
```

The pipeline auto-resolves the latest available month. No manual date updates needed.

---

## Run a specific month (programmatic)

```bash
python - <<'PY'
from dotenv import load_dotenv
from extract import extract
from logging_config import configure_logging
from transform import transform
from load import load

load_dotenv()
configure_logging()

raw_path = extract(year=2026, month=3)
load(transform(raw_path))
PY
```

---

## Outputs

| Artifact | Location | Example |
|---|---|---|
| Raw dataset | `data/raw/` | `yellow_tripdata_2026-03.parquet` |
| Processed dataset | `data/processed/` | `prcsd_yellow_tripdata_2026-03.parquet.gzip` |
| Execution log | `logs/YYYYMMDD/` | `143022-482910.log` |

**Engineered features added during transform:**

| Column | Description |
|---|---|
| `trip_duration_minutes` | Dropoff − pickup time in minutes |
| `hour_of_day` | Pickup hour (0–23) |
| `day_of_week` | Pickup weekday name (e.g. `Monday`) |
| `payment_type` | Human-readable label mapped from numeric code |

---

## Testing

```bash
pytest
```

---

## Roadmap

- Add a CLI (`--year`, `--month`, `--latest`) with proper exit codes for operational use
- Write a `run_summary.json` per execution (row counts, dropped rows by reason, input/output paths, git SHA)
- Add unit tests for URL resolution, transform logic, and schema validation paths
- Skip extraction and processing if the output already exists for the target month
- Schedule via cron and add run alerting on failure
