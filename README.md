## Simple Python ETL (NYC Yellow Taxi)

A small, production-leaning **Python ETL pipeline** that:
- **Extracts** the most recent available NYC Yellow Taxi *monthly* Parquet file from a configurable endpoint
- **Transforms** it with **Pandas** (data cleaning + feature engineering)
- **Loads** a **compressed Parquet** output into a local “processed” zone
- **Logs** every run to both console and timestamped files for auditing

This project is intentionally lightweight (no external services) but showcases core **Data Engineering** practices: **configuration**, **idempotent runs**, **data lineage through filenames**, **bounded retries**, and **operational logging**.

---

## What this demonstrates (Data Engineering focus)

- **Robust extraction**: resolves “latest available month” safely (handles the common upstream lag where the current month is not published yet)
- **Separation of concerns**: clear module boundaries for `extract` / `transform` / `load`
- **Reproducible artifacts**: deterministic output naming that keeps a trace to the exact source file used
- **Operational readiness**: log files per execution, with a predictable folder structure (`logs/YYYYMMDD/`)
- **Efficient I/O**: streaming download avoids loading large files into memory

---

## Architecture and data flow

```text
  .env (BASE_URL, LOG_LEVEL)
            |
            v
  src/pipeline.py (orchestration)
      |     |      |
      |     |      +--> src/logging_config.py  -> logs/YYYYMMDD/HHMMSS-ffffff.log
      |     |
      |     +--> src/extract.py  -> data/raw/yellow_tripdata_YYYY-MM.parquet
      |
      +--> src/transform.py -> (df, raw_path)
                 |
                 v
           src/load.py -> data/processed/prcsd_<raw_filename>.gzip
```

---

## Key design decisions

### Latest-month resolution (bounded probe window)
- **Why**: upstream sources often publish monthly files with delay; hardcoding “current month” makes runs flaky.
- **How**: `src/extract.py:get_latest_data_url()` probes from current month backward using HTTP `HEAD` until it finds `200 OK`, bounded by `max_backwards_threshold=6`.
- **Benefit**: repeatable runs without manual month updates, with deterministic failure if availability/naming assumptions change.

### Configuration-driven URL construction
- **Why**: extraction endpoints vary by environment; code changes should not be required.
- **How**: `src/extract.py:build_url()` reads `BASE_URL` (via `python-dotenv`) and constructs the remote filename pattern.
- **Benefit**: fails fast with a clear error when configuration is missing.

### Streaming download to disk
- **Why**: Parquet files can be large; downloading into memory is unnecessary.
- **How**: `requests.get(..., stream=True)` + chunked writes.
- **Benefit**: stable memory usage during extraction.

### Traceable outputs via filename lineage
- **Why**: in data pipelines, outputs must be attributable to a specific input.
- **How**: `src/transform.py:transform()` returns `(df, raw_path)`; `src/load.py:load()` derives the processed filename from `raw_path.name`.
- **Benefit**: artifact lineage without needing a metadata store for this scope.

---

## Tech stack

- **Python**: tested locally with Python 3.13
- **Pandas**: transformations and Parquet I/O
- **PyArrow**: Parquet engine
- **Requests**: HTTP probing + streaming download
- **python-dateutil**: month arithmetic for backward probing
- **python-dotenv**: environment-based configuration
- **pytest**: test runner dependency (tests folder exists; currently no discovered test modules)

Exact pinned versions live in `requirements.txt`.

---

## Project structure

```text
simple_python_etl/
├── data/
│   ├── raw/                  # Downloaded Parquet source files
│   └── processed/            # Processed Parquet outputs (gzip-compressed)
├── logs/
│   └── YYYYMMDD/             # One folder per day; one file per execution
├── src/
│   ├── extract.py            # URL construction, latest-month resolution, streaming download
│   ├── transform.py          # Cleaning + feature engineering with Pandas
│   ├── load.py               # Write transformed dataframe to processed zone
│   ├── logging_config.py     # Console + file logging setup
│   └── pipeline.py           # ETL orchestration entrypoint
├── requirements.txt          # Pinned dependencies
├── .env.example              # BASE_URL and LOG_LEVEL example
└── .state.json               # Present in repo; not referenced by runtime code
```

---

## Quickstart

### Prerequisites
- Python 3.13+

### Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configure

```bash
cp .env.example .env
```

Minimal `.env` example:

```bash
BASE_URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata
LOG_LEVEL=INFO
```

### Run (automatic latest available month)

```bash
source .venv/bin/activate
python src/pipeline.py
```

---

## Run for a specific year/month (programmatic)

There is no CLI argument parser yet; you can run a specific month by calling `extract(year=..., month=...)` directly:

```bash
source .venv/bin/activate
python - <<'PY'
from dotenv import load_dotenv
from extract import extract
from logging_config import configure_logging
from transform import transform
from load import load

load_dotenv()
configure_logging()

raw_path = extract(year=2026, month=3)
transformed = transform(raw_path)
load(transformed)
PY
```

---

## Outputs

### Generated files

1. **Raw dataset (Parquet)**
   - **Location**: `data/raw/`
   - **Produced by**: `src/extract.py:extract()`
   - **Example**: `data/raw/yellow_tripdata_2026-03.parquet`

2. **Processed dataset (gzip-compressed Parquet)**
   - **Location**: `data/processed/`
   - **Produced by**: `src/load.py:load()`
   - **Naming**: `prcsd_<raw_filename>.gzip`
   - **Example**: `data/processed/prcsd_yellow_tripdata_2026-03.parquet.gzip`
   - **Includes engineered features such as**:
     - `trip_duration_minutes`
     - `hour_of_day`
     - `day_of_week`
     - `payment_type` (mapped from numeric codes to labels)

3. **Execution logs**
   - **Location**: `logs/YYYYMMDD/`
   - **Produced by**: `src/logging_config.py:configure_logging()`
   - **Naming**: `HHMMSS-ffffff.log` (microsecond precision to avoid collisions)

### Summary report

No JSON run summary is generated at the moment; execution outcomes are captured in the log files under `logs/`.

---

## Testing

```bash
source .venv/bin/activate
pytest
```

Note: `tests/` exists, but currently no test modules are discovered by pytest.

---

## Roadmap (next improvements)

- Add a small CLI (`--year`, `--month`, `--latest`) and exit codes for operational use
- Introduce data quality checks (schema validation + expectations-style rules)
- Add a run summary artifact (JSON) with row counts, null counts, and input/output paths
- Partition processed outputs (e.g., by pickup date) and/or write to a local warehouse format
- Add unit tests for URL building, latest-month probing, and transform logic

