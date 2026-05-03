# Simple Python ETL — NYC Yellow Taxi

A **production-leaning ETL pipeline** that downloads, validates, cleans, and enriches the NYC Yellow Taxi monthly dataset. Built as a portfolio project to demonstrate core Data Engineering practices — not at scale, but with the same operational discipline expected in production systems.

---

## Data Engineering principles demonstrated

### 1. Schema contracts at every data boundary
Most pipelines validate data at the end, if at all. This pipeline enforces two explicit Pandera schemas:

- **`RAW_SCHEMA`** — applied immediately after reading the parquet file, before any transformation. Catches upstream format changes (missing columns, wrong dtypes, nulls in required fields) at their origin.
- **`PROCESSED_SCHEMA`** — applied after all feature engineering, before writing to disk. Guarantees that what gets persisted is exactly what downstream consumers expect — no silent corruption from a broken transform.

Validation uses `lazy=True` so all failures are collected before raising, not just the first one. Error output is bounded to prevent megabyte-scale stack traces on large datasets.

### 2. Explicit data quality enforcement with tracked drop counts
Three cleaning rules reject rows that would corrupt downstream metrics:

| Rule | What it catches |
|---|---|
| `remove_invalid_passengers` | Trips with zero or negative passenger count |
| `remove_invalid_distances` | Trips with zero or negative distance |
| `remove_invalid_durations` | Trips where dropoff precedes or equals pickup |

Each rule is an isolated, independently tested function. Drop counts per rule are tracked across the entire cleaning pass and written to the run summary — so you can see exactly where data quality issues concentrate, not just how many rows were lost in total.

### 3. Fault-tolerant run metadata
Every execution produces a structured JSON summary in `logs/YYYYMMDD/`, written in a `finally` block — meaning it is recorded whether the pipeline succeeds or fails mid-run. A failed run with a partial summary is more useful for debugging than no record at all.

Real output from a completed run:
```json
{
  "run_id": "861e25cf-75cc-4f3c-bed1-3bcefbd1a0f9",
  "started_at": "2026-05-03 20:59:06.139387+00:00",
  "ended_at": "2026-05-03 20:59:26.094844+00:00",
  "status": "success",
  "error": null,
  "input_url": "https://.../yellow_tripdata_2026-03.parquet",
  "raw_path": "data/raw/yellow_tripdata_2026-03.parquet",
  "processed_path": "data/processed/prcsd_yellow_tripdata_2026-03.parquet.gzip",
  "rows_in": 3952451,
  "rows_out": 2911018,
  "dropped_by_reason": {
    "invalid_passengers": 958715,
    "invalid_distances": 35157,
    "invalid_durations": 47561
  },
  "pickup_min": "2026-03-01 00:00:17",
  "pickup_max": "2026-03-31 23:59:54",
  "git_sha": "e24ff54ec5101d14e648f8e9a8f72b2aba145e20"
}
```

The git SHA ties every output file to the exact version of the code that produced it — critical when the pipeline runs on a schedule.

### 4. Robust extraction with bounded upstream probing
The NYC TLC dataset is published with a lag — the current month is often not yet available. Rather than hardcoding a date or failing immediately, `get_latest_data_url()` probes months backwards via HTTP `HEAD` (no download cost) until it finds a `200 OK`, bounded by a configurable threshold.

Runs are fully deterministic without manual date updates. If the source is unavailable for longer than the threshold, the pipeline fails fast with a clear error.

### 5. Memory-efficient streaming I/O
The source files are large (the March 2026 dataset has ~3.9M rows). The download uses `requests.get(..., stream=True)` with chunked writes — memory usage stays flat regardless of file size. Processed output is written as gzip-compressed Parquet.

### 6. Separation of concerns and testability
`extract`, `transform`, and `load` are independent modules with no knowledge of each other. `pipeline.py` is the only file that knows the full sequence. Each step can be tested, replaced, or extended in isolation.

27 tests cover URL construction, bounded retry logic, all cleaning rules, payment normalization, output path derivation, and schema acceptance/rejection — without mocking the entire pipeline.

### 7. Traceable artifact lineage
Every output filename encodes its source: `prcsd_yellow_tripdata_2026-03.parquet.gzip` maps unambiguously to `yellow_tripdata_2026-03.parquet`. No metadata store required to answer "where did this file come from."

### 8. Operational observability
Every run writes to both console and a timestamped log file under `logs/YYYYMMDD/`. Format includes module, function, and line number. Log level is configurable via environment variable. The run summary JSON and log file share the same date folder, so all artifacts from a single run are co-located.

---

## Architecture and data flow

```text
  .env (BASE_URL, LOG_LEVEL)
            │
            ▼
  pipeline.py ──────────────────────────────────────────────────────┐
       │                                                             │
       │  ┌─ extract.py ──────────────────────────────────────┐     │
       │  │  HEAD probe (current month → backwards, max N)    │     │
       │  │  GET + chunked write                              │     │
       │  │  → data/raw/yellow_tripdata_YYYY-MM.parquet       │     │
       │  └───────────────────────────────────────────────────┘     │
       │                                                             │
       │  ┌─ transform.py ────────────────────────────────────┐     │
       │  │  RAW_SCHEMA.validate()                            │     │
       │  │  clean_data()  → drop counts per rule             │     │
       │  │  RAW_SCHEMA.validate()  (post-clean)              │     │
       │  │  add_trip_duration()                              │     │
       │  │  add_time_features()                              │     │
       │  │  normalize_payment_type()                         │     │
       │  │  PROCESSED_SCHEMA.validate()                      │     │
       │  └───────────────────────────────────────────────────┘     │
       │                                                             │
       │  ┌─ load.py ─────────────────────────────────────────┐     │
       │  │  → data/processed/prcsd_<source_filename>.gzip    │     │
       │  └───────────────────────────────────────────────────┘     │
       │                                                             │
       │                               ┌─ logging_config.py ────┐   │
       └───────────────────────────────┤  logs/YYYYMMDD/*.log   │   │
                                       ├─ run_context.py ───────┤   │
                                       │  logs/YYYYMMDD/        │   │
                                       │  run_summary_*.json    │   │
                                       └────────────────────────┘   │
                                                                     │
       finally block (success or failure) ──────────────────────────┘
```

---

## Tech stack

| Library | Role |
|---|---|
| **Python 3.13** | Runtime |
| **Pandas** | DataFrame transformations and Parquet I/O |
| **Pandera** | Schema validation at raw and processed boundaries |
| **PyArrow** | Parquet engine |
| **Requests** | HTTP HEAD probing and streaming download |
| **python-dateutil** | Month arithmetic for backwards probing |
| **python-dotenv** | Environment-based configuration |
| **pytest** | 27 tests across extract, transform, load, and schemas |

Pinned versions in `requirements.txt`.

---

## Project structure

```text
simple_python_etl/
├── data/
│   ├── raw/                  # Downloaded source Parquet files
│   └── processed/            # Cleaned, enriched outputs (gzip-compressed Parquet)
├── logs/
│   └── YYYYMMDD/             # Per-day folder
│       ├── HHMMSS-ffffff.log         # Execution log
│       └── run_summary_<ts>.json     # Structured run metadata
├── tests/
│   ├── conftest.py           # sys.path setup for test discovery
│   ├── test_extract.py       # URL construction and bounded retry logic
│   ├── test_transform.py     # Cleaning rules and payment normalization
│   ├── test_load.py          # Output path and filename derivation
│   └── test_schemas.py       # Schema acceptance and rejection cases
└── src/
    ├── pipeline.py           # Orchestration: extract → transform → load
    ├── extract.py            # URL resolution, bounded probe, streaming download
    ├── transform.py          # Data cleaning and feature engineering
    ├── load.py               # Persist processed dataframe to disk
    ├── run_context.py        # Run metadata container and JSON writer
    ├── schemas.py            # Pandera RAW_SCHEMA and PROCESSED_SCHEMA
    ├── utils.py              # Bounded Pandera error summarization
    └── logging_config.py     # Console + file handler setup
```

---

## Quickstart

**Prerequisites:** Python 3.13+

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set `BASE_URL` in `.env`:
```
BASE_URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata
```

```bash
python src/pipeline.py
```

The pipeline resolves the latest available month automatically.

---

## Engineered features

| Column | Description |
|---|---|
| `trip_duration_minutes` | Dropoff − pickup in minutes |
| `hour_of_day` | Pickup hour (0–23) |
| `day_of_week` | Pickup weekday name (e.g. `Monday`) |
| `payment_type` | Human-readable label mapped from numeric code (e.g. `credit_card`) |

---

## Testing

```bash
pytest
```

---

## Roadmap

- CLI with `--year`, `--month`, `--latest` flags and proper exit codes for scheduler use
- Skip extraction and processing if the output already exists for the target month
- Automate via cron with failure alerting
