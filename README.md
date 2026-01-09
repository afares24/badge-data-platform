# Badge Data Platform

End-to-end data platform demonstrating streaming ingestion, file-based storage, automatic compaction, and real-time analytics.

## Architecture

```
FastAPI (synthetic data) → API Client → Pipeline → Parquet Data Lake → DuckDB → Streamlit
                              ↑                           ↑
                        retry/backoff              compaction monitor
```

## Components

**Data Generation** — FastAPI server generates synthetic employee badge logs with configurable batch sizes (up to 100k records). Simulates realistic distributions across departments and buildings.

**Ingestion Pipeline** — Buffered writes (100k records) to Snappy-compressed Parquet. Atomic writes via temp directory staging to prevent partial file reads.

**Compaction** — Watchdog-based file monitor triggers compaction at 100+ files. Snapshots file list before read to prevent race conditions. Targets 100MB-500MB compacted files.

**Query Layer** — DuckDB for analytical queries over Parquet files. No external database required.

**Dashboard** — Streamlit app with access distribution, compliance tracking, and query performance monitoring.

## Tech Stack

- Python 3.13
- FastAPI / Uvicorn
- DuckDB
- PyArrow / Parquet
- Watchdog
- Streamlit / Plotly

## Run Locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Terminal 1: Start API server
uvicorn api.main:app --reload

# Terminal 2: Run pipeline
python pipeline.py

# Terminal 3: Launch dashboard
streamlit run analytics_app.py
```

## Configuration

```bash
# .env
DATA_LAKE_PATH=/path/to/data_lake
EMPLOYEE_BADGE_LOGS_URL=http://localhost:8000/employee-badge-logs/
```

## Project Structure

```
├── api/
│   ├── main.py          # FastAPI routes
│   ├── generators.py    # Synthetic data generation
│   └── client.py        # API client with exponential backoff
├── pipeline.py          # Ingestion + analytics queries
├── compaction_monitoring.py
├── analytics_app.py     # Streamlit dashboard
└── utils/tools.py       # Decorators (timing, sql_monitoring)
```

## Implementation Details

**Atomic Writes** — Files write to `temp/` then rename to `landing/`. Rename is atomic on POSIX, ensuring readers never see partial files.

**Compaction Strategy** — Event-driven via watchdog. Snapshots file list before COPY to prevent deleting files that land mid-compaction.

**Retry Logic** — Exponential backoff (3^n seconds) with configurable max attempts. Separates transient network errors from application errors.

**Query Monitoring** — `@sql_monitoring` decorator captures query execution time, displayed in dashboard.
