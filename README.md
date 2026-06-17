# Football Analytics Pipeline

Prospect Sport AI — Data Engineering technical assessment.

Modular ETL pipeline on the [Kaggle Football Player Scores](https://www.kaggle.com/datasets/davidcariboo/player-scores/data) dataset using **Medallion Architecture** (Bronze → Silver → Gold), data quality checks, SCD Type 2, Airflow orchestration, and Docker.

## How to run

All commands assume you are in the project root:

```bash
cd /path/to/project/prospect
```

### Prerequisites

- **pyenv** with Python **3.14.5** (pinned in [`.python-version`](.python-version))

```bash
pyenv install -s 3.14.5   # first time only
python3 --version         # Python 3.14.5
```

### One-time setup

```bash
make venv
source .venv/bin/activate
make reqs
cp .env.example .env      # optional — defaults work for local runs
```

### Run tests

```bash
make test                 # full pytest suite (64+ tests)
make test-cov             # pytest with coverage report
make lint                 # ruff check
```

### Run the pipeline

Steps not yet implemented are marked *coming soon*.

| Step | Command | Output |
|------|---------|--------|
| **Bronze ingest** (day1) | `make ingest` | `data/bronze/data_day1/*.parquet` |
| **Bronze ingest** (day2) | `make ingest-day2` | `data/bronze/data_day2/*.parquet` |
| **DQ checks** (day1) | `make dq` | `reports/dq/dq_report_*.json` |
| **DQ checks** (day2) | `make dq-day2` | validates `data/bronze/data_day2/` |
| **Silver transform** (day1) | `make silver` | `data/silver/*.parquet` |
| **Silver transform** (day2) | `make silver-day2` | merges into `data/silver/` (SCD + new facts) |
| **Gold aggregates** | `make gold` | `data/gold/*.parquet` (rebuilds from silver) |
| **Full pipeline** (day1) | `make pipeline` | ingest → dq → silver → gold |
| **Full pipeline** (day2) | `make pipeline-day2` | incremental end-to-end |
| **Incremental runner** | `make incremental` | stateful day1 → day2, skips processed |
| **Reset checkpoint** | `make pipeline-reset` | clears `data/state/pipeline_state.json` |
| **Airflow UI** | `make docker-up` | http://localhost:8080 (airflow / airflow) |
| **Pipeline in Docker** | `make docker-pipeline` | same outputs as `make incremental` |

**Bronze (equivalent commands):**

```bash
# Baseline snapshot (initial load)
make ingest
# or
python -m src.ingestion

# Incremental snapshot (new games, SCD test data)
make ingest-day2
# or
DATA_SNAPSHOT=data_day2 python -m src.ingestion
```

**DQ (run after bronze, before silver):**

```bash
make dq
# or
python -m src.dq

# Day2 bronze
make dq-day2
```

DQ reads bronze parquet for the active snapshot, runs config-driven checks, and writes JSON reports under `reports/dq/`. Set `DQ_FAIL_ON_ERROR=false` to produce a report without aborting.

**Silver (run after bronze + DQ for the same snapshot):**

```bash
# Initial star schema from day1 bronze
make silver
# or
python -m src.transform

# Merge day2 changes (SCD Type 2, new games/appearances)
make ingest-day2 && make silver-day2
# or
DATA_SNAPSHOT=data_day2 python -m src.transform
```

Recommended order for the full day1 → day2 flow:

```bash
make pipeline
# or step-by-step:
make ingest && make dq && make silver && make gold

make pipeline-day2
# or:
make ingest-day2 && make dq-day2 && make silver-day2 && make gold
```

**Incremental (stateful — recommended for production-style runs):**

```bash
make incremental
# First run  → processes data_day1 then data_day2
# Second run → skips both (unchanged content hashes)

make pipeline-reset   # clear checkpoint
PIPELINE_FORCE=true make incremental   # full rebuild from scratch (same end state)
```

### Airflow + Docker (Step 7)
**One-time Docker setup** :

```bash
cp .env.example .env
# Then edit .env and set the required secrets:
#   POSTGRES_PASSWORD=<strong password>
#   FERNET_KEY — generate with:
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
#   On macOS, set AIRFLOW_UID to your uid: id -u
```

> **Never commit `.env`** — it is git-ignored. Use `.env.example` as the template; it contains placeholder values for all required secrets.

**Build and start Airflow:**

```bash
make docker-build
make docker-up
```

Open http://localhost:8080 and sign in with:

> Not `admin` — the init container creates user **`airflow`** (see `_AIRFLOW_WWW_USER_USERNAME` in `.env`).

Then unpause `football_analytics_pipeline` and trigger or wait for the `@hourly` schedule.

**Run the pipeline without the scheduler** (standalone container):

```bash
make docker-pipeline          # incremental runner
make docker-pipeline-reset    # clear checkpoint inside Docker
make docker-down              # stop the cluster
```

The DAG (`dags/football_analytics_pipeline.py`) chains tasks that call the same code as the local runner:

`bronze (+ cumulative merge) → DQ → silver → gold → checkpoint update`

Task wrappers live in `src/pipeline/airflow_tasks.py`.

**Gold (run after silver):**

```bash
make gold
# or
python -m src.transform.gold
```

Gold always rebuilds from the cumulative `data/silver/` tables (idempotent full refresh).

After bronze ingest, check outputs:

```bash
ls data/bronze/data_day1/
cat data/bronze/data_day1/_ingest_manifest.json

# Silver outputs (7 tables + manifest)
ls data/silver/
cat data/silver/_transform_manifest.json

# Gold outputs (3 marts + manifest)
ls data/gold/
cat data/gold/_aggregate_manifest.json

# DQ report
cat reports/dq/dq_report_data_day1_latest.json
```

### Environment variables

Set in `.env` or export before running:

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATA_SNAPSHOT` | `data_day1` | Input folder: `data_day1` or `data_day2` |
| `ENGINE` | `pandas` | Processing engine (`pandas` or `spark` later) |
| `LOG_LEVEL` | `INFO` | Pipeline logging verbosity |
| `ENVIRONMENT` | `local` | Runtime label in config |
| `DQ_FAIL_ON_ERROR` | `true` | Exit non-zero when DQ checks fail |
| `PIPELINE_FORCE` | `false` | Full rebuild: clears state, silver, cumulative bronze |
| `PIPELINE_RESET` | `false` | Set `true` with `make pipeline-reset` to clear state |

### Makefile reference

| Target | Description |
|--------|-------------|
| `make venv` | Create `.venv` with pyenv Python 3.14.5 |
| `make reqs` | Install `requirements.txt` + dev deps |
| `make test` | Run pytest |
| `make test-cov` | Run pytest with coverage report |
| `make lint` | Run ruff |
| `make ingest` | Bronze ingest from `data_day1` |
| `make ingest-day2` | Bronze ingest from `data_day2` |
| `make dq` | DQ checks on `data_day1` bronze |
| `make dq-day2` | DQ checks on `data_day2` bronze |
| `make pipeline` | Full day1: ingest → dq → silver → gold |
| `make pipeline-day2` | Full day2 incremental pipeline |
| `make incremental` | Stateful runner — processes pending snapshots only |
| `make pipeline-reset` | Delete pipeline checkpoint state |
| `make silver` | Silver transform from `data_day1` bronze |
| `make silver-day2` | Silver transform from `data_day2` bronze (cumulative) |
| `make gold` | Gold aggregates from cumulative silver |
| `make docker-build` | Build custom Airflow + pipeline image |
| `make docker-up` | Start Airflow cluster (CeleryExecutor) |
| `make docker-down` | Stop Airflow cluster |
| `make docker-logs` | Tail compose logs |
| `make docker-pipeline` | Run incremental pipeline in a container |
| `make docker-pipeline-reset` | Reset checkpoint via Docker pipeline service |

## Project layout

```
prospect/
├── config/
│   ├── config.yaml          # Main pipeline config
│   └── schemas/bronze.yaml  # Bronze column schemas
├── data/
│   ├── data_day1/           # Baseline CSV snapshot
│   ├── data_day2/           # Incremental snapshot (SCD / new games)
│   ├── bronze/              # Generated — Step 2
│   ├── silver/              # Generated — Step 3
│   └── gold/                # Generated — Step 4
├── dags/
│   └── football_analytics_pipeline.py
├── src/
│   ├── config_loader.py
│   ├── engine/              # Pandas ↔ Spark swap boundary
│   ├── ingestion/           # Bronze — Step 2
│   ├── transform/           # Silver + Gold — Steps 3–4
│   ├── dq/                  # Data quality — Step 5
│   ├── load/                # Parquet writers
│   └── pipeline/            # Incremental runner + Airflow task wrappers
├── tests/
├── Dockerfile               # Extends apache/airflow:3.2.2 with pipeline deps
├── docker-compose.yml       # In-repo Airflow cluster + pipeline profile
├── requirements-airflow.txt
└── plugins/                 # Optional Airflow plugins mount
```

## Configuration

- **`config/config.yaml`** — paths, tables, DQ rules, SCD settings, orchestration defaults
- **`config/schemas/bronze.yaml`** — bronze column schemas enforced on ingest
- **`.env`** — see [Environment variables](#environment-variables) above

Bronze ingest writes one parquet per table plus `_ingest_manifest.json` (row counts, schema drift, timestamp).

Silver transform builds a cumulative star schema under `data/silver/`:

| Table | Type | Notes |
|-------|------|-------|
| `dim_date` | Dimension | Calendar + derived season |
| `dim_competitions` | Dimension | Competition reference |
| `dim_clubs` | Dimension | Club reference |
| `dim_players` | SCD Type 2 | Club / market-value history; soft-delete closes current row |
| `dim_player_valuations` | SCD Type 2 | Valuation history with effective/end dates |
| `fact_games` | Fact | Win/loss/draw, season, dimension keys |
| `fact_appearances` | Fact | Player appearance metrics linked to dims |

`_transform_manifest.json` records snapshot, as-of date, and row counts per table.

Gold aggregation builds analyst-ready marts under `data/gold/`:

| Table | Grain | Metrics |
|-------|-------|---------|
| `gold_player_performance` | player × season | matches, goals, assists, minutes |
| `gold_club_performance` | club × season | wins, losses, draws, goals for/against |
| `gold_player_valuation_trend` | player × date | market value, rolling average |

`_aggregate_manifest.json` records build timestamp and row counts per mart.

Data quality runs on bronze tables before silver transform:

| Check | Rule |
|-------|------|
| Null checks | `appearances.player_id`, `appearances.game_id` not null |
| Referential integrity | `appearances.player_id` exists in `players` |
| Valid ranges | `minutes_played >= 0`, `goals >= 0` |
| Duplicate detection (bonus) | Unique `appearance_id`, `game_id`, `player_id` |
| Schema drift (bonus) | Flags extra columns dropped at bronze ingest |

Reports are written to `reports/dq/dq_report_{snapshot}_{timestamp}.json` and `dq_report_{snapshot}_latest.json`.

Incremental processing uses a checkpoint at `data/state/pipeline_state.json`:

| Mechanism | Behaviour |
|-----------|-----------|
| Snapshot order | `data_day1` → `data_day2` (configurable in `config.yaml`) |
| Content hash | Skips snapshots already processed with identical CSV content |
| Game watermark | Tracks `games_date`; cumulative merge reports rows after watermark |
| Cumulative bronze | `data/bronze/cumulative/` — upsert by natural key per table |
| Idempotent upserts | Facts, SCD, cumulative bronze, and gold rebuild are safe to rerun |

## Processing engine choice

**Decision: Pandas** (default via `ENGINE=pandas` in `.env` / `config/config.yaml`).

### Why Pandas

| Factor | Rationale |
|--------|-----------|
| Dataset size | Sample snapshots are thousands of rows; full Kaggle extract is laptop-scale (low millions of rows) and fits comfortably in memory |
| Execution environment | Assignment requires laptop execution and Docker reproducibility — pandas + pyarrow avoids JVM/Spark cluster overhead |
| Development speed | Rich ecosystem for CSV/parquet, SCD merges, and DQ checks with straightforward pytest assertions |
| Operational cost | Single-process runs align with hourly incremental feeds in the sample data without distributed coordination |

### Scale threshold — when to switch to Spark

Move to **PySpark** when any of the following apply:

- **Row volume:** sustained growth beyond ~**10M rows per fact table** (appearances/games) or bronze parquet exceeding ~**50 GB** on disk
- **Memory pressure:** transform or gold steps require more than ~**16 GB** RAM on a single node even after column pruning
- **Latency SLA:** wall-clock runtime exceeds the orchestration window (e.g. hourly DAG cannot finish before the next feed)
- **Infrastructure:** data already lands on a distributed store (S3/ADLS) with EMR/Databricks/Spark-on-K8s available

Below that threshold, pandas remains simpler to develop, test, and operate.

### Modularity — swapping engines

Engine selection is config-driven. The swap boundary lives in `src/engine/`:

```
config.yaml / ENGINE env  →  get_engine()  →  PandasEngine | SparkEngine
                                      ↓
                            src/load/parquet_writer.py (read/write parquet)
```

- **`PandasEngine`** — production implementation used by all pipeline runs today
- **`SparkEngine`** — reserved stub in `src/engine/spark_engine.py`; setting `ENGINE=spark` fails fast with guidance to add PySpark and implement the same `DataEngine` protocol

Transform functions continue to accept pandas DataFrames. A Spark migration would implement `SparkEngine` I/O and add thin adapters at the transform boundary — no changes to config, DAG structure, or DQ rules.

```bash
# Default (pandas)
make incremental

# Reserved — raises a clear error until PySpark is wired in
ENGINE=spark make incremental
```

## Sample data

See [data/README.md](data/README.md) for `data_day1` vs `data_day2` delta scenarios.