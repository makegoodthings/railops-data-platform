# RailOps Data Platform

A production-minded weekend data engineering project built around synthetic railway events.
It teaches the complete batch lifecycle without using employer data:

```text
PostgreSQL source -> incremental ingestion -> DuckDB Bronze -> Silver -> Gold
                  -> data-quality gate -> Airflow DAG -> downstream-ready tables
```

## What this project proves

- Incremental extraction with a composite watermark
- Idempotent upserts and safe reruns
- Bronze/Silver/Gold modeling with explicit grains
- Reject/quarantine handling instead of silently dropping bad rows
- Data quality checks that fail the pipeline
- Backfills that do not corrupt the normal watermark
- Airflow orchestration, structured logs, unit tests, and GitHub Actions CI

## Stack

- Python 3.12
- PostgreSQL 16 as the operational source
- DuckDB as a lightweight local analytical warehouse
- Apache Airflow 3 for orchestration (Sunday milestone)
- Docker Compose for reproducibility
- pytest + Ruff + GitHub Actions

DuckDB is deliberately used instead of pretending a laptop is a distributed cluster. The
engineering contracts map directly to Databricks/Delta: raw ingestion, `MERGE`, table grain,
watermarks, data quality, backfill, and orchestration.

## Start here

1. Read [Weekend plan](docs/WEEKEND_PLAN.md).
2. Complete [Milestone 0](docs/MILESTONE_0.md).
3. Then begin Milestone 1 with me. Do not rush ahead: every milestone has an observable
   acceptance test and a Git commit.

## Quick start (after Milestone 0)

```bash
cp .env.example .env
docker compose up -d source-db
docker compose run --rm pipeline python -m railops.cli init
docker compose run --rm pipeline python -m railops.cli seed --rows 100
docker compose run --rm pipeline python -m railops.cli run
docker compose run --rm pipeline python -m railops.cli inspect
```

Run the same pipeline again. The second run should ingest zero new rows. That is your first
idempotency proof.

## Architecture

See [Architecture and contracts](docs/ARCHITECTURE.md). The short version:

| Layer | Grain | Purpose |
|---|---|---|
| Source `train_events` | One row per `event_id` | Mutable operational records |
| Bronze `train_events_raw` | Latest ingested version per `event_id` | Source-aligned data + ingestion metadata |
| Silver `train_events_clean` | One valid event per `event_id` | Typed, normalized, trustworthy events |
| Silver `train_events_rejected` | One rejection per event/run | Invalid rows with reasons |
| Gold `train_current_status` | One row per `train_id` | Latest known train state |
| Gold `daily_station_metrics` | One station/event/day | Downstream aggregate |

## Repository map

```text
src/railops/       Pipeline code
airflow/dags/      Production-style orchestration
tests/             Unit and integration-style tests
docs/              Weekend curriculum and architecture decisions
data/              Local warehouse files (gitignored)
.github/workflows/ CI checks
```

## Important scope note

Docker Compose and Airflow `standalone` are local development tools, not a claim that this is a
production deployment platform. The production-grade part of this project is the pipeline
behavior and contracts; a real deployment would use managed Airflow/Kubernetes, managed storage,
secrets management, centralized logs, and platform monitoring.

