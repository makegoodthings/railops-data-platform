import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import typer

from railops.config import get_settings
from railops.ingestion import (
    advance_watermark,
    complete_pipeline_run,
    extract_source_events,
    fail_pipeline_run,
    merge_source_events,
    read_watermark,
    start_pipeline_run,
)
from railops.source import initialize_source, seed_source
from railops.watermark import Watermark

app = typer.Typer(no_args_is_help=True)


def _warehouse() -> duckdb.DuckDBPyConnection:
    settings = get_settings()
    Path(settings.warehouse_path).parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.warehouse_path))
    connection.execute("SET TimeZone = 'UTC'")
    return connection


@app.command()
def init() -> None:
    """Initialize warehouse metadata; source schema arrives in Milestone 1."""
    initialize_source()
    with _warehouse() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS meta")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.pipeline_state (
                pipeline_name VARCHAR PRIMARY KEY,
                last_updated_at TIMESTAMPTZ NOT NULL,
                last_event_id BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        con.execute(
            """
            INSERT INTO meta.pipeline_state
            SELECT 'train_events', TIMESTAMPTZ '1970-01-01 00:00:00+00', 0, current_timestamp
            WHERE NOT EXISTS (
                SELECT 1 FROM meta.pipeline_state WHERE pipeline_name = 'train_events'
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                pipeline_name VARCHAR NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                ended_at TIMESTAMPTZ,
                status VARCHAR NOT NULL
                    CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

                start_watermark_ts TIMESTAMPTZ NOT NULL,
                start_watermark_id BIGINT NOT NULL,
                end_watermark_ts TIMESTAMPTZ,
                end_watermark_id BIGINT,

                rows_extracted BIGINT NOT NULL DEFAULT 0,
                error_message VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.train_events_raw (
                event_id BIGINT PRIMARY KEY,
                train_id VARCHAR NOT NULL,
                station_code VARCHAR,
                event_type VARCHAR,
                scheduled_ts TIMESTAMPTZ,
                event_ts TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,

                _ingested_at TIMESTAMPTZ NOT NULL,
                _batch_id VARCHAR NOT NULL,
                _source_system VARCHAR NOT NULL
            )
            """
        )
    typer.echo("Warehouse schemas and initial watermark created.")


@app.command()
def seed(rows: int = typer.Option(100, min=1)) -> None:
    """Insert synthetic train events into the source database."""
    initialize_source()
    inserted_rows = seed_source(rows)

    typer.echo(
        f"Inserted {inserted_rows} synthetic train events into PostgreSQL."
    )


@app.command()
def preview_extract(batch_size: int = typer.Option(1000, min=1)) -> None:
    """Read-only preview of the next incremental extraction window.

    Does not write to Bronze and does not advance the watermark.
    """
    settings = get_settings()
    if not Path(settings.warehouse_path).exists():
        raise typer.BadParameter("Warehouse does not exist. Run `init` first.")
    with _warehouse() as con:
        watermark = read_watermark(con)
    events = extract_source_events(watermark, batch_size=batch_size)

    def cursor(row: dict) -> dict:
        return {"updated_at": row["updated_at"], "event_id": row["event_id"]}

    payload = {
        "starting_watermark": {
            "updated_at": watermark.updated_at,
            "event_id": watermark.event_id,
        },
        "rows_extracted": len(events),
        "first_cursor": cursor(events[0]) if events else None,
        "last_cursor": cursor(events[-1]) if events else None,
    }
    typer.echo(json.dumps(payload, default=str, indent=2))


@app.command()
def run(batch_size: int = typer.Option(50, min=1, max=10000)) -> None:
    """Ingest one incremental batch from PostgreSQL into Bronze."""
    settings = get_settings()
    if not Path(settings.warehouse_path).exists():
        raise typer.BadParameter("Warehouse does not exist. Run `init` first.")

    run_id = str(uuid.uuid4())
    with _warehouse() as con:
        start_wm = read_watermark(con)
        start_pipeline_run(con, run_id, start_wm)

        in_transaction = False
        try:
            events = extract_source_events(start_wm, batch_size=batch_size)
            if events:
                end_wm = Watermark(
                    updated_at=events[-1]["updated_at"],
                    event_id=events[-1]["event_id"],
                )
            else:
                end_wm = start_wm

            con.execute("BEGIN TRANSACTION")
            in_transaction = True
            inserted, updated = merge_source_events(con, events, run_id)
            advance_watermark(con, end_wm)
            complete_pipeline_run(con, run_id, end_wm, len(events))
            con.execute("COMMIT")
            in_transaction = False
        except Exception as exc:
            if in_transaction:
                con.execute("ROLLBACK")
            fail_pipeline_run(con, run_id, str(exc))
            raise

    payload = {
        "run_id": run_id,
        "status": "SUCCESS",
        "rows_extracted": len(events),
        "rows_inserted": inserted,
        "rows_updated": updated,
        "start_watermark": {
            "updated_at": start_wm.updated_at,
            "event_id": start_wm.event_id,
        },
        "end_watermark": {
            "updated_at": end_wm.updated_at,
            "event_id": end_wm.event_id,
        },
    }
    typer.echo(json.dumps(payload, default=str, indent=2))


@app.command()
def inspect() -> None:
    """Display current schemas, tables, and watermark state."""
    settings = get_settings()
    if not Path(settings.warehouse_path).exists():
        raise typer.BadParameter("Warehouse does not exist. Run `init` first.")
    with _warehouse() as con:
        tables = con.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('meta', 'bronze', 'silver', 'gold')
            ORDER BY 1, 2
            """
        ).fetchall()
        state = con.execute("SELECT * FROM meta.pipeline_state").fetchall()
    typer.echo(json.dumps({"tables": tables, "state": state}, default=str, indent=2))


@app.command()
def doctor() -> None:
    """Check basic local configuration without modifying source data."""
    settings = get_settings()
    typer.echo(
        json.dumps(
            {
                "checked_at": datetime.now(UTC).isoformat(),
                "source": f"{settings.source_db_host}:{settings.source_db_port}",
                "warehouse": str(settings.warehouse_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    app()
