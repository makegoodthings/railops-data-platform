import json
from datetime import UTC, datetime
from pathlib import Path
from railops.source import initialize_source, seed_source

import duckdb
import typer

from railops.config import get_settings

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
def run() -> None:
    """Placeholder for Milestones 2–5: execute the full batch pipeline."""
    typer.echo("Pipeline implementation begins in Milestone 2.")


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
