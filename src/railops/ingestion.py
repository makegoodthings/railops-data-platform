from datetime import UTC, datetime
from typing import Any

import duckdb
import psycopg
from psycopg.rows import dict_row

from railops.config import get_settings
from railops.watermark import Watermark

EXTRACT_SQL_TEMPLATE = """
    SELECT
        event_id,
        train_id,
        station_code,
        event_type,
        scheduled_ts,
        event_ts,
        created_at,
        updated_at
    FROM public.train_events
    WHERE {predicate}
    ORDER BY updated_at, event_id
    LIMIT %s
"""


def read_watermark(
    con: duckdb.DuckDBPyConnection,
    pipeline_name: str = "train_events",
) -> Watermark:
    row = con.execute(
        """
        SELECT last_updated_at, last_event_id
        FROM meta.pipeline_state
        WHERE pipeline_name = ?
        """,
        [pipeline_name],
    ).fetchone()
    if row is None:
        raise ValueError(
            f"No watermark found for pipeline '{pipeline_name}'. Run `init` first."
        )
    return Watermark(updated_at=row[0], event_id=row[1])


def extract_source_events(
    watermark: Watermark,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    predicate_sql, predicate_params = watermark.source_predicate()
    sql = EXTRACT_SQL_TEMPLATE.format(predicate=predicate_sql)
    settings = get_settings()

    with psycopg.connect(settings.source_dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (*predicate_params, batch_size))
            return cursor.fetchall()


def merge_source_events(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    run_id: str,
) -> tuple[int, int]:
    """Upsert extracted source rows into Bronze. Returns (inserted, updated).

    Existing rows are only overwritten when the incoming version is newer,
    so replaying an old batch can never regress Bronze.
    """
    if not rows:
        return 0, 0

    ingested_at = datetime.now(UTC)
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE _stage_train_events (
            event_id BIGINT,
            train_id VARCHAR,
            station_code VARCHAR,
            event_type VARCHAR,
            scheduled_ts TIMESTAMPTZ,
            event_ts TIMESTAMPTZ,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            _ingested_at TIMESTAMPTZ,
            _batch_id VARCHAR,
            _source_system VARCHAR
        )
        """
    )
    con.executemany(
        "INSERT INTO _stage_train_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            [
                row["event_id"],
                row["train_id"],
                row["station_code"],
                row["event_type"],
                row["scheduled_ts"],
                row["event_ts"],
                row["created_at"],
                row["updated_at"],
                ingested_at,
                run_id,
                "postgresql",
            ]
            for row in rows
        ],
    )

    inserted = con.execute(
        """
        SELECT count(*)
        FROM _stage_train_events s
        WHERE NOT EXISTS (
            SELECT 1 FROM bronze.train_events_raw b WHERE b.event_id = s.event_id
        )
        """
    ).fetchone()[0]
    updated = con.execute(
        """
        SELECT count(*)
        FROM _stage_train_events s
        JOIN bronze.train_events_raw b USING (event_id)
        WHERE s.updated_at > b.updated_at
        """
    ).fetchone()[0]

    con.execute(
        """
        MERGE INTO bronze.train_events_raw AS b
        USING _stage_train_events AS s
        ON b.event_id = s.event_id
        WHEN MATCHED AND s.updated_at > b.updated_at THEN UPDATE SET
            train_id = s.train_id,
            station_code = s.station_code,
            event_type = s.event_type,
            scheduled_ts = s.scheduled_ts,
            event_ts = s.event_ts,
            created_at = s.created_at,
            updated_at = s.updated_at,
            _ingested_at = s._ingested_at,
            _batch_id = s._batch_id,
            _source_system = s._source_system
        WHEN NOT MATCHED THEN INSERT (
            event_id, train_id, station_code, event_type,
            scheduled_ts, event_ts, created_at, updated_at,
            _ingested_at, _batch_id, _source_system
        ) VALUES (
            s.event_id, s.train_id, s.station_code, s.event_type,
            s.scheduled_ts, s.event_ts, s.created_at, s.updated_at,
            s._ingested_at, s._batch_id, s._source_system
        )
        """
    )
    con.execute("DROP TABLE _stage_train_events")
    return inserted, updated


def start_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    start_watermark: Watermark,
    pipeline_name: str = "train_events",
) -> None:
    con.execute(
        """
        INSERT INTO meta.pipeline_runs (
            run_id, pipeline_name, started_at, status,
            start_watermark_ts, start_watermark_id
        )
        VALUES (?, ?, ?, 'RUNNING', ?, ?)
        """,
        [
            run_id,
            pipeline_name,
            datetime.now(UTC),
            start_watermark.updated_at,
            start_watermark.event_id,
        ],
    )


def complete_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    end_watermark: Watermark,
    rows_extracted: int,
) -> None:
    con.execute(
        """
        UPDATE meta.pipeline_runs
        SET status = 'SUCCESS',
            ended_at = ?,
            end_watermark_ts = ?,
            end_watermark_id = ?,
            rows_extracted = ?
        WHERE run_id = ?
        """,
        [
            datetime.now(UTC),
            end_watermark.updated_at,
            end_watermark.event_id,
            rows_extracted,
            run_id,
        ],
    )


def fail_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    error_message: str,
) -> None:
    con.execute(
        """
        UPDATE meta.pipeline_runs
        SET status = 'FAILED',
            ended_at = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        [datetime.now(UTC), error_message, run_id],
    )


def advance_watermark(
    con: duckdb.DuckDBPyConnection,
    watermark: Watermark,
    pipeline_name: str = "train_events",
) -> None:
    con.execute(
        """
        UPDATE meta.pipeline_state
        SET last_updated_at = ?,
            last_event_id = ?,
            updated_at = ?
        WHERE pipeline_name = ?
        """,
        [watermark.updated_at, watermark.event_id, datetime.now(UTC), pipeline_name],
    )
