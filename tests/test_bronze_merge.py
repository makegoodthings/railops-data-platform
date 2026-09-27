from datetime import UTC, datetime
from typing import Any

import duckdb
import pytest

from railops.ingestion import merge_source_events

BRONZE_DDL = """
CREATE TABLE bronze.train_events_raw (
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

T0 = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 27, 13, 0, tzinfo=UTC)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("CREATE SCHEMA bronze")
    connection.execute(BRONZE_DDL)
    yield connection
    connection.close()


def source_event(
    event_id: int = 1,
    updated_at: datetime = T0,
    station_code: str = "CHI",
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "train_id": "T0001",
        "station_code": station_code,
        "event_type": "ARRIVAL",
        "scheduled_ts": T0,
        "event_ts": T0,
        "created_at": T0,
        "updated_at": updated_at,
    }


def bronze_rows(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    return con.execute(
        """
        SELECT event_id, station_code, updated_at, _batch_id
        FROM bronze.train_events_raw
        ORDER BY event_id
        """
    ).fetchall()


def test_same_version_twice_creates_no_duplicates(con) -> None:
    batch = [source_event(event_id=1), source_event(event_id=2)]

    assert merge_source_events(con, batch, "run-1") == (2, 0)
    assert merge_source_events(con, batch, "run-2") == (0, 0)

    rows = bronze_rows(con)
    assert len(rows) == 2
    assert all(row[3] == "run-1" for row in rows)


def test_newer_version_updates_existing_row(con) -> None:
    merge_source_events(con, [source_event(updated_at=T0, station_code="CHI")], "run-1")

    newer = source_event(updated_at=T1, station_code="FAL")
    assert merge_source_events(con, [newer], "run-2") == (0, 1)

    rows = bronze_rows(con)
    assert rows == [(1, "FAL", T1, "run-2")]


def test_older_version_does_not_overwrite_newer_row(con) -> None:
    merge_source_events(con, [source_event(updated_at=T1, station_code="FAL")], "run-1")

    older = source_event(updated_at=T0, station_code="CHI")
    assert merge_source_events(con, [older], "run-2") == (0, 0)

    rows = bronze_rows(con)
    assert rows == [(1, "FAL", T1, "run-1")]
