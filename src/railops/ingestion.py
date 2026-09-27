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
