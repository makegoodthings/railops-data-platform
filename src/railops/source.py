import random
from datetime import UTC, datetime, timedelta

import psycopg

from railops.config import get_settings

SOURCE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.train_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    train_id VARCHAR(20) NOT NULL,
    station_code VARCHAR(10),
    event_type VARCHAR(20),
    scheduled_ts TIMESTAMPTZ,
    event_ts TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_train_events_incremental
    ON public.train_events (updated_at, event_id);

CREATE OR REPLACE FUNCTION public.set_train_event_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_train_event_updated_at
    ON public.train_events;

CREATE TRIGGER trg_train_event_updated_at
BEFORE UPDATE ON public.train_events
FOR EACH ROW
EXECUTE FUNCTION public.set_train_event_updated_at();
"""


def initialize_source() -> None:
    settings = get_settings()

    with psycopg.connect(settings.source_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SOURCE_SCHEMA_SQL)


def seed_source(rows: int, random_seed: int = 42) -> int:
    settings = get_settings()
    generator = random.Random(random_seed)

    stations = ["CHI", "GBB", "FAL", "MIN", "SPK", "PAS"]
    event_types = ["ARRIVAL", "DEPARTURE"]

    base_time = (
        datetime.now(UTC).replace(second=0, microsecond=0)
        - timedelta(hours=6)
    )

    records = []

    for index in range(rows):
        scheduled_ts = base_time + timedelta(minutes=index * 5)
        delay_minutes = generator.randint(-5, 60)
        event_ts = scheduled_ts + timedelta(minutes=delay_minutes)

        records.append(
            (
                f"T{(index % 10) + 1:04d}",
                stations[index % len(stations)],
                event_types[index % len(event_types)],
                scheduled_ts,
                event_ts,
            )
        )

    insert_sql = """
        INSERT INTO public.train_events (
            train_id,
            station_code,
            event_type,
            scheduled_ts,
            event_ts
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    with psycopg.connect(settings.source_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(insert_sql, records)

    return len(records)