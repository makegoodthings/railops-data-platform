from datetime import UTC, datetime

from railops.watermark import Watermark


def test_initial_watermark_is_before_business_data() -> None:
    watermark = Watermark.initial()
    assert watermark.updated_at == datetime(1970, 1, 1, tzinfo=UTC)
    assert watermark.event_id == 0


def test_watermark_orders_same_timestamp_by_event_id() -> None:
    timestamp = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)
    assert Watermark(timestamp, 10) < Watermark(timestamp, 11)


def test_source_predicate_uses_both_cursor_fields() -> None:
    timestamp = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)
    sql, params = Watermark(timestamp, 42).source_predicate()
    assert "updated_at >" in sql
    assert "event_id >" in sql
    assert params == (timestamp, timestamp, 42)
