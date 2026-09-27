from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, order=True)
class Watermark:
    updated_at: datetime
    event_id: int

    @classmethod
    def initial(cls) -> "Watermark":
        return cls(datetime(1970, 1, 1, tzinfo=UTC), 0)

    def source_predicate(self) -> tuple[str, tuple[datetime, datetime, int]]:
        sql = "(updated_at > %s OR (updated_at = %s AND event_id > %s))"
        return sql, (self.updated_at, self.updated_at, self.event_id)
