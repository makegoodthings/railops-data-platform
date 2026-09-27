# Architecture and data contracts

## Business scenario

An operations application writes train arrival/departure events to PostgreSQL. Analytics and a
future ETA model need trustworthy, current, and historical data. The source is mutable: an event
may be corrected after insertion.

## Flow and ownership

1. **Source**: PostgreSQL owns operational truth.
2. **Ingestion**: extract rows after `(updated_at, event_id)` watermark in deterministic batches.
3. **Bronze**: merge source records and add `_ingested_at`, `_batch_id`, `_source_system`.
4. **Silver**: validate, normalize station/event values, and quarantine invalid records.
5. **Gold**: publish current train state and station/day aggregates.
6. **Quality gate**: stop publication if contract checks fail.
7. **Orchestration**: Airflow controls order, retry policy, and schedule.

## Why a composite watermark?

Many rows can share the same `updated_at`. Saving only a timestamp can skip rows at the boundary.
The extraction predicate is lexicographic:

```sql
WHERE updated_at > :last_updated_at
   OR (updated_at = :last_updated_at AND event_id > :last_event_id)
ORDER BY updated_at, event_id
```

The watermark advances only after the destination transaction commits. A failed run therefore
replays data rather than losing it. Bronze `MERGE` makes that replay safe.

## Table contracts

### Source `public.train_events`

- Grain: one mutable row per `event_id`
- Key: `event_id`
- Incremental cursor: `(updated_at, event_id)`
- Timestamps: UTC-aware

### Bronze `bronze.train_events_raw`

- Grain: latest ingested source version per `event_id`
- Preserves all source columns
- Adds ingestion lineage fields
- No business filtering

### Silver `silver.train_events_clean`

- Grain: one valid normalized event per `event_id`
- `station_code` is uppercase and trimmed
- `event_type` is ARRIVAL or DEPARTURE
- Impossible rows go to `silver.train_events_rejected`

### Gold `gold.train_current_status`

- Grain: one row per `train_id`
- Latest event by `(event_ts, event_id)`
- Designed for an application/API to query

### Gold `gold.daily_station_metrics`

- Grain: one row per `event_date`, `station_code`, `event_type`
- Contains event count and average delay minutes

## Failure semantics

| Failure | Expected behavior |
|---|---|
| Extract fails | Watermark unchanged; retry reads same source range |
| Bronze merge fails | Transaction rolls back; watermark unchanged |
| Transform fails | Bronze remains; rerun rebuilds downstream deterministically |
| Bad source row | Quarantined with a reason; valid rows continue |
| Quality gate fails | DAG fails before downstream publication |
| Historical correction | Updated source row is re-ingested and merged |

## Backfill contract

A backfill accepts an explicit time range and writes through the same Bronze/Silver/Gold logic,
but it does **not** change the normal incremental watermark. Mixing those two states is a common
way to create silent gaps.

