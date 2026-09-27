"""Milestone 7 starter DAG.

The DAG is intentionally skeletal on day one. Each completed pipeline stage becomes one task so
the orchestration code reflects real dependencies instead of hiding everything in one notebook.
"""

from datetime import UTC, datetime, timedelta

try:
    from airflow.sdk import DAG, task
except ImportError:  # Allows local lint/tests before Airflow is installed.
    DAG = None
    task = None


if DAG is not None:
    with DAG(
        dag_id="railops_batch",
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        schedule="0 * * * *",
        catchup=False,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
        tags=["learning", "batch", "railops"],
    ) as dag:

        @task
        def not_implemented_yet() -> None:
            raise NotImplementedError("Complete pipeline stages before enabling this DAG")

        not_implemented_yet()
