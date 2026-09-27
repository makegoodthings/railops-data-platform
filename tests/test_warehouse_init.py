from pathlib import Path

import duckdb
from typer.testing import CliRunner

from railops.cli import app
from railops.config import get_settings


def test_init_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    warehouse = tmp_path / "test.duckdb"
    monkeypatch.setenv("WAREHOUSE_PATH", str(warehouse))
    get_settings.cache_clear()
    runner = CliRunner()

    assert runner.invoke(app, ["init"]).exit_code == 0
    assert runner.invoke(app, ["init"]).exit_code == 0

    with duckdb.connect(str(warehouse)) as con:
        rows = con.execute("SELECT count(*) FROM meta.pipeline_state").fetchone()[0]
    assert rows == 1
    get_settings.cache_clear()

