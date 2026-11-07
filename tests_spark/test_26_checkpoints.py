"""Loads after the target and the state got a Delta checkpoint (every 10th commit): with a
delta-spark built for another Spark version, reading a checkpoint fails inside Spark, and
``toArrow()`` waited forever for that error instead of raising it."""

import contextlib
import signal
from pathlib import Path

import pytest

from sql2deltalake.spark import run
from sql2deltalake.spark._driver import _STATE_CACHE_ENV, _State, _to_arrow

from .conftest import execute

TABLE = """CREATE TABLE dbo.cp (id int NOT NULL PRIMARY KEY, n int NOT NULL, rv rowversion);
           INSERT INTO dbo.cp (id, n) VALUES (1, 0), (2, 0), (3, 0)"""


@contextlib.contextmanager
def _deadline(seconds: int):
    def expired(signum, frame):
        raise TimeoutError(f"still running after {seconds} s")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def test_loads_after_checkpoints(spark, source, tmp_path, monkeypatch):
    # no key state kept in the driver: every prepare() reads latest_pk_version back
    monkeypatch.setenv(_STATE_CACHE_ENV, "0")
    execute(source, TABLE)
    target = str(tmp_path / "cp")
    options = dict(url=source, table="dbo.cp", target=target)
    with _deadline(600):
        assert run(spark, **options) == "full"
        for i in range(1, 13):
            execute(source, f"UPDATE dbo.cp SET n = {i} WHERE id = {i % 3 + 1}")
            assert run(spark, **options) == "delta"
    for table in (target, target + "_state/latest_pk_version"):
        assert list(Path(table, "_delta_log").glob("*.checkpoint*.parquet")), table
    assert (_State(spark, target + "_state").latest.version() or 0) >= 10
    rows = spark.read.format("delta").load(target).collect()
    latest = {}
    for r in sorted(rows, key=lambda r: r["__timestamp"]):
        latest[r["id"]] = r["n"]
    assert latest == {1: 12, 2: 10, 3: 11}


def test_to_arrow_raises_a_planning_error():
    """A fatal JVM error while planning is raised before toArrow() could wait for it."""
    calls = []

    class Execution:
        def executedPlan(self):
            calls.append("plan")
            raise RuntimeError(
                "java.lang.NoSuchMethodError: 'void org.apache.spark.sql.execution.datasources."
                "parquet.ParquetToSparkSchemaConverter.<init>(boolean)'"
            )

    class JavaFrame:
        def queryExecution(self):
            return Execution()

    class Frame:
        _jdf = JavaFrame()

        def toArrow(self):
            calls.append("arrow")

    with pytest.raises(RuntimeError, match="delta-spark does not match the Spark version"):
        _to_arrow(Frame())
    assert calls == ["plan"]
