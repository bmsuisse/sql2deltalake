"""prepare() asks the source only what its plan needs: a first load reads no MAX/COUNT, and
MIN_ACTIVE_ROWVERSION() and the scale of time columns are optional probes, each run at most once,
within a short deadline and never retried. A login that stalls on one of them must cost seconds,
not a failed prepare() (a cluster whose every second login stalled for 15 s made prepare() fail
and the caller retry it for half an hour)."""

import logging
import time

import pyarrow as pa
import pytest

from sql2deltalake import _core
from sql2deltalake.reader import arrowsql_reader
from sql2deltalake.reader.arrowsql_reader import ArrowSQLSource, strip_odbc_driver
from sql2deltalake.spark import FORMAT, commit, prepare
from sql2deltalake.spark._options import LoadOptions
from sql2deltalake.spark._plan import Plan
from sql2deltalake.spark._source_load import SourceLoad

from .conftest import execute

TABLE = """CREATE TABLE dbo.pp (id int NOT NULL PRIMARY KEY, v nvarchar(20), rv rowversion);
           INSERT INTO dbo.pp (id, v) VALUES (1, 'a'), (2, 'b')"""
TIME_TABLE = """CREATE TABLE dbo.pt (id int NOT NULL PRIMARY KEY, t time(3), rv rowversion);
                INSERT INTO dbo.pt (id, t) VALUES (1, '12:00:01.5'), (2, NULL)"""


@pytest.fixture
def statements(monkeypatch):
    """Every statement prepare() sends to the source: ("read", sql) or ("probe", sql)."""
    seen = []
    source_arrow = ArrowSQLSource._source_arrow
    bounded = ArrowSQLSource._bounded_sql_to_py

    def traced_arrow(self, sql, *args, **kwargs):
        seen.append(("read", sql))
        return source_arrow(self, sql, *args, **kwargs)

    def traced_bounded(self, sql, timeout):
        seen.append(("probe", sql))
        return bounded(self, sql, timeout)

    monkeypatch.setattr(ArrowSQLSource, "_source_arrow", traced_arrow)
    monkeypatch.setattr(ArrowSQLSource, "_bounded_sql_to_py", traced_bounded)
    return seen


def _load(spark, opts, target):
    spark.read.format(FORMAT).options(**opts).load().write.format("delta").mode("append").save(target)
    commit(spark, opts)


def _with(seen, text):
    return [s for s in seen if text in s[1]]


def test_first_load_reads_only_what_it_needs(spark, source, tmp_path, statements):
    execute(source, TABLE)
    target = str(tmp_path / "pp")
    opts = prepare(spark, url=source, table="dbo.pp", target=target, keys="id")
    plan = Plan.decode(opts["plan"])
    assert plan.kind == "full"
    assert not _with(statements, "COUNT(*)"), statements  # nothing to compare MAX/COUNT with
    # MIN_ACTIVE_ROWVERSION() and the clock: one probe, no table read
    assert [k for k, _ in _with(statements, "MIN_ACTIVE_ROWVERSION")] == ["probe"]
    assert not _with(statements, "dm_exec_describe_first_result_set")  # no time column
    assert len(statements) == 3, statements  # columns, clock and MAR, schema
    assert plan.commit is not None and plan.commit["min_active_rowversion"] is not None
    _load(spark, opts, target)

    # the MIN_ACTIVE_ROWVERSION() of the first load lets the next one take the shortcut
    statements.clear()
    opts = prepare(spark, url=source, table="dbo.pp", target=target, keys="id")
    assert Plan.decode(opts["plan"]).kind == "unchanged"
    fused = _with(statements, "COUNT(*)")
    assert len(fused) == 1 and "MIN_ACTIVE_ROWVERSION" in fused[0][1], statements
    assert not [s for s in statements if s[0] == "probe"], statements


def test_failing_probes_cost_one_attempt_each(spark, source, tmp_path, monkeypatch, caplog):
    execute(source, TIME_TABLE)
    probes = []

    def stalled(self, sql, timeout):
        probes.append(sql)
        raise TimeoutError(f"login did not finish within {timeout:g} s")

    monkeypatch.setattr(ArrowSQLSource, "_bounded_sql_to_py", stalled)
    target = str(tmp_path / "pt")
    with caplog.at_level(logging.WARNING):
        opts = prepare(spark, url=source, table="dbo.pt", target=target)
    plan = Plan.decode(opts["plan"])
    assert plan.kind == "full"
    # one attempt per probe: MIN_ACTIVE_ROWVERSION() and the time scales
    assert len(probes) == 2, probes
    assert sum("MIN_ACTIVE_ROWVERSION" in p for p in probes) == 1
    assert sum("dm_exec_describe_first_result_set" in p for p in probes) == 1
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2, warnings
    assert plan.commit is not None and plan.commit["min_active_rowversion"] is None
    assert plan.time_scales == {"t": 3}  # from the column metadata
    _load(spark, opts, target)
    rows = {r["id"]: r["t"] for r in spark.read.format("delta").load(target).collect()}
    assert rows == {1: "12:00:01.500", 2: None}


def test_time_scales_are_probed_once(spark, source, tmp_path, statements):
    execute(source, TIME_TABLE)
    opts = prepare(spark, url=source, table="dbo.pt", target=str(tmp_path / "pt"))
    assert Plan.decode(opts["plan"]).time_scales == {"t": 3}
    assert len(_with(statements, "dm_exec_describe_first_result_set")) == 1, statements


def _source_load(source, table):
    return SourceLoad(LoadOptions.parse({"url": source, "table": table, "keys": "id"}))


@pytest.mark.parametrize(
    "error, attempts",
    [
        ("[HYT00] [Microsoft][ODBC Driver 18 for SQL Server]Query timeout expired", 1),
        ("[08S01] [Microsoft][ODBC Driver 18 for SQL Server]Communication link failure", 1),
        ("[42000] [Microsoft][ODBC Driver 18 for SQL Server]permission denied", 2),
    ],
)
def test_counts_without_rowversion_only_after_a_server_error(source, monkeypatch, error, attempts):
    """A server error (e.g. no permission in the table's database) reads MAX/COUNT once more
    without MIN_ACTIVE_ROWVERSION(); a failed login or a timeout is raised at once."""
    execute(source, TABLE)
    load = _source_load(source, "dbo.pp")
    sent = []
    real = ArrowSQLSource.source_sql_to_py

    def failing(self, sql):
        sent.append(sql)
        if "MIN_ACTIVE_ROWVERSION" in str(sql):
            raise RuntimeError(error)
        return real(self, sql)

    monkeypatch.setattr(ArrowSQLSource, "source_sql_to_py", failing)
    if attempts == 1:
        with pytest.raises(RuntimeError, match=r"HYT00|08S01"):
            load.source_state(counts=True)
    else:
        state = load.source_state(counts=True)
        assert state["mar"] is None and state["count"] == 2
    assert len(sent) == attempts


def test_optional_probe_runs_once_also_when_it_fails(source, monkeypatch, caplog):
    reader = ArrowSQLSource(source)
    calls = []

    def flaky(self, sql, timeout):
        calls.append(sql)
        if "fail" in sql:
            raise TimeoutError("login did not finish within 1 s")
        return [{"a": 1}]

    monkeypatch.setattr(ArrowSQLSource, "_bounded_sql_to_py", flaky)
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert reader.optional_sql_to_py("SELECT 1 AS a", "a") == [{"a": 1}]
            assert reader.optional_sql_to_py("SELECT 1 AS fail", "b, using 7") is None
    assert calls == ["SELECT 1 AS a", "SELECT 1 AS fail"]
    assert [r.getMessage() for r in caplog.records] == [
        "Could not read b, using 7: login did not finish within 1 s"
    ]


def test_probe_timeout_setting(monkeypatch):
    monkeypatch.setenv(arrowsql_reader.PROBE_TIMEOUT_ENV, "2.5")
    assert arrowsql_reader.probe_timeout() == 2.5
    for bad in ("0", "-1", "soon"):
        monkeypatch.setenv(arrowsql_reader.PROBE_TIMEOUT_ENV, bad)
        assert arrowsql_reader.probe_timeout() == 10.0


def test_bounded_read_cancels_at_the_deadline(source):
    conn = strip_odbc_driver(source)
    rows = pa.RecordBatchReader.from_stream(_core.read_sql(conn, "SELECT 1 AS a", timeout=10)).read_all()
    assert rows.to_pylist() == [{"a": 1}]
    started = time.perf_counter()
    with pytest.raises(RuntimeError, match=r"HYT00.*query did not finish within 1 s"):
        pa.RecordBatchReader.from_stream(
            _core.read_sql(conn, "WAITFOR DELAY '00:00:20'; SELECT 1 AS a", timeout=1)
        ).read_all()
    assert time.perf_counter() - started < 5
    with pytest.raises(ValueError):
        _core.read_sql(conn, "SELECT 1", timeout=0)
    with pytest.raises(ValueError):
        _core.read_sql(conn, "SELECT 1", timeout=5, partition_on="a", partitions=2)
