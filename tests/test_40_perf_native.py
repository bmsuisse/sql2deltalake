"""Timings of the Rust load (`SQL2DELTA_NATIVE=1`) against the Python implementation on a 1M-row
table: full load, unchanged reload, reload after 1% of the rows changed. Opt-in (slow):

    SQL2DELTA_PERF=1 scripts/run_tests_local.sh tests/test_40_perf_native.py -s

`SQL2DELTA_PERF_ROWS` sets the row count (default 1000000).
"""

import os
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from tests.conftest import DB_Connection

ROWS = int(os.getenv("SQL2DELTA_PERF_ROWS", "1000000"))

pytestmark = pytest.mark.skipif(
    os.getenv("SQL2DELTA_PERF") != "1", reason="performance test, set SQL2DELTA_PERF=1"
)


def _exec(connection: "DB_Connection", conf_name: str, *sqls: str):
    with connection.new_connection(conf_name) as nc:
        for sql in sqls:
            with nc.cursor() as cursor:
                cursor.execute(sql)


def _delta_rows(dest) -> "tuple[int, int]":
    from deltalake import DeltaTable

    data = DeltaTable(str(dest / "delta")).to_pyarrow_dataset().count_rows()
    lpk = (
        DeltaTable(str(dest / "delta_load" / "latest_pk_version"))
        .to_pyarrow_dataset()
        .count_rows()
    )
    return data, lpk


def test_native_vs_python_1m(
    connection: "DB_Connection", monkeypatch: pytest.MonkeyPatch
):
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake.destination.file_system import FileSystemDestination
    from odbc2deltalake.load_result import DeltaLoadResult, NoLoadResult
    from odbc2deltalake.reader.arrowsql_reader import ArrowSQLReader

    if connection.source_server != "mssql":
        pytest.skip("SQL Server only")
    conf_name = next(iter(connection.conn_str))
    _exec(
        connection,
        conf_name,
        "DROP TABLE IF EXISTS dbo.perf_1m",
        """CREATE TABLE dbo.perf_1m (
            id int NOT NULL PRIMARY KEY,
            code varchar(20) NOT NULL,
            name nvarchar(100) NULL,
            amount decimal(18, 4) NULL,
            created datetime2(6) NOT NULL,
            qty int NULL,
            flag bit NULL,
            rv rowversion)""",
        f"""WITH n AS (
                SELECT TOP ({ROWS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
            INSERT INTO dbo.perf_1m (id, code, name, amount, created, qty, flag)
            SELECT i, CONCAT('C', i % 1000), CONCAT(N'name ', i), i * 1.25,
                DATEADD(second, i, '2020-01-01'), i % 97, CAST(i % 2 AS bit) FROM n""",
    )
    reader = ArrowSQLReader(connection.conn_str[conf_name])
    cfg = WriteConfig(load_mode="append", dialect="tsql")
    dests = {}
    for path in ["python", "native"]:
        p = Path(f"tests/_data/perf/{path}")
        if p.exists():
            shutil.rmtree(p)
        dests[path] = FileSystemDestination(p)

    timings: dict[str, dict[str, float]] = {"python": {}, "native": {}}

    def _run(phase: str, path: str):
        monkeypatch.setenv("SQL2DELTA_NATIVE", "1" if path == "native" else "0")
        start = time.perf_counter()
        res = write_db_to_delta(reader, ("dbo", "perf_1m"), dests[path], cfg)
        timings[path][phase] = time.perf_counter() - start
        return res

    for path in ["python", "native"]:
        _run("full", path)
        assert _delta_rows(dests[path]) == (ROWS, ROWS)
    for path in ["python", "native"]:
        assert isinstance(_run("unchanged", path), NoLoadResult)

    changed = ROWS // 100
    _exec(
        connection,
        conf_name,
        f"UPDATE dbo.perf_1m SET qty = qty + 1, name = CONCAT(name, N'!') WHERE id % 100 = 7",
    )
    time.sleep(1)
    for path in ["python", "native"]:
        res = _run("changed 1%", path)
        assert isinstance(res, DeltaLoadResult) and not res.dirty
        assert _delta_rows(dests[path]) == (ROWS + changed, ROWS)

    print(f"\n{'phase':<12} {'python s':>10} {'native s':>10} {'speedup':>8}  ({ROWS} rows)")
    for phase in ["full", "unchanged", "changed 1%"]:
        py, nat = timings["python"][phase], timings["native"][phase]
        print(f"{phase:<12} {py:>10.2f} {nat:>10.2f} {py / nat:>7.1f}x")
