"""The per-pass load timestamp with clocks that stand still, go back or fail, and where
MIN_ACTIVE_ROWVERSION() is read (it is per database)."""

import dataclasses
import datetime as dt
import logging
from typing import Any

import pytest
import sqlglot as sg

from sql2deltalake.db_to_delta import (
    _fetch_load_timestamp,
    _load_timestamp_value,
    _next_pass,
    _watermark_sql,
)

US = dt.timedelta(microseconds=1)


class _Clock:
    """A source whose clock query returns the given values (or raises them)."""

    def __init__(self, *values: Any):
        self.values = list(values)

    def source_sql_to_py(self, sql: str) -> list:
        v = self.values.pop(0)
        if isinstance(v, Exception):
            raise v
        return [{"ts": v}]


@dataclasses.dataclass
class _Infos:
    load_timestamp: Any
    source: Any
    write_config: Any
    logger: Any = logging.getLogger("test_19")


@pytest.mark.parametrize("dialect", ["tsql", "postgres"])
def test_pass_timestamps_increase_with_frozen_and_backward_clocks(dialect):
    noon = dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc if dialect == "postgres" else None)
    first = _fetch_load_timestamp(_Clock(noon), dialect)
    assert first is not None
    frozen = _fetch_load_timestamp(_Clock(noon), dialect, after=first)
    assert frozen is not None
    back = _fetch_load_timestamp(_Clock(noon - dt.timedelta(hours=1)), dialect, after=frozen)
    assert back is not None
    values = [_load_timestamp_value(t) for t in (first, frozen, back)]
    assert values[1] == values[0] + US  # type: ignore[operator]
    assert values[2] == values[1] + US  # type: ignore[operator]


@pytest.mark.parametrize("dialect", ["tsql", "postgres"])
def test_failed_clock_read_uses_previous_plus_one_microsecond(dialect):
    noon = dt.datetime(2024, 1, 1, 12)
    first = _fetch_load_timestamp(_Clock(noon), dialect)
    infos = _Infos(
        load_timestamp=first,
        source=_Clock(RuntimeError("clock unavailable")),
        write_config=dataclasses.make_dataclass("WC", [("dialect", str)])(dialect),
    )
    nxt = _next_pass(infos)  # type: ignore[arg-type]
    before, after = _load_timestamp_value(first), _load_timestamp_value(nxt.load_timestamp)
    assert before is not None and after is not None
    assert after.replace(tzinfo=None) == before.replace(tzinfo=None) + US


def _watermark(table_or_query: Any):
    from sql2deltalake.metadata import InformationSchemaColInfo
    from sql2deltalake.write_init import WriteConfig, WriteConfigAndInfos
    import sqlglot.expressions as ex

    rv = InformationSchemaColInfo(
        column_name="rv", data_type=ex.DataType.build("bigint"), data_type_str="rowversion"
    )
    infos = WriteConfigAndInfos(
        col_infos=[rv],
        pk_cols=[],
        delta_col=rv,
        write_config=WriteConfig(dialect="tsql"),
        destination=None,  # type: ignore[arg-type]
        source=None,  # type: ignore[arg-type]
        table_or_query=table_or_query,
        logger=logging.getLogger("test_19"),  # type: ignore[arg-type]
    )
    return _watermark_sql(infos)


def test_watermark_is_read_in_the_tables_database():
    sql = _watermark(("other", "dbo", "t"))
    assert sql is not None and sql.startswith("EXEC [other].sys.sp_executesql N'")
    local = _watermark(("dbo", "t"))
    assert local is not None and not local.startswith("EXEC")
    assert "OBJECT_ID(N''[dbo].[t]'')" not in local and "OBJECT_ID(N'[dbo].[t]')" in local
    query = sg.parse_one("SELECT * FROM other.dbo.t", dialect="tsql")
    assert (_watermark(query) or "").startswith("EXEC [other]")
    two = sg.parse_one("SELECT * FROM a.dbo.t JOIN b.dbo.u ON 1 = 1", dialect="tsql")
    assert _watermark(two) is None  # several databases: no watermark, no shortcut


def test_watermark_of_another_database_and_cross_database_view(connection):
    """Live: MIN_ACTIVE_ROWVERSION() of a three-part name comes from that database, a view
    reading another database has none."""
    import os

    from .conftest import DB_PREFIX, get_conn

    if connection.source_server != "mssql" or (
        os.getenv("ODBCLAKE_TEST_CONFIGURATION") or ""
    ).lower() not in ("arrowsql", "local"):
        pytest.skip("SQL Server through arrowsql")
    from sql2deltalake.reader.arrowsql_reader import ArrowSQLSource

    conf = "arrowsql" if "arrowsql" in connection.conn_str else next(iter(connection.conn_str))
    other = DB_PREFIX + "xdb"
    with get_conn(connection.conn_str_master, autocommit=True) as master:
        with master.cursor() as c:
            c.execute(f"DROP DATABASE IF EXISTS {other}")
        with master.cursor() as c:
            c.execute(f"CREATE DATABASE {other}")
    try:
        with get_conn(connection.conn_str_master, autocommit=True) as master:
            with master.cursor() as c:
                c.execute(
                    f"CREATE TABLE {other}.dbo.t (id int primary key, rv rowversion); "
                    f"INSERT INTO {other}.dbo.t (id) VALUES (1), (2)"
                )
        source = ArrowSQLSource(connection.conn_str[conf])
        sql = _watermark((other, "dbo", "t"))
        assert sql is not None
        got = source.source_sql_to_py(sql)[0]["mar"]
        expected = source.source_sql_to_py(
            f"EXEC {other}.sys.sp_executesql N'SELECT CAST(MIN_ACTIVE_ROWVERSION() AS BIGINT) AS mar'"
        )[0]["mar"]
        here = source.source_sql_to_py("SELECT CAST(MIN_ACTIVE_ROWVERSION() AS BIGINT) AS mar")[0]["mar"]
        assert got == expected
        assert got != here or expected == here
        with connection.new_connection(conf) as nc:
            with nc.cursor() as c:
                c.execute("DROP VIEW IF EXISTS dbo.v_xdb")
            with nc.cursor() as c:
                c.execute(f"CREATE VIEW dbo.v_xdb AS SELECT id, rv FROM {other}.dbo.t")
        view_sql = _watermark(("dbo", "v_xdb"))
        assert view_sql is not None
        assert source.source_sql_to_py(view_sql)[0]["mar"] is None
    finally:
        with get_conn(connection.conn_str_master, autocommit=True) as master:
            with master.cursor() as c:
                c.execute(
                    f"IF DB_ID('{other}') IS NOT NULL BEGIN ALTER DATABASE {other} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {other}; END"
                )
