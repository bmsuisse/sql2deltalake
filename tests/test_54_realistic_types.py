"""Realistic native load tests: SQL Server types whose load fails or loses precision (each one a table).

    SQL2DELTA_REALISTIC=1 scripts/run_tests_local.sh tests/test_54_realistic_types.py -s
"""

import datetime as dt

import pytest

from .rt_support import Src, check_state, delta_rows, ensure_db, fresh_dest, load, needs_realistic

pytestmark = [pytest.mark.realistic, needs_realistic]


@pytest.mark.parametrize("typ,value", [("text", "'abc'"), ("ntext", "N'abc'")])
def test_legacy_text_types_load(typ: str, value: str):
    """B7 (fixed): text/ntext failed with 'Argument data type text is invalid for argument 1 of Trim function';
    they are read as nvarchar(max)."""
    ensure_db()
    s = Src()
    t = f"rt_type_{typ}"
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, v {typ}, rv rowversion)", f"INSERT INTO dbo.{t} (id, v) VALUES (1, {value}), (2, NULL)")
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    assert len(delta_rows(dest)) == 2


@pytest.mark.parametrize(
    "typ,value",
    [("sql_variant", "CAST(5 AS sql_variant)"), ("hierarchyid", "hierarchyid::Parse('/1/2/')")],
)
def test_exotic_types_fail_early_and_clearly(typ: str, value: str):
    """B7 (fixed): sql_variant failed in arrowsql ('Unsupported data type for Arrow batch fetch'), hierarchyid with a
    sqlglot ParseError; now a ValueError naming column and type before anything is read."""
    ensure_db()
    s = Src()
    t = f"rt_type_{typ}"
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, v {typ}, rv rowversion)", f"INSERT INTO dbo.{t} (id, v) VALUES (1, {value}), (2, NULL)")
    dest = fresh_dest(t)
    with pytest.raises(ValueError, match=rf"'v'.*{typ}"):
        load(t, f"dbo.{t}", dest)


def test_datetime2_7_rounds_but_datetimeoffset_7_truncates():
    """B5 (fixed): Delta keeps microseconds. datetime2(7) was rounded half up to the microsecond, datetimeoffset(7)
    truncated: the same 7th digit gave different results per type (datetime2(7) keys 100ns apart collapse: B4)."""
    ensure_db()
    s = Src()
    t = "rt_prec"
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, d datetime2(7), o datetimeoffset(7), rv rowversion)")
    for i in range(10):
        s.x(f"INSERT INTO dbo.{t} (id, d, o) VALUES ({i}, '2020-01-01 00:00:00.123456{i}', '2020-01-01 00:00:00.123456{i} +00:00')")
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    rows = sorted(delta_rows(dest), key=lambda r: r["id"])
    d_us = [r["d"].microsecond for r in rows]
    o_us = [r["o"].microsecond for r in rows]
    assert d_us == o_us, f"datetime2(7) -> {d_us}, datetimeoffset(7) -> {o_us}"
