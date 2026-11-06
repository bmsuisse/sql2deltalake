"""The Rust query builder (odbc2deltalake._core) against the SQL the sqlglot implementation
generates, for the cases of tests/sql_golden.py. No database needed."""

import json
from pathlib import Path

import pytest

from .sql_golden import OUT, cases

core = pytest.importorskip("odbc2deltalake._core")


def _value(spec):
    import datetime as dt
    import decimal

    (k, v), = spec.items()
    if k == "decimal":
        return decimal.Decimal(v[0]).scaleb(-v[1])
    if k == "bytes":
        return bytes.fromhex(v)
    if k == "timestamp":
        ts = dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=v[0])
        return ts.replace(tzinfo=dt.timezone.utc) if v[1] else ts
    if k == "date":
        return dt.date(1970, 1, 1) + dt.timedelta(days=v)
    return v


def _check(case):
    b = core.SqlBuilder(
        case["dialect"], case["source"], case["type_map"], case["no_trim"], case["columns"]
    )
    exp = case["expected"]
    lit = core.load_timestamp_literal(case["dialect"], case["now_micros"])
    assert lit == exp["load_timestamp"]
    assert b.full_load() == exp["full_load_clock"]
    assert b.full_load(lit) == exp["full_load_literal"]
    n = len(case["columns"])
    assert b.pk_ts([0, 4, n - 1]) == exp["pk_ts"]
    assert b.source_ts_cnt(n - 1) == exp["source_ts_cnt"]
    assert b.updates([]) == exp["updates_no_criterion"]
    for u in exp["updates"]:
        crit = b.criterion(u["column"], u["op"], _value(u["value"]))
        assert b.updates([crit], lit) == u["sql"]
    for r in exp["rows_by_pk"]:
        assert b.rows_by_pk_json(r["pks"], r["json"], lit) == r["sql"]


@pytest.mark.parametrize("case", cases(), ids=lambda c: f"{c['dialect']}-{c['source']}")
def test_rust_sql_matches_sqlglot(case):
    _check(case)


def test_snapshot_is_current():
    """The snapshot cargo test uses equals what sqlglot generates now."""
    assert json.loads(Path(OUT).read_text(encoding="utf-8")) == json.loads(
        json.dumps(cases(), ensure_ascii=False)
    )


def test_compat_name():
    from odbc2deltalake.write_init import compat_name
    from odbc2deltalake.metadata import InformationSchemaColInfo
    import sqlglot.expressions as ex

    for name in ["User - iD", "a,b;c{d}(e)\nf\tg=h", "plain"]:
        c = InformationSchemaColInfo(
            column_name=name, data_type=ex.DataType.build("int"), data_type_str="int"
        )
        assert core.compat_name(name) == compat_name(c)
