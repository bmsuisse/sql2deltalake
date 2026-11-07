"""SQL Server types beyond the common ones: text/ntext load as strings, sql_variant and
hierarchyid are a clear error before anything is read, and sub-microsecond digits of datetime2(7)
and datetimeoffset(7) are rounded the same way (Delta keeps microseconds)."""

import pytest

from .test_32_load_edge_cases import _delta_rows, _exec, _skip_unless_mssql
from .utils import config_names, get_test_run_configs


@pytest.mark.order(31)
@pytest.mark.parametrize("impl", ["native", "python"])
@pytest.mark.parametrize("typ,value", [("text", "'abc '"), ("ntext", "N'äbc '")])
@pytest.mark.parametrize("conf_name", config_names)
def test_text_and_ntext_load(connection, spark_session, conf_name, typ, value, impl, monkeypatch):
    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1" if impl == "native" else "0")
    table = f"type_{typ}_{impl}"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{table}",
        f"CREATE TABLE dbo.{table} (id int NOT NULL PRIMARY KEY, v {typ}, rv rowversion)",
        f"INSERT INTO dbo.{table} (id, v) VALUES (1, {value}), (2, NULL)",
    )
    cfg = WriteConfig(primary_keys=["id"], delta_col="rv", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    _exec(connection, conf_name, f"UPDATE dbo.{table} SET v = {value} WHERE id = 2")
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    expected = value.lstrip("N").strip("'").strip()
    rows = [(r["id"], r["v"]) for r in _delta_rows(dest)]
    assert sorted(rows, key=lambda r: (r[0], r[1] is not None)) == [
        (1, expected),
        (2, None),
        (2, expected),
    ]


@pytest.mark.order(31)
@pytest.mark.parametrize("typ,value", [("sql_variant", "CAST(5 AS sql_variant)"), ("hierarchyid", "hierarchyid::Parse('/1/2/')")])
@pytest.mark.parametrize("conf_name", config_names)
def test_unsupported_types_are_a_clear_error(connection, spark_session, conf_name, typ, value):
    """The error names the column and its type, and comes before any source read or write."""
    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    table = f"type_{typ}"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{table}",
        f"CREATE TABLE dbo.{table} (id int NOT NULL PRIMARY KEY, odd_col {typ}, rv rowversion)",
        f"INSERT INTO dbo.{table} (id, odd_col) VALUES (1, {value})",
    )
    cfg = WriteConfig(primary_keys=["id"], delta_col="rv", dialect=reader.source_dialect)
    with pytest.raises(ValueError, match=rf"odd_col.*{typ}"):
        write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert not (dest / "delta").exists()


@pytest.mark.order(31)
@pytest.mark.parametrize("impl", ["native", "python"])
@pytest.mark.parametrize("conf_name", config_names)
def test_datetime2_and_datetimeoffset_round_alike(connection, spark_session, conf_name, impl, monkeypatch):
    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1" if impl == "native" else "0")
    table = f"type_prec_{impl}"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    values = ", ".join(
        f"({i}, '2020-01-01 00:00:00.123456{i}', '2020-01-01 00:00:00.123456{i} +00:00')" for i in range(10)
    )
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{table}",
        f"CREATE TABLE dbo.{table} (id int NOT NULL PRIMARY KEY, d datetime2(7), o datetimeoffset(7), rv rowversion)",
        f"INSERT INTO dbo.{table} (id, d, o) VALUES {values}",
    )
    cfg = WriteConfig(primary_keys=["id"], delta_col="rv", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    rows = sorted(_delta_rows(dest), key=lambda r: r["id"])
    assert [r["d"].microsecond for r in rows] == [r["o"].microsecond for r in rows]
    assert [r["d"].microsecond for r in rows] == [123456] * 5 + [123457] * 5
