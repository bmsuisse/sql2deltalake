import time
from typing import TYPE_CHECKING
import pytest
from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def _pk_ts_version(reader, dest) -> int:
    return reader.get_local_delta_ops(dest / "delta_load" / "primary_keys_ts").version()


@pytest.mark.order(22)
@pytest.mark.parametrize("conf_name", config_names)
def test_unchanged_load_takes_shortcut(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake.load_result import NoLoadResult

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/rv_shortcut")[
        conf_name
    ]
    is_pg = reader.source_dialect == "postgres"
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS dbo.rv_shortcut")
        with nc.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE dbo.rv_shortcut (id int primary key, val varchar(50))"
                if is_pg
                else "CREATE TABLE dbo.rv_shortcut (id int primary key, val varchar(50), rv rowversion)"
            )
        with nc.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dbo.rv_shortcut (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
            )

    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])

    time.sleep(1)
    version = _pk_ts_version(reader, dest) if _has_pk_ts(dest) else None
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert isinstance(res, NoLoadResult), "unchanged load must take the shortcut"
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])
    if version is not None:
        assert _pk_ts_version(reader, dest) == version, "PK/TS must not be re-read"

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("UPDATE dbo.rv_shortcut SET val = 'changed' WHERE id = 2")
    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert not isinstance(res, NoLoadResult)
    assert _delta_stats_for(dest, conf_name) == (5, 4, [2])

    # a delete without any other change: count differs, so the shortcut must not skip it
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("DELETE FROM dbo.rv_shortcut WHERE id = 3")
    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert not isinstance(res, NoLoadResult)
    assert _delta_stats_for(dest, conf_name) == (6, 4, [2, 3])
    assert _deleted_ids(dest, conf_name) == [3]

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_shortcut"), dest, cfg)
    assert isinstance(res, NoLoadResult)


def _create_shortcut_table(connection: "DB_Connection", conf_name: str, name: str):
    is_pg = connection.source_server == "postgres"
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS dbo.{name}")
        with nc.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE dbo.{name} (id int primary key, val varchar(50))"
                if is_pg
                else f"CREATE TABLE dbo.{name} (id int primary key, val varchar(50), rv rowversion)"
            )
        with nc.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
            )


@pytest.mark.order(22)
@pytest.mark.parametrize("conf_name", config_names)
def test_failed_load_does_not_hide_updates(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """A load that fails after it read the source PK snapshot must not make the next load
    believe that the target is up to date."""
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake import db_to_delta
    from odbc2deltalake.load_result import NoLoadResult

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/rv_failed")[
        conf_name
    ]
    _create_shortcut_table(connection, conf_name, "rv_failed")
    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                "UPDATE dbo.rv_failed SET val = 'changed' WHERE id IN (1, 3)"
            )
    time.sleep(1)

    def _fail(*args, **kwargs):
        raise RuntimeError("simulated failure while loading the updates")

    with monkeypatch.context() as m:
        m.setattr(db_to_delta, "_load_updates_to_delta", _fail)
        with pytest.raises(RuntimeError, match="simulated failure"):
            write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    assert not isinstance(res, NoLoadResult), "the retry must load the updates"
    assert _delta_stats_for(dest, conf_name) == (6, 4, [1, 3])

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    assert isinstance(res, NoLoadResult)


@pytest.mark.order(22)
@pytest.mark.parametrize("conf_name", config_names)
def test_late_commit_of_lower_rowversion_is_loaded(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Under read committed snapshot a transaction can hold a lower rowversion while a higher one
    is committed and loaded. When it commits later, MAX(rowversion) and COUNT(*) do not change,
    so the no-updates shortcut must not be taken."""
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake.load_result import NoLoadResult
    from .conftest import get_conn

    if connection.source_server != "mssql" or conf_name == "spark":
        pytest.skip("rowversion and read committed snapshot are SQL Server only")
    reader_base, _ = get_test_run_configs(connection, spark_session, "dbo/rv_late")[
        conf_name
    ]
    db_name = "db_to_delta_test_rcsi"
    conn_str = connection.conn_str[conf_name].replace(
        f"database=db_to_delta_test_{conf_name}", "database=" + db_name
    )
    assert db_name in conn_str
    with get_conn(connection.conn_str_master, autocommit=True) as master:
        with master.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        with master.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {db_name}")
        with master.cursor() as cursor:
            cursor.execute(f"ALTER DATABASE {db_name} SET READ_COMMITTED_SNAPSHOT ON")
    with get_conn(conn_str, autocommit=True) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE dbo.rv_late (id int primary key, val varchar(50), rv rowversion)"
            )
        with nc.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dbo.rv_late (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
            )

    from pathlib import Path
    from odbc2deltalake.destination.file_system import FileSystemDestination

    reader = type(reader_base)(
        conn_str, "tests/_db/_rv_late.duckdb", source_dialect=connection.dialect
    )
    dest = FileSystemDestination(Path("tests/_data/dbo/rv_late"))
    cfg = WriteConfig(load_mode="append", primary_keys=["id"], dialect="tsql")
    write_db_to_delta(reader, ("dbo", "rv_late"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])

    late = get_conn(conn_str, autocommit=False)
    try:
        with (
            late.cursor() as cursor
        ):  # takes the lower rowversion, commits after the load
            cursor.execute("UPDATE dbo.rv_late SET val = 'late' WHERE id = 1")
        with get_conn(conn_str, autocommit=True) as nc:
            with nc.cursor() as cursor:
                cursor.execute("UPDATE dbo.rv_late SET val = 'early' WHERE id = 2")
        time.sleep(1)
        res = write_db_to_delta(reader, ("dbo", "rv_late"), dest, cfg)
        assert not isinstance(res, NoLoadResult)
        assert _delta_stats_for(dest, conf_name) == (5, 4, [2])
        late.commit()
    finally:
        late.close()

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_late"), dest, cfg)
    assert not isinstance(res, NoLoadResult), "the late commit must be loaded"
    assert _delta_stats_for(dest, conf_name) == (6, 4, [1, 2])

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_late"), dest, cfg)
    assert isinstance(res, NoLoadResult), (
        "a load without changes still takes the shortcut"
    )

    with get_conn(connection.conn_str_master, autocommit=True) as master:
        with master.cursor() as cursor:
            cursor.execute(
                f"ALTER DATABASE {db_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            )
        with master.cursor() as cursor:
            cursor.execute(f"DROP DATABASE {db_name}")


def _has_pk_ts(dest) -> bool:
    return (dest / "delta_load" / "primary_keys_ts").exists()


def _view(con, dest, conf_name):
    from deltalake2db import duckdb_create_view_for_delta

    duckdb_create_view_for_delta(
        con,
        (dest / "delta").as_delta_table(),
        "v_sc",
        use_delta_ext=conf_name == "spark",
    )


def _delta_stats_for(dest, conf_name: str):
    import duckdb

    with duckdb.connect() as con:
        _view(con, dest, conf_name)
        total = con.execute("SELECT count(*) FROM v_sc").fetchall()[0][0]
        full = con.execute(
            'SELECT count(*) FROM v_sc WHERE "__is_full_load"'
        ).fetchall()[0][0]
        changed = [
            r[0]
            for r in con.execute(
                'SELECT id FROM v_sc WHERE NOT "__is_full_load" ORDER BY id'
            ).fetchall()
        ]
        return total, full, changed


def _deleted_ids(dest, conf_name: str):
    import duckdb

    with duckdb.connect() as con:
        _view(con, dest, conf_name)
        return [
            r[0]
            for r in con.execute(
                'SELECT id FROM v_sc WHERE "__is_deleted" ORDER BY id'
            ).fetchall()
        ]
