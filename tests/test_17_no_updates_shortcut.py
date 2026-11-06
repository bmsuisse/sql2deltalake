import time
from typing import TYPE_CHECKING
import pytest
from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def _pk_ts_version(reader, dest) -> int:
    return reader.get_local_delta_ops(dest / "delta_load" / "primary_keys_ts").version()


def _assert_no_update_load(res, is_pg: bool, msg: str = "unchanged load must take the shortcut"):
    """An unchanged source: the shortcut on SQL Server; on Postgres the shortcut is off by design
    (xmin can commit late), so a delta load runs that finds nothing."""
    from odbc2deltalake.load_result import NoLoadResult

    if is_pg:
        assert not isinstance(res, NoLoadResult), "no shortcut for xmin"
    else:
        assert isinstance(res, NoLoadResult), msg


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
    _assert_no_update_load(res, is_pg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])
    if version is not None and not is_pg:
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
    _assert_no_update_load(res, is_pg)
    assert _delta_stats_for(dest, conf_name) == (6, 4, [2, 3])


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
    from odbc2deltalake import _native
    from odbc2deltalake.load_result import NoLoadResult

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/rv_failed")[
        conf_name
    ]
    is_pg = reader.source_dialect == "postgres"
    _create_shortcut_table(connection, conf_name, "rv_failed")
    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])
    write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)  # writes primary_keys_ts
    pk_ts_before = _pk_ts_version(reader, dest) if _has_pk_ts(dest) else None

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                "UPDATE dbo.rv_failed SET val = 'changed' WHERE id IN (1, 3)"
            )
    time.sleep(1)

    def _fail(name):
        if name == "load_updates":
            raise RuntimeError("simulated failure while loading the updates")

    with monkeypatch.context() as m:
        m.setattr(_native, "fail_point", _fail)
        with pytest.raises(RuntimeError, match="simulated failure"):
            write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])
    if pk_ts_before is not None and conf_name != "spark":
        # the snapshot the failed load read is rolled back with latest_pk_version
        assert _pk_ts_snapshot(reader, dest) == _pk_ts_snapshot(
            reader, dest, pk_ts_before
        )

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    assert not isinstance(res, NoLoadResult), "the retry must load the updates"
    assert _delta_stats_for(dest, conf_name) == (6, 4, [1, 3])

    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_failed"), dest, cfg)
    _assert_no_update_load(res, is_pg)


def _pk_ts_snapshot(reader, dest, version=None):
    from deltalake import DeltaTable

    path, opts = (dest / "delta_load" / "primary_keys_ts").as_path_options(
        "object_store"
    )
    rows = DeltaTable(path, version=version, storage_options=opts).to_pyarrow_table()
    return sorted(tuple(r.values()) for r in rows.to_pylist())


@pytest.mark.order(22)
@pytest.mark.parametrize("conf_name", config_names)
def test_stale_pk_snapshot_does_not_hide_updates(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Fix, then retry: primary_keys_ts already holds the source's current keys and rowversions
    (a snapshot read by a load that did not finish, left behind), latest_pk_version the older
    ones. The next load must still load the updates: the shortcut does not trust primary_keys_ts."""
    import pyarrow as pa
    from deltalake import write_deltalake
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake.load_result import NoLoadResult

    if connection.source_server != "mssql" or conf_name == "spark":
        pytest.skip("writes a rowversion snapshot with delta-rs")
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/rv_stale")[
        conf_name
    ]
    _create_shortcut_table(connection, conf_name, "rv_stale")
    cfg = WriteConfig(load_mode="append", primary_keys=["id"], dialect="tsql")
    write_db_to_delta(reader, ("dbo", "rv_stale"), dest, cfg)
    write_db_to_delta(reader, ("dbo", "rv_stale"), dest, cfg)
    assert _delta_stats_for(dest, conf_name) == (4, 4, [])

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("UPDATE dbo.rv_stale SET val = 'changed' WHERE id IN (1, 3)")
    stale = reader.source_sql_to_py(
        "SELECT id, CAST(rv AS BIGINT) AS rv FROM dbo.rv_stale"
    )
    path, opts = (dest / "delta_load" / "primary_keys_ts").as_path_options(
        "object_store"
    )
    write_deltalake(
        path,
        pa.table(
            {
                "id": pa.array([r["id"] for r in stale], pa.int32()),
                "rv": pa.array([r["rv"] for r in stale], pa.int64()),
            }
        ),
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=opts,
    )
    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_stale"), dest, cfg)
    assert not isinstance(res, NoLoadResult), "the updates must be loaded"
    assert _delta_stats_for(dest, conf_name) == (6, 4, [1, 3])
    time.sleep(1)
    res = write_db_to_delta(reader, ("dbo", "rv_stale"), dest, cfg)
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
    from .conftest import get_conn, DB_PREFIX

    if connection.source_server != "mssql" or conf_name == "spark":
        pytest.skip("rowversion and read committed snapshot are SQL Server only")
    reader_base, _ = get_test_run_configs(connection, spark_session, "dbo/rv_late")[
        conf_name
    ]
    db_name = DB_PREFIX + "rcsi"
    conn_str = connection.conn_str[conf_name].replace(
        f"database={DB_PREFIX}{conf_name}", "database=" + db_name
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


@pytest.mark.order(22)
@pytest.mark.parametrize("conf_name", config_names)
def test_no_shortcut_for_auto_detected_row_start(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """A system-versioned table's AS ROW START column is the transaction start time, which can
    commit late: picked automatically as delta column, it never allows the shortcut. Configured
    explicitly, it is the caller's choice."""
    from odbc2deltalake import WriteConfig, write_db_to_delta
    from odbc2deltalake.load_result import NoLoadResult

    if connection.source_server != "mssql":
        pytest.skip("system-versioned tables are SQL Server only")
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/rs_shortcut")[
        conf_name
    ]
    with connection.new_connection(conf_name) as nc:
        for sql in [
            "IF OBJECT_ID('dbo.rs_shortcut') IS NOT NULL ALTER TABLE dbo.rs_shortcut SET (SYSTEM_VERSIONING = OFF)",
            "DROP TABLE IF EXISTS dbo.rs_shortcut",
            "DROP TABLE IF EXISTS dbo.rs_shortcut_history",
            """CREATE TABLE dbo.rs_shortcut (id int primary key, val varchar(50),
               vf datetime2 GENERATED ALWAYS AS ROW START, vt datetime2 GENERATED ALWAYS AS ROW END,
               PERIOD FOR SYSTEM_TIME (vf, vt)) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.rs_shortcut_history))""",
            "INSERT INTO dbo.rs_shortcut (id, val) VALUES (1, 'a'), (2, 'b')",
        ]:
            with nc.cursor() as cursor:
                cursor.execute(sql)
    auto = WriteConfig(load_mode="append", primary_keys=["id"], dialect="tsql")
    write_db_to_delta(reader, ("dbo", "rs_shortcut"), dest, auto)
    res = write_db_to_delta(reader, ("dbo", "rs_shortcut"), dest, auto)
    assert not isinstance(res, NoLoadResult), "no shortcut for an automatic AS ROW START"
    assert _delta_stats_for(dest, conf_name) == (2, 2, [])

    explicit = WriteConfig(
        load_mode="append", primary_keys=["id"], delta_col="vf", dialect="tsql"
    )
    write_db_to_delta(reader, ("dbo", "rs_shortcut"), dest, explicit)
    res = write_db_to_delta(reader, ("dbo", "rs_shortcut"), dest, explicit)
    assert isinstance(res, NoLoadResult)
