"""Edge cases of the delta load: keys that repeat in the PK snapshot, readers that copy the source
to Delta themselves, interrupts and failed restores. Both implementations (native and
`SQL2DELTA_NATIVE=0`) must pass them."""

import time
from typing import TYPE_CHECKING

import pytest

from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from tests.conftest import DB_Connection


def _exec(connection: "DB_Connection", conf_name: str, *stmts: str):
    with connection.new_connection(conf_name) as nc:
        for stmt in stmts:
            with nc.cursor() as cursor:
                cursor.execute(stmt)


def _delta_rows(dest, rel: str = "delta") -> list[dict]:
    from deltalake import DeltaTable

    table = (dest / rel).as_delta_table()
    if not isinstance(table, DeltaTable):
        table = DeltaTable(table)
    return table.to_pyarrow_table().to_pylist()


def _skip_unless_mssql(connection: "DB_Connection"):
    if connection.source_server != "mssql":
        pytest.skip("SQL Server only")


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_keys_equal_after_trim_are_not_deleted(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """'a' and ' a' are two rows in SQL Server and one key after TRIM: an update of a third row
    must not mark 'a' as deleted nor read it again."""
    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/dup_trim")[conf_name]
    _exec(
        connection,
        conf_name,
        "DROP TABLE IF EXISTS dbo.dup_trim",
        "CREATE TABLE dbo.dup_trim (id nvarchar(10) NOT NULL PRIMARY KEY, val int, rv rowversion)",
        "INSERT INTO dbo.dup_trim (id, val) VALUES (N'a', 1), (N' a', 2), (N'c', 3)",
    )
    cfg = WriteConfig(primary_keys=["id"], dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", "dup_trim"), dest, cfg)
    time.sleep(1)
    _exec(connection, conf_name, "UPDATE dbo.dup_trim SET val = 30 WHERE id = N'c'")
    write_db_to_delta(reader, ("dbo", "dup_trim"), dest, cfg)

    rows = _delta_rows(dest)
    assert [r for r in rows if r["__is_deleted"]] == []
    assert sorted((r["id"], r["val"]) for r in rows if not r["__is_full_load"]) == [("c", 30)]
    assert len(rows) == 4


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_user_keys_of_a_view_may_repeat(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """User-set primary keys of a view that are not unique: an update of another key must not
    delete or re-read the repeated key."""
    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/v_dup_keys")[conf_name]
    _exec(
        connection,
        conf_name,
        "DROP VIEW IF EXISTS dbo.v_dup_keys",
        "DROP TABLE IF EXISTS dbo.dup_keys",
        "CREATE TABLE dbo.dup_keys (id int NOT NULL PRIMARY KEY, grp int, val int, rv rowversion)",
        "INSERT INTO dbo.dup_keys (id, grp, val) VALUES (1, 10, 1), (2, 10, 2), (3, 20, 3)",
        "CREATE VIEW dbo.v_dup_keys AS SELECT grp, val, rv FROM dbo.dup_keys",
    )
    cfg = WriteConfig(primary_keys=["grp"], delta_col="rv", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", "v_dup_keys"), dest, cfg)
    time.sleep(1)
    _exec(connection, conf_name, "UPDATE dbo.dup_keys SET val = 30 WHERE id = 3")
    write_db_to_delta(reader, ("dbo", "v_dup_keys"), dest, cfg)

    rows = _delta_rows(dest)
    assert [r for r in rows if r["__is_deleted"]] == []
    assert sorted((r["grp"], r["val"]) for r in rows if not r["__is_full_load"]) == [(20, 30)]
    assert len(rows) == 4


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_copying_reader_strange_updates_in_chunks(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """A reader that writes source queries to Delta itself (`source_write_sql_to_delta`): the
    strange updates are read by key in several appended chunks, each row must land once."""
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake.reader.arrowsql_reader import ArrowSQLReader

    _skip_unless_mssql(connection)
    base, dest = get_test_run_configs(connection, spark_session, "dbo/copy_chunks")[conf_name]
    if not isinstance(base, ArrowSQLReader):
        pytest.skip("needs ArrowSQLReader")

    class CopyingReader(ArrowSQLReader):
        modes: list = []

        def source_write_sql_to_delta(self, sql, delta_path, mode, *, allow_schema_drift):
            if "OPENJSON" in sql:
                self.modes.append(mode)
            return super().source_write_sql_to_delta(
                sql, delta_path, mode, allow_schema_drift=allow_schema_drift
            )

    reader = CopyingReader(
        base.connection_string, ":memory:", source_dialect=base.source_dialect
    )
    _exec(
        connection,
        conf_name,
        "DROP TABLE IF EXISTS dbo.copy_chunks",
        "CREATE TABLE dbo.copy_chunks (id nvarchar(40) NOT NULL PRIMARY KEY, val int, ts datetime2(6))",
        """WITH n AS (SELECT TOP (400) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                      FROM sys.all_objects)
           INSERT INTO dbo.copy_chunks (id, val, ts)
           SELECT CONCAT('k', FORMAT(i, '0000')), i, '2024-01-02' FROM n""",
    )
    cfg = WriteConfig(primary_keys=["id"], delta_col="ts", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", "copy_chunks"), dest, cfg)
    # 300 rows change to an older delta value (strange updates), one row is new
    _exec(
        connection,
        conf_name,
        "UPDATE dbo.copy_chunks SET val = -val, ts = '2024-01-01' WHERE val <= 300",
        "INSERT INTO dbo.copy_chunks (id, val, ts) VALUES ('k9999', 9999, '2024-01-03')",
    )
    write_db_to_delta(reader, ("dbo", "copy_chunks"), dest, cfg)

    assert len(CopyingReader.modes) >= 2 and "append" in CopyingReader.modes
    rows = [r for r in _delta_rows(dest) if not r["__is_full_load"]]
    assert len(rows) == 301
    assert len({r["id"] for r in rows}) == 301
    assert not any(r["__is_deleted"] for r in rows)


def _rv_table(connection: "DB_Connection", conf_name: str, name: str):
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{name}",
        f"CREATE TABLE dbo.{name} (id int NOT NULL PRIMARY KEY, val int, rv rowversion)",
        f"INSERT INTO dbo.{name} (id, val) VALUES (1, 1), (2, 2), (3, 3)",
    )


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_interrupting_reader_ends_the_load(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """A KeyboardInterrupt in the source read of the load timestamp is not 'could not read the
    clock, use the source clock per query': it ends the load."""
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake.reader.arrowsql_reader import ArrowSQLReader

    _skip_unless_mssql(connection)
    base, dest = get_test_run_configs(connection, spark_session, "dbo/interrupt_clock")[conf_name]
    if not isinstance(base, ArrowSQLReader):
        pytest.skip("needs ArrowSQLReader")

    class InterruptingReader(ArrowSQLReader):
        interrupted = False

        def source_sql_to_py(self, sql):
            if "GETUTCDATE() AS datetime2(6)) AS ts" in sql and not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt()
            return super().source_sql_to_py(sql)

    reader = InterruptingReader(
        base.connection_string, ":memory:", source_dialect=base.source_dialect
    )
    _rv_table(connection, conf_name, "interrupt_clock")
    cfg = WriteConfig(primary_keys=["id"], dialect=reader.source_dialect)
    with pytest.raises(KeyboardInterrupt):
        write_db_to_delta(reader, ("dbo", "interrupt_clock"), dest, cfg)
    assert reader.interrupted
    assert not (dest / "meta" / "lock.txt").exists()


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_interrupt_in_snapshot_diff_is_not_retried(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """A KeyboardInterrupt while the load compares the PK snapshot is passed on, not handled
    by restoring the primary keys and comparing again."""
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake import _native

    _skip_unless_mssql(connection)
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/interrupt_diff")[conf_name]
    _rv_table(connection, conf_name, "interrupt_diff")
    cfg = WriteConfig(primary_keys=["id"], dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", "interrupt_diff"), dest, cfg)
    _exec(connection, conf_name, "UPDATE dbo.interrupt_diff SET val = 20 WHERE id = 2")

    calls = []

    def fail_point(name):
        if name == "additional_updates":
            calls.append(name)
            if len(calls) == 1:
                raise KeyboardInterrupt()
        return None

    monkeypatch.setattr(_native, "fail_point", fail_point)
    with pytest.raises(KeyboardInterrupt):
        write_db_to_delta(reader, ("dbo", "interrupt_diff"), dest, cfg)
    assert calls == ["additional_updates"]


def _error_text(e: BaseException) -> str:
    parts = [str(e), *getattr(e, "__notes__", [])]
    if e.__context__ is not None:
        parts.append(str(e.__context__))
    return "\n".join(parts)


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_failed_restore_is_reported_with_the_load_error(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """A load fails after it wrote latest_pk_version and the restore of the old version fails
    too: the error says both."""
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake import _native

    _skip_unless_mssql(connection)
    from sql2deltalake.reader.arrowsql_reader import ArrowSQLReader

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/restore_fails")[conf_name]
    if not _native.enabled() or not isinstance(reader, ArrowSQLReader):
        pytest.skip("the restore of the native load")
    _rv_table(connection, conf_name, "restore_fails")
    cfg = WriteConfig(primary_keys=["id"], dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", "restore_fails"), dest, cfg)
    _exec(connection, conf_name, "UPDATE dbo.restore_fails SET val = 20 WHERE id = 2")

    def fail_point(name):
        if name == "local_pk_count":
            raise RuntimeError("load failed on purpose")
        return None

    def restore(self, rel, version):
        raise OSError(f"restore of {rel} failed on purpose")

    monkeypatch.setattr(_native, "fail_point", fail_point)
    monkeypatch.setattr(_native.Host, "restore", restore)
    with pytest.raises(RuntimeError) as ei:
        write_db_to_delta(reader, ("dbo", "restore_fails"), dest, cfg)
    text = _error_text(ei.value)
    assert "load failed on purpose" in text
    assert "restore of delta_load/latest_pk_version failed on purpose" in text


@pytest.mark.order(30)
@pytest.mark.parametrize("impl", ["native", "python"])
@pytest.mark.parametrize("conf_name", config_names)
def test_append_inserts_with_a_zero_watermark(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    impl: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """The largest delta value loaded is 0: the next append_inserts load still reads only rows
    above it (0 is a watermark, not a missing one)."""
    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1" if impl == "native" else "0")
    table = f"zero_wm_{impl}"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{table}",
        f"CREATE TABLE dbo.{table} (id int NOT NULL PRIMARY KEY, seq int NOT NULL, msg nvarchar(20))",
        f"INSERT INTO dbo.{table} (id, seq, msg) VALUES (1, 0, N'zero')",
    )
    cfg = WriteConfig(load_mode="append_inserts", delta_col="seq", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert sorted(r["seq"] for r in _delta_rows(dest)) == [0]
    _exec(connection, conf_name, f"INSERT INTO dbo.{table} (id, seq, msg) VALUES (2, 1, N'one')")
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert sorted(r["seq"] for r in _delta_rows(dest)) == [0, 1]


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_primary_keys_match_the_table_of_the_constraint(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Constraint names are unique per schema only: a primary key named like the primary key
    (or a unique constraint) of a table in another schema."""
    from sql2deltalake.metadata import get_primary_keys

    _skip_unless_mssql(connection)
    reader, _ = get_test_run_configs(connection, spark_session, "dbo/pk_name_a")[conf_name]
    _exec(
        connection,
        conf_name,
        "DROP TABLE IF EXISTS dbo.pk_name_a",
        "DROP TABLE IF EXISTS dbo.pk_name_c",
        "IF SCHEMA_ID('pk_other') IS NULL EXEC('CREATE SCHEMA pk_other')",
        "DROP TABLE IF EXISTS pk_other.pk_name_b",
        "DROP TABLE IF EXISTS pk_other.pk_name_d",
        "CREATE TABLE dbo.pk_name_a (id int NOT NULL CONSTRAINT PK_same PRIMARY KEY, val int)",
        "CREATE TABLE pk_other.pk_name_b (code int NOT NULL CONSTRAINT PK_same PRIMARY KEY, val int)",
        "CREATE TABLE dbo.pk_name_c (x int NOT NULL, y int NOT NULL CONSTRAINT UQ_shared UNIQUE)",
        "CREATE TABLE pk_other.pk_name_d (z int NOT NULL CONSTRAINT UQ_shared PRIMARY KEY)",
    )
    dialect = reader.source_dialect
    assert get_primary_keys(reader, ("dbo", "pk_name_a"), dialect=dialect) == ["id"]
    assert get_primary_keys(reader, ("pk_other", "pk_name_b"), dialect=dialect) == ["code"]
    assert get_primary_keys(reader, ("dbo", "pk_name_c"), dialect=dialect) == []
    assert get_primary_keys(reader, ("pk_other", "pk_name_d"), dialect=dialect) == ["z"]


def _lock_table(connection, conf_name, table):
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{table}",
        f"CREATE TABLE dbo.{table} (id int NOT NULL PRIMARY KEY, val int, rv rowversion)",
        f"INSERT INTO dbo.{table} (id, val) VALUES (1, 1), (2, 2)",
    )


@pytest.mark.order(30)
@pytest.mark.parametrize("impl", ["native", "python"])
@pytest.mark.parametrize("conf_name", config_names)
def test_load_lock(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    impl: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """meta/lock.txt: a second load fails while the lock is fresh, takes over a stale one, and a
    load leaves a lock alone that another load took over in the meantime."""
    import os
    from sql2deltalake import LoadAlreadyRunning, WriteConfig, write_db_to_delta
    from sql2deltalake import _native, db_to_delta
    from sql2deltalake.destination.file_system import FileSystemDestination

    _skip_unless_mssql(connection)
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1" if impl == "native" else "0")
    table = f"lock_{impl}"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    if not isinstance(dest, FileSystemDestination):
        pytest.skip("sets the modification time of the lock file")
    lock = dest / "meta" / "lock.txt"
    _lock_table(connection, conf_name, table)
    cfg = WriteConfig(primary_keys=["id"], dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert not lock.exists()

    lock.upload_str("another load")
    rows = len(_delta_rows(dest))
    _exec(connection, conf_name, f"UPDATE dbo.{table} SET val = 10 WHERE id = 1")
    with pytest.raises(LoadAlreadyRunning):
        write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert lock.read_str() == "another load"
    assert len(_delta_rows(dest)) == rows

    old = time.time() - 2 * 3600
    os.utime(str(lock), (old, old))
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert not lock.exists()
    assert len(_delta_rows(dest)) == rows + 1

    def take_over():
        lock.upload_str("taken over")

    if impl == "native":
        monkeypatch.setattr(
            _native, "fail_point", lambda name: take_over() if name == "load_updates" else None
        )
    else:
        orig = db_to_delta._load_updates_to_delta

        def load_updates(*args, **kwargs):
            take_over()
            return orig(*args, **kwargs)

        monkeypatch.setattr(db_to_delta, "_load_updates_to_delta", load_updates)
    _exec(connection, conf_name, f"UPDATE dbo.{table} SET val = 20 WHERE id = 2")
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert lock.read_str() == "taken over"


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_repeated_column_names_of_a_query_are_a_clear_error(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """A query with two columns of the same name (SELECT a.id, b.id): an error that names the
    column, not a KeyError or a row with one of the two values."""
    import sqlglot

    from sql2deltalake.reader.arrowsql_reader import ArrowSQLReader

    _skip_unless_mssql(connection)
    reader, _ = get_test_run_configs(connection, spark_session, "dbo/dup_names")[conf_name]
    if not isinstance(reader, ArrowSQLReader):
        pytest.skip("ArrowSQLReader")
    query = sqlglot.parse_one("SELECT 1 AS id, 2 AS id, 3 AS v", dialect="tsql")
    with pytest.raises(ValueError, match="column name id more than once"):
        reader.source_schema_limit_one(query)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="column name id more than once"):
        reader.source_sql_to_py("SELECT 1 AS id, 2 AS id, 3 AS v")


@pytest.mark.order(30)
@pytest.mark.parametrize("impl", ["native", "python"])
@pytest.mark.parametrize("conf_name", config_names)
def test_reordered_query_columns_keep_their_values(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    impl: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """The select list of a query changes order (SELECT id, a, b -> SELECT id, b, a, same types):
    every value stays in its own column, in the full load and in the delta load after it."""
    import sqlglot

    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1" if impl == "native" else "0")
    table = f"reorder_{impl}"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{table}",
        f"CREATE TABLE dbo.{table} (id int NOT NULL PRIMARY KEY, a int, b int, ver rowversion)",
        f"INSERT INTO dbo.{table} (id, a, b) VALUES (1, 10, 20), (2, 11, 21)",
    )

    def load(select: str, mode: str):
        query = sqlglot.parse_one(f"SELECT {select} FROM dbo.{table}", dialect="tsql")
        cfg = WriteConfig(
            primary_keys=["id"], delta_col="ver", load_mode=mode, dialect=reader.source_dialect  # type: ignore[arg-type]
        )
        write_db_to_delta(reader, query, dest, cfg)  # type: ignore[arg-type]

    def current() -> list:
        latest: dict = {}
        for r in sorted(_delta_rows(dest), key=lambda r: r["__timestamp"]):
            latest[r["id"]] = r
        return sorted((r["id"], r["a"], r["b"]) for r in latest.values() if not r["__is_deleted"])

    load("id, a, b, ver", "append")
    assert current() == [(1, 10, 20), (2, 11, 21)]
    time.sleep(1)
    load("id, b, a, ver", "force_full")
    assert current() == [(1, 10, 20), (2, 11, 21)]
    _exec(connection, conf_name, f"UPDATE dbo.{table} SET a = 12, b = 22 WHERE id = 2")
    load("id, b, a, ver", "append")
    assert current() == [(1, 10, 20), (2, 12, 22)]


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_unreadable_delta_log_is_an_error_not_a_missing_table(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """A _delta_log that cannot be listed (permissions, I/O) is an error of the load, not a
    table that is not there: the native path must not go on as if latest_pk_version were gone."""
    import os

    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    if os.name != "posix" or os.geteuid() == 0:
        pytest.skip("needs POSIX permissions and a non-root user")
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1")
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/unreadable_log")[conf_name]
    if not hasattr(dest, "path"):
        pytest.skip("local destination only")
    _rv_table(connection, conf_name, "unreadable_log")
    cfg = WriteConfig(primary_keys=["id"], delta_col="rv", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", "unreadable_log"), dest, cfg)
    log = dest.path / "delta_load" / "latest_pk_version" / "_delta_log"
    os.chmod(log, 0o300)  # no listing; files can still be written
    try:
        with pytest.raises(Exception) as ei:
            write_db_to_delta(reader, ("dbo", "unreadable_log"), dest, cfg)
    finally:
        os.chmod(log, 0o755)
    assert "cannot list" in _error_text(ei.value) and "_delta_log" in _error_text(ei.value)


def _current_rows(dest) -> list:
    """(id, val) of the latest non-deleted row per key of the target."""
    latest: dict = {}
    for r in sorted(_delta_rows(dest), key=lambda r: r["__timestamp"]):
        latest[r["id"]] = None if r["__is_deleted"] else (r["id"], r["val"])
    return sorted(v for v in latest.values() if v is not None)


@pytest.mark.order(30)
@pytest.mark.parametrize("point", ["load_updates", "write_delta2", "local_pk_count"])
@pytest.mark.parametrize("impl", ["native", "python"])
@pytest.mark.parametrize("conf_name", config_names)
def test_loads_after_failed_loads_converge(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    impl: str,
    point: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """A load that fails after it changed the load's state tables restores them; every later load
    must still read them (the restore must not write a Delta protocol the native reader rejects)
    and converge to the source, round after round."""
    from sql2deltalake import WriteConfig, _native, write_db_to_delta

    _skip_unless_mssql(connection)
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1" if impl == "native" else "0")
    table = f"fail_rounds_{impl}_{point}"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    _rv_table(connection, conf_name, table)
    cfg = WriteConfig(primary_keys=["id"], delta_col="rv", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)

    def fail(name):
        if name == point:
            raise RuntimeError(f"failed on purpose at {name}")

    for round in range(3):
        _exec(
            connection,
            conf_name,
            f"UPDATE dbo.{table} SET val = val + 10 WHERE id = 1",
            f"DELETE FROM dbo.{table} WHERE id = {10 + round - 1}" if round else "SELECT 1",
            f"INSERT INTO dbo.{table} (id, val) VALUES ({10 + round}, {round})",
        )
        with monkeypatch.context() as m:
            m.setattr(_native, "fail_point", fail)
            try:
                write_db_to_delta(reader, ("dbo", table), dest, cfg)
            except RuntimeError as e:
                assert "failed on purpose" in _error_text(e)
        write_db_to_delta(reader, ("dbo", table), dest, cfg)
        write_db_to_delta(reader, ("dbo", table), dest, cfg)
        expected = [(1, 1 + 10 * (round + 1)), (2, 2), (3, 3), (10 + round, round)]
        assert _current_rows(dest) == expected, f"round {round}"


@pytest.mark.order(30)
@pytest.mark.parametrize("conf_name", config_names)
def test_tables_restored_by_delta_rs_stay_readable(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """State tables an older version restored with delta-rs RESTORE carry a protocol action that
    delta-kernel rejects (empty feature lists below reader version 3): the native load reads
    them through the Python host instead of failing on every load."""
    from deltalake import DeltaTable, write_deltalake

    from sql2deltalake import WriteConfig, write_db_to_delta

    _skip_unless_mssql(connection)
    monkeypatch.setenv("SQL2DELTA_NATIVE", "1")
    table = "restored_by_delta_rs"
    reader, dest = get_test_run_configs(connection, spark_session, f"dbo/{table}")[conf_name]
    if not hasattr(dest, "path"):
        pytest.skip("local destination only")
    _rv_table(connection, conf_name, table)
    cfg = WriteConfig(primary_keys=["id"], delta_col="rv", dialect=reader.source_dialect)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    _exec(connection, conf_name, f"UPDATE dbo.{table} SET val = 20 WHERE id = 2")
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    for rel in ("latest_pk_version", "primary_keys_ts"):
        dt = DeltaTable(str(dest.path / "delta_load" / rel))
        version = dt.version()
        write_deltalake(dt, dt.to_pyarrow_table(), mode="overwrite")
        dt.update_incremental()
        dt.restore(version)  # a RESTORE commit with the protocol delta-kernel rejects
    _exec(
        connection,
        conf_name,
        f"UPDATE dbo.{table} SET val = 30 WHERE id = 3",
        f"DELETE FROM dbo.{table} WHERE id = 1",
    )
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    write_db_to_delta(reader, ("dbo", table), dest, cfg)
    assert _current_rows(dest) == [(2, 20), (3, 30)]
