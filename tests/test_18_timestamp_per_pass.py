import time
from typing import TYPE_CHECKING
import duckdb
import pytest
from deltalake2db import duckdb_create_view_for_delta
from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def _exec(connection: "DB_Connection", conf_name: str, *sqls: str):
    with connection.new_connection(conf_name) as nc:
        for sql in sqls:
            with nc.cursor() as cursor:
                cursor.execute(sql)


def _create_table(
    connection: "DB_Connection", conf_name: str, name: str, *, modified_col=False
):
    is_pg = connection.source_server == "postgres"
    if modified_col:
        extra = ", modified timestamp" if is_pg else ", modified datetime2(6)"
    else:
        extra = "" if is_pg else ", rv rowversion"
    _exec(
        connection,
        conf_name,
        f"DROP TABLE IF EXISTS dbo.{name}",
        f"CREATE TABLE dbo.{name} (id int primary key, val varchar(50){extra})",
        (
            f"INSERT INTO dbo.{name} (id, val, modified) VALUES (1, 'a', '2020-01-01'), (2, 'b', '2020-01-02'), (3, 'c', '2020-01-03'), (4, 'd', '2020-01-04')"
            if modified_col
            else f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
        ),
    )


def _rows(dest, conf_name: str, table: str = "delta"):
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / table).as_delta_table()
            if table == "delta"
            else (dest / "delta_load" / table).as_delta_table(),
            "v_rows",
            use_delta_ext=conf_name == "spark",
        )
        cur = con.execute("SELECT * FROM v_rows")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]


def _versions_of(dest, conf_name: str, pk: int):
    rows = [
        r
        for r in _rows(dest, conf_name)
        if r["id"] == pk and not r["__is_full_load"] and not r["__is_deleted"]
    ]
    return sorted(((r["__timestamp"], r["val"]) for r in rows), key=lambda x: x[0])


@pytest.mark.order(23)
@pytest.mark.parametrize("conf_name", config_names)
def test_simple_delta_check_second_pass_gets_later_timestamp(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """simple_delta_check falls back to a normal delta load in the same run. A row that changes
    between the two passes is appended twice and the second version must order after the first."""
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake import _native

    reader, dest = get_test_run_configs(
        connection, spark_session, "dbo/ts_simple_check"
    )[conf_name]
    _create_table(connection, conf_name, "ts_simple_check")
    cfg = WriteConfig(
        load_mode="simple_delta_check",
        primary_keys=["id"],
        dialect=reader.source_dialect,
    )
    write_db_to_delta(reader, ("dbo", "ts_simple_check"), dest, cfg)

    _exec(
        connection,
        conf_name,
        "DELETE FROM dbo.ts_simple_check WHERE id = 4",  # count mismatch -> second pass
        "UPDATE dbo.ts_simple_check SET val = 'a1' WHERE id = 1",
    )
    time.sleep(1)
    calls = []

    def _count_and_change(name):
        if name != "local_pk_count":
            return None
        if not calls:
            _exec(
                connection,
                conf_name,
                "UPDATE dbo.ts_simple_check SET val = 'a2' WHERE id = 1",
            )
        calls.append(1)

    monkeypatch.setattr(_native, "fail_point", _count_and_change)
    write_db_to_delta(reader, ("dbo", "ts_simple_check"), dest, cfg)
    assert len(calls) == 2, "expected the simple pass and the fallback pass"
    versions = _versions_of(dest, conf_name, 1)
    assert [v for _, v in versions] == ["a1", "a2"]
    assert versions[0][0] < versions[1][0]


@pytest.mark.order(23)
@pytest.mark.parametrize("conf_name", config_names)
def test_strange_updates_reload_gets_later_timestamp(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """With no_complex_entries_load (or > 1000 strange updates) the rows are read a second time by
    MIN(timestamp). A row that changed between the two reads must order after its first version."""
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake import _native

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/ts_strange")[
        conf_name
    ]
    _create_table(connection, conf_name, "ts_strange", modified_col=True)
    cfg = WriteConfig(
        load_mode="append",
        primary_keys=["id"],
        delta_col="modified",
        no_complex_entries_load=True,
        dialect=reader.source_dialect,
    )
    write_db_to_delta(reader, ("dbo", "ts_strange"), dest, cfg)

    _exec(
        connection,
        conf_name,
        "UPDATE dbo.ts_strange SET val = 'b1', modified = '2021-01-01' WHERE id = 1",
        "UPDATE dbo.ts_strange SET val = 'strange', modified = '2019-06-01' WHERE id = 2",
    )
    time.sleep(1)
    calls = []

    def _write_delta2_and_change(name):
        if name != "write_delta2":
            return None
        if not calls:
            _exec(
                connection,
                conf_name,
                "UPDATE dbo.ts_strange SET val = 'b2', modified = '2021-01-02' WHERE id = 1",
            )
        calls.append(1)

    monkeypatch.setattr(_native, "fail_point", _write_delta2_and_change)
    write_db_to_delta(reader, ("dbo", "ts_strange"), dest, cfg)
    assert calls, "expected the strange-updates branch"
    versions = _versions_of(dest, conf_name, 1)
    assert [v for _, v in versions] == ["b1", "b2"]
    assert versions[0][0] < versions[1][0]
    # id 2 has the smallest timestamp of the strange updates, the re-read must include it
    assert [v for _, v in _versions_of(dest, conf_name, 2)] == ["strange"]


@pytest.mark.order(23)
@pytest.mark.parametrize("conf_name", config_names)
def test_full_load_fallback_gets_later_timestamp(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """If the additional-updates view cannot be built and the PKs cannot be restored, the delta load
    falls back to a full load after delta_1 was already appended. The full load must order after delta_1."""
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake import _native

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/ts_fallback")[
        conf_name
    ]
    _create_table(connection, conf_name, "ts_fallback")
    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "ts_fallback"), dest, cfg)
    _exec(connection, conf_name, "UPDATE dbo.ts_fallback SET val = 'c1' WHERE id = 1")
    time.sleep(1)

    failed = []

    def _fail_and_no_restore(name):
        if name == "additional_updates" and not failed:
            failed.append(1)
            raise RuntimeError("simulated: cannot build additional_updates")
        if name == "restore_last_pk":
            _exec(
                connection,
                conf_name,
                "UPDATE dbo.ts_fallback SET val = 'c2' WHERE id = 1",
            )
            return False
        return None

    with monkeypatch.context() as m:
        m.setattr(_native, "fail_point", _fail_and_no_restore)
        write_db_to_delta(reader, ("dbo", "ts_fallback"), dest, cfg)
    assert failed

    rows = _rows(dest, conf_name)
    delta_1_ts = [
        r["__timestamp"] for r in rows if r["val"] == "c1" and not r["__is_full_load"]
    ]
    assert len(delta_1_ts) == 1
    fallback = [r for r in rows if r["__is_full_load"] and r["val"] == "c2"]
    assert len(fallback) == 1
    assert fallback[0]["__timestamp"] > delta_1_ts[0]


@pytest.mark.order(23)
@pytest.mark.parametrize("conf_name", config_names)
def test_restore_pk_ignores_non_full_rows_of_last_full_load(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """restore_last_pk takes the last full load plus newer changes. A non-full row that has the same
    __timestamp as the last full load (written by older versions) must not end up as a second row per PK."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from deltalake import DeltaTable, write_deltalake
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake.write_init import make_writer
    from sql2deltalake.write_utils.restore_pk import restore_last_pk

    if conf_name == "spark":
        pytest.skip("writes the delta table with delta-rs")
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/ts_restore")[
        conf_name
    ]
    _create_table(connection, conf_name, "ts_restore")
    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "ts_restore"), dest, cfg)
    delta_col = "rv" if connection.source_server == "mssql" else "xmin"

    path, opts = (dest / "delta").as_path_options("object_store")
    tbl = DeltaTable(path, storage_options=opts).to_pyarrow_table()
    row = tbl.filter(pc.equal(tbl["id"], 2))
    full_rv = row[delta_col][0].as_py()
    for name, value in [
        ("val", pa.array(["newer"], row.schema.field("val").type)),
        (
            delta_col,
            pc.add(row[delta_col], pa.scalar(1000, row.schema.field(delta_col).type)),
        ),
        ("__is_full_load", pa.array([False], row.schema.field("__is_full_load").type)),
    ]:
        row = row.set_column(row.schema.get_field_index(name), name, value)
    write_deltalake(path, row, mode="append", storage_options=opts)

    infos = make_writer(reader, ("dbo", "ts_restore"), dest, cfg)
    assert restore_last_pk(infos)
    lpk = _rows(dest, conf_name, "latest_pk_version")
    assert sorted(r["id"] for r in lpk) == [1, 2, 3, 4]
    # the full load is the later read of the two, see the full-load fallback in _handle_additional_updates
    assert next(r[delta_col] for r in lpk if r["id"] == 2) == full_rv


@pytest.mark.parametrize("conf_name", config_names)
def test_restore_pk_breaks_timestamp_ties_by_delta_value(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Two versions of a key after the last full load with the same __timestamp (tables written
    before every pass had its own timestamp): restore_last_pk keeps the higher delta value."""
    import datetime

    import pyarrow as pa
    import pyarrow.compute as pc
    from deltalake import DeltaTable, write_deltalake
    from sql2deltalake import WriteConfig, write_db_to_delta
    from sql2deltalake.write_init import make_writer
    from sql2deltalake.write_utils.restore_pk import restore_last_pk

    if conf_name == "spark":
        pytest.skip("writes the delta table with delta-rs")
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/ts_tie")[
        conf_name
    ]
    _create_table(connection, conf_name, "ts_tie")
    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(reader, ("dbo", "ts_tie"), dest, cfg)
    delta_col = "rv" if connection.source_server == "mssql" else "xmin"

    path, opts = (dest / "delta").as_path_options("object_store")
    tbl = DeltaTable(path, storage_options=opts).to_pyarrow_table()
    ts_type = tbl.schema.field("__timestamp").type
    later = pc.add(tbl["__timestamp"][0], pa.scalar(datetime.timedelta(minutes=1)))
    full_rv = {}
    rows = []
    # id 2: the lower delta value written first, id 3: the higher one first
    for pk, bumps in [(2, [(10, "lower"), (1000, "highest")]), (3, [(1000, "highest"), (10, "lower")])]:
        row = tbl.filter(pc.equal(tbl["id"], pk))
        full_rv[pk] = row[delta_col][0].as_py()
        for bump, val in bumps:
            r = row
            for name, value in [
                ("val", pa.array([val], r.schema.field("val").type)),
                (delta_col, pa.array([full_rv[pk] + bump], r.schema.field(delta_col).type)),
                ("__timestamp", pa.array([later.as_py()], ts_type)),
                ("__is_full_load", pa.array([False], r.schema.field("__is_full_load").type)),
            ]:
                r = r.set_column(r.schema.get_field_index(name), name, value)
            rows.append(r)
    write_deltalake(path, pa.concat_tables(rows), mode="append", storage_options=opts)

    infos = make_writer(reader, ("dbo", "ts_tie"), dest, cfg)
    assert restore_last_pk(infos)
    lpk = _rows(dest, conf_name, "latest_pk_version")
    assert sorted(r["id"] for r in lpk) == [1, 2, 3, 4]
    for pk in (2, 3):
        assert next(r[delta_col] for r in lpk if r["id"] == pk) == full_rv[pk] + 1000
