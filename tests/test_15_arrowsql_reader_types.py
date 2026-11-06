import datetime as dt
import pyarrow as pa
import pytest

from odbc2deltalake.reader.arrowsql_reader import (
    _normalize_batch,
    _normalize_schema,
    strip_odbc_driver,
)


@pytest.mark.parametrize(
    "conn_str,expected",
    [
        (
            "DRIVER=ODBC Driver 18 for SQL Server;Server=a;Database=b",
            "Server=a;Database=b",
        ),
        ("Server=a;Driver={ODBC Driver 18 for SQL Server};UID=x", "Server=a;UID=x"),
        ("Server=a;Database=b", "Server=a;Database=b"),
        (" Driver = x ;Server=a;", "Server=a;"),
        # a braced value can contain ;Driver= and must stay intact
        ("Server=a;Pwd={x;Driver=y};UID=u", "Server=a;Pwd={x;Driver=y};UID=u"),
        ("PWD={secret;Driver=x;part};Server=a", "PWD={secret;Driver=x;part};Server=a"),
        (
            "Server=a;Pwd={a}};Driver=b};Driver={ODBC Driver 18};UID=u",
            "Server=a;Pwd={a}};Driver=b};UID=u",
        ),
    ],
)
def test_strip_odbc_driver(conn_str: str, expected: str):
    assert strip_odbc_driver(conn_str) == expected


def test_arrowsql_types_normalized_for_delta():
    schema = pa.schema(
        [
            ("s", pa.large_string()),
            ("b", pa.large_binary()),
            ("tinyint", pa.uint8()),
            ("t", pa.time64("ns")),
            ("dto", pa.timestamp("us", tz="+00:00")),
            ("ts", pa.timestamp("us")),
            ("d", pa.decimal128(18, 2)),
        ]
    )
    target = _normalize_schema(schema)
    assert target.types == [
        pa.string(),
        pa.binary(),
        pa.int16(),
        pa.string(),
        pa.timestamp("us", tz="UTC"),
        pa.timestamp("us"),
        pa.decimal128(18, 2),
    ]
    batch = pa.RecordBatch.from_pydict(
        {
            "s": ["a", None],
            "b": [b"\x01", None],
            "tinyint": [255, None],
            "t": [dt.time(10, 0, 1, 123000), None],
            "dto": [dt.datetime(2020, 1, 1, 9), None],
            "ts": [dt.datetime(2020, 1, 1), None],
            "d": [None, None],
        },
        schema=schema,
    )
    out = _normalize_batch(batch, target)
    assert out.schema.equals(target)
    assert out.column("t").to_pylist() == ["10:00:01.1230000", None]
    assert out.column("tinyint").to_pylist() == [255, None]


def test_time_keeps_all_digits_and_uses_column_scale():
    """arrowsql delivers time(n) as time64[ns]; SQL Server renders it with n fractional digits."""
    t = pa.array(
        [(10 * 3600 + 1) * 10**9 + 123456700, None, 23 * 3600 * 10**9 + 5 * 10**8],
        pa.time64("ns"),
    )
    batch = pa.RecordBatch.from_arrays([t, t, t], names=["t7", "t3", "t0"])
    target = _normalize_schema(batch.schema)
    out = _normalize_batch(batch, target, {0: 7, 1: 3, 2: 0})
    assert out.column("t7").to_pylist() == [
        "10:00:01.1234567",
        None,
        "23:00:00.5000000",
    ]
    assert out.column("t3").to_pylist() == ["10:00:01.123", None, "23:00:00.500"]
    assert out.column("t0").to_pylist() == ["10:00:01", None, "23:00:00"]
    # without a known scale: time(7), the default of the time type
    assert _normalize_batch(batch, target).column("t0").to_pylist()[0] == (
        "10:00:01.1234567"
    )


def test_time32_columns_are_formatted():
    """time32[s] and time32[ms] (not what arrowsql delivers today) format like time64."""
    secs = pa.array([3661, None], pa.time32("s"))
    millis = pa.array([3661 * 1000 + 250, None], pa.time32("ms"))
    batch = pa.RecordBatch.from_arrays([secs, millis], names=["s", "ms"])
    target = _normalize_schema(batch.schema)
    out = _normalize_batch(batch, target, {0: 0, 1: 3})
    assert out.column("s").to_pylist() == ["01:01:01", None]
    assert out.column("ms").to_pylist() == ["01:01:01.250", None]


def test_time_from_server_as_text(connection):
    import os
    from odbc2deltalake.reader.arrowsql_reader import ArrowSQLReader

    if (os.getenv("ODBCLAKE_TEST_CONFIGURATION") or "").lower() != "arrowsql":
        pytest.skip("needs the arrowsql test configuration")
    reader = ArrowSQLReader(connection.conn_str["arrowsql"])
    rows = reader.source_sql_to_py(
        "SELECT CAST('10:00:01.1234567' AS time(7)) AS t7, CAST('10:00:01.123' AS time(3)) AS t3, "
        "CAST('10:00:01' AS time(0)) AS t0, CAST(NULL AS time(2)) AS tn"
    )
    assert rows == [
        {"t7": "10:00:01.1234567", "t3": "10:00:01.123", "t0": "10:00:01", "tn": None}
    ]


def test_utf8_strings_probe_is_opt_in(monkeypatch, tmp_path):
    """arrowsql's utf8_strings rewrite runs a describe and a TOP 256 sample of the query before the
    real read. It must not run by default, and never for small metadata/control queries."""
    arrowsql = pytest.importorskip("arrowsql")
    from odbc2deltalake.destination.file_system import FileSystemDestination
    from odbc2deltalake.reader.arrowsql_reader import ArrowSQLReader

    calls = []

    def _fake_stream(connection_str, query, **kwargs):
        calls.append(kwargs.get("utf8_strings", True))
        schema = pa.schema([("s", pa.large_string())])
        return pa.RecordBatchReader.from_batches(
            schema, [pa.RecordBatch.from_pydict({"s": ["x"]}, schema=schema)]
        )

    monkeypatch.setattr(arrowsql, "read_arrow_stream", _fake_stream)
    monkeypatch.setenv("SQL2DELTA_ARROWSQL_CORE", "0")  # through arrowsql's Python side

    default = ArrowSQLReader("Server=a;Database=b")
    default.source_sql_to_py("SELECT N'x' AS s")
    default.source_write_sql_to_delta(
        "SELECT N'x' AS s",
        FileSystemDestination(tmp_path / "d1"),
        mode="overwrite",
        allow_schema_drift=True,
    )
    assert calls == [False, False]

    calls.clear()
    opted = ArrowSQLReader("Server=a;Database=b", utf8_strings=True)
    opted.source_sql_to_py("SELECT N'x' AS s")
    opted.source_write_sql_to_delta(
        "SELECT N'x' AS s",
        FileSystemDestination(tmp_path / "d2"),
        mode="overwrite",
        allow_schema_drift=True,
    )
    assert calls == [False, True]


def test_core_reads_unless_entra_login(monkeypatch):
    from odbc2deltalake import _core
    from odbc2deltalake.reader.arrowsql_reader import core_reads

    if not hasattr(_core, "read_sql"):
        pytest.skip("built without the arrowsql-core feature")
    assert core_reads("Server=a;Database=b;UID=u;PWD={x;Authentication=y}")
    assert not core_reads("Server=a;Authentication=ActiveDirectoryDefault")
    monkeypatch.setenv("SQL2DELTA_ARROWSQL_CORE", "0")
    assert not core_reads("Server=a;Database=b")
