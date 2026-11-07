"""Unit tests without a database for small fixes."""

from typing import Any, List, Tuple


class _RecordingSource:
    """Collects what DeltaStorageBackend.flush writes instead of writing Delta."""

    def __init__(self) -> None:
        self.writes: List[Tuple[Any, List[str]]] = []

    def local_pylist_to_delta(self, rows, path, mode, dummy_record=None):
        self.writes.append((path, [r["message"] for r in rows]))


def test_loggers_keep_their_own_pending_messages():
    from sql2deltalake.delta_logger import DeltaLogger

    source = _RecordingSource()
    a = DeltaLogger("log_a", source)  # type: ignore[arg-type]
    b = DeltaLogger("log_b", source)  # type: ignore[arg-type]
    for i in range(3):
        a.info(f"a{i}")
        b.info(f"b{i}")
    a.storage_backend.flush()  # type: ignore[union-attr]
    assert source.writes == [("log_a", ["a0", "a1", "a2"])]
    b.storage_backend.flush()  # type: ignore[union-attr]
    assert source.writes[1] == ("log_b", ["b0", "b1", "b2"])


def test_sql_quote_value_of_bool_is_a_bit():
    from sql2deltalake.query import sql_quote_value

    assert sql_quote_value(True) == "1"
    assert sql_quote_value(False) == "0"
    assert sql_quote_value(5) == "5"


def _lock(tmp_path):
    from sql2deltalake.destination.file_system import FileSystemDestination

    (tmp_path / "meta").mkdir()
    return FileSystemDestination(tmp_path / "meta" / "lock.txt")


def test_a_fresh_lock_of_another_load_is_not_taken():
    import tempfile
    from pathlib import Path

    import pytest

    from sql2deltalake import LoadAlreadyRunning
    from sql2deltalake.load_lock import acquire

    with tempfile.TemporaryDirectory() as d:
        lock = _lock(Path(d))
        lock.upload_str("another load")
        with pytest.raises(LoadAlreadyRunning, match="another load"):
            acquire(lock)
        assert lock.read_str() == "another load"


def test_a_stale_lock_is_taken_over():
    import os
    import tempfile
    import time
    from pathlib import Path

    from sql2deltalake.load_lock import acquire

    with tempfile.TemporaryDirectory() as d:
        lock = _lock(Path(d))
        lock.upload_str("a load that died")
        old = time.time() - 2 * 3600
        os.utime(lock.path, (old, old))
        token = acquire(lock)
        assert lock.read_str() == token != "a load that died"


def test_only_the_owner_releases_the_lock():
    import tempfile
    from pathlib import Path

    from sql2deltalake.load_lock import acquire, release

    with tempfile.TemporaryDirectory() as d:
        lock = _lock(Path(d))
        token = acquire(lock)
        lock.upload_str("taken over by another load")
        release(lock, token)
        assert lock.read_str() == "taken over by another load"
        lock.remove()
        token = acquire(lock)
        release(lock, token)
        assert not lock.exists()
        release(lock, token)  # already gone


def test_databricks_exists_raises_errors_other_than_not_found():
    from unittest.mock import MagicMock

    import pytest

    from sql2deltalake.destination.databricks import DatabricksDestination

    dbutils = MagicMock()
    dest = DatabricksDestination(dbutils, "raw", "x/meta/lock.txt", "acc")
    dbutils.fs.ls.side_effect = Exception(
        "java.io.FileNotFoundException: Operation failed: The specified path does not exist."
    )
    assert dest.exists() is False
    dbutils.fs.ls.side_effect = Exception("Operation failed: 403 This request is not authorized")
    with pytest.raises(Exception, match="403"):
        dest.exists()
    dbutils.fs.put.side_effect = Exception(
        "org.apache.hadoop.fs.FileAlreadyExistsException: Operation failed: The specified path already exists."
    )
    assert dest.create_exclusive("token") is False
    dbutils.fs.put.side_effect = None
    assert dest.create_exclusive("token") is True
    dbutils.fs.put.assert_called_with(dest.to_az_path(), "token", overwrite=False)


def test_repeated_column_names_are_a_clear_error():
    import pytest
    import sqlglot

    from sql2deltalake.reader.arrowsql_reader import ArrowSQLReader

    reader = ArrowSQLReader("server=localhost;database=unused", ":memory:")
    assert reader.local_execute_sql_to_py(sqlglot.parse_one("SELECT 1 AS id, 2 AS v")) == [
        {"id": 1, "v": 2}
    ]
    with pytest.raises(ValueError, match="column name id more than once"):
        reader.local_execute_sql_to_py(sqlglot.parse_one("SELECT 1 AS id, 2 AS id"))
