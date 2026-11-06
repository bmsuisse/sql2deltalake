"""Slices of a source query for readers that read one slice per task (a Spark data source)."""

import os

import pyarrow as pa
import pytest


def test_partitions_cover_all_rows_once(connection):
    from odbc2deltalake.reader.arrowsql_reader import ArrowSQLReader

    if connection.source_server != "mssql":
        pytest.skip("needs a SQL Server read through arrowsql")
    if (os.getenv("ODBCLAKE_TEST_CONFIGURATION") or "").lower() not in ("arrowsql", "local"):
        pytest.skip("needs a SQL Server read through arrowsql")
    reader = ArrowSQLReader(connection.conn_str[next(iter(connection.conn_str))])
    sql = "SELECT [User - iD] AS id, FirstName, LastName FROM dbo.[user]"
    parts = reader.partitions_of(sql, "id", 3)
    assert len(parts) == 3
    ids = []
    for p in parts:
        r = pa.RecordBatchReader.from_stream(reader.read_partition(sql, "id", p))
        assert r.schema.field("LastName").type == pa.string()  # nvarchar(max): no large_string
        ids += r.read_all().column("id").to_pylist()
    expected = [r["id"] for r in reader.source_sql_to_py(sql)]
    assert sorted(ids) == sorted(expected)
