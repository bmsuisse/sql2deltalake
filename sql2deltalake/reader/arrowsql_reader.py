import logging
from typing import TYPE_CHECKING, Literal, Mapping, Optional, Union

import sqlglot.expressions as ex

from sql2deltalake.destination.destination import Destination
from sql2deltalake.read_utils.delta_rs import _build_type, _get_type

from .local_reader import LocalDeltaReader
from .reader import check_unique_names

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
    from sql2deltalake.metadata import InformationSchemaColInfo


from sql2deltalake._conn_str import (  # noqa: F401  # re-exported
    _conn_str_segments,
    core_reads,
    strip_odbc_driver,
)


PROBE_TIMEOUT_ENV = "SQL2DELTA_PROBE_TIMEOUT"
_PROBE_TIMEOUT_DEFAULT = 10.0


def probe_timeout() -> float:
    """Seconds an optional probe may take, login included (env SQL2DELTA_PROBE_TIMEOUT, 10 by
    default)."""
    import os

    try:
        value = float(os.environ.get(PROBE_TIMEOUT_ENV, _PROBE_TIMEOUT_DEFAULT))
    except ValueError:
        return _PROBE_TIMEOUT_DEFAULT
    return value if value > 0 else _PROBE_TIMEOUT_DEFAULT


def _in_thread(fn, timeout: float):
    """`fn()` on a daemon thread, given up after `timeout` seconds (the thread is left to
    finish on its own): for reads through arrowsql's Python package, which has no deadline for
    a whole read."""
    import threading

    box: dict = {}

    def run():
        try:
            box["value"] = fn()
        except BaseException as e:  # handed to the caller
            box["error"] = e

    thread = threading.Thread(target=run, daemon=True, name="sql2deltalake-probe")
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError(f"did not finish within {timeout:g} s")
    if "error" in box:
        raise box["error"]
    return box["value"]


def _normalize_schema(schema: "pa.Schema") -> "pa.Schema":
    """Arrow types Delta can store: no large/time/uint8 types, UTC timestamps (the Rust stage in
    `normalize.rs`; time columns become `HH:MM:SS[.f...]` text)."""
    import pyarrow as pa
    from sql2deltalake import _core

    return pa.schema(_core.normalize(pa.RecordBatchReader.from_batches(schema, [])))


def _normalize_batch(
    batch: "pa.RecordBatch",
    target: "pa.Schema",
    time_scales: Optional[Mapping[int, int]] = None,
) -> "pa.RecordBatch":
    """`batch` cast to `target` (from `_normalize_schema`). `time_scales`: the scale of time columns
    by position, time(7) if missing."""
    import pyarrow as pa
    from sql2deltalake import _core

    out = pa.RecordBatchReader.from_stream(
        _core.normalize(batch, dict(time_scales or {}))
    ).read_next_batch()
    assert out.schema.equals(target)
    return out


class ArrowSQLSource:
    """The source side of `ArrowSQLReader`: reads SQL Server through
    [arrowsql](https://pypi.org/project/arrowsql/) (TDS in Rust, no driver needed) and normalizes the
    Arrow types to what Delta can store. Needs no local engine (DuckDB, delta-rs).

    `utf8_strings`: transfer nvarchar/nchar/ntext columns of the data reads as UTF-8 (about half the
    bytes for mostly-ASCII text). arrowsql decides per query with a describe and a TOP 256 sample
    of the query, so the server runs part of every data query twice. Off by default.

    `partitions`: split the data reads of the (native) load into this many range queries on an
    integer primary key, read on parallel connections. 1 (off) by default.
    """

    def __init__(
        self,
        connection_string: str,
        source_dialect: str = "tsql",
        *,
        utf8_strings: bool = False,
        partitions: int = 1,
    ) -> None:
        self.connection_string = strip_odbc_driver(connection_string)
        self.source_dialect = source_dialect
        self.utf8_strings = utf8_strings
        self.partitions = partitions
        # optional probes by SQL: their rows, or None if they failed (see optional_sql_to_py)
        self._optional: "dict[str, Optional[list[dict]]]" = {}

    def _source_arrow(
        self, sql: str, utf8_strings: bool = False, partition_on: Optional[str] = None
    ):
        """The raw Arrow stream of `sql` (arrowsql's types; the Rust load normalizes them). With
        `partitions` > 1 and an integer `partition_on` column, the read is split into that many
        range queries on parallel connections."""
        kwargs = {}
        if partition_on is not None and self.partitions > 1:
            kwargs = {"partition_on": partition_on, "partitions": self.partitions}
        if core_reads(self.connection_string):
            from sql2deltalake import _core

            return _core.read_sql(
                self.connection_string, sql, utf8_strings=utf8_strings, **kwargs
            )
        import arrowsql

        return arrowsql.read_arrow_stream(  # pyright: ignore[reportPrivateImportUsage]
            self.connection_string, sql, utf8_strings=utf8_strings, **kwargs
        )

    @property
    def supports_proc_exec(self) -> bool:
        return True

    def _source_reader(
        self, sql: str, utf8_strings: bool = False, partition_on: Optional[str] = None
    ) -> "pa.RecordBatchReader":
        import pyarrow as pa
        import pyarrow.types as pat
        from sql2deltalake import _core

        reader = pa.RecordBatchReader.from_stream(
            self._source_arrow(sql, utf8_strings, partition_on)
        )
        time_scales = (
            self._time_scales(sql)
            if any(pat.is_time(f.type) for f in reader.schema)
            else None
        )
        return pa.RecordBatchReader.from_stream(_core.normalize(reader, time_scales))

    def _time_scales(self, sql: str) -> Optional[Mapping[int, int]]:
        """Scale of the time columns by position. Arrow's time64[ns] does not carry it,
        so ask the server for the result set metadata (no execution of the query); None if
        that fails (see `optional_sql_to_py`)."""
        from ..query import sql_quote_value

        rows = self.optional_sql_to_py(
            "SELECT d.column_ordinal, d.scale FROM sys.dm_exec_describe_first_result_set(N"
            + sql_quote_value(sql)
            + ", NULL, 0) AS d WHERE d.is_hidden = 0 AND d.system_type_id = 41",
            "the scale of time columns, using 7",
        )
        if rows is None:
            return None
        return {r["column_ordinal"] - 1: r["scale"] for r in rows}

    def optional_sql_to_py(self, sql: str, what: str) -> Optional["list[dict]"]:
        """The rows of `sql`, a probe the load can do without (`what`: what it reads, and the
        fallback). It runs at most once per source object (the rows, or the failure, are kept),
        within `probe_timeout()` seconds, login included, and is never retried: on any error or
        timeout it logs one warning and returns None."""
        if sql in self._optional:
            return self._optional[sql]
        try:
            rows = self._bounded_sql_to_py(sql, probe_timeout())
        except Exception as e:
            logger.warning(f"Could not read {what}: {e}")
            rows = None
        self._optional[sql] = rows
        return rows

    def _bounded_sql_to_py(self, sql: str, timeout: float) -> "list[dict]":
        import pyarrow as pa
        from sql2deltalake import _core

        if core_reads(self.connection_string):
            stream = _core.read_sql(self.connection_string, sql, timeout=timeout)
            reader = pa.RecordBatchReader.from_stream(_core.normalize(stream, None))
            return reader.read_all().to_pylist()
        return _in_thread(lambda: self.source_sql_to_py(sql), timeout)

    def source_schema_limit_one(
        self, sql: ex.Query
    ) -> "list[InformationSchemaColInfo]":
        from ..metadata import InformationSchemaColInfo

        sc = self._source_reader(sql.limit(0).sql(self.source_dialect)).schema
        check_unique_names(sc.names)
        return [
            InformationSchemaColInfo(
                column_name=f.name,
                data_type_str=str(f.type),
                data_type=_build_type(_get_type(f.type)),
                is_nullable=f.nullable,
            )
            for f in sc
        ]

    def source_sql_to_py(self, sql: Union[str, ex.Query]) -> list[dict]:
        if isinstance(sql, ex.Query):
            sql = sql.sql(self.source_dialect)
        result: list[dict] = []
        reader = self._source_reader(sql)
        try:
            check_unique_names(reader.schema.names)
        except ValueError:
            reader.close()
            raise
        for batch in reader:
            result.extend(batch.to_pylist())
        return result


class ArrowSQLReader(ArrowSQLSource, LocalDeltaReader):
    """`ArrowSQLSource` plus the local half: Delta tables are written with delta-rs and queried
    locally with DuckDB. The native load (`_core.run_load`) runs on this reader."""

    def __init__(
        self,
        connection_string: str,
        local_db: str = ":memory:",
        source_dialect: str = "tsql",
        *,
        utf8_strings: bool = False,
        partitions: int = 1,
    ) -> None:
        LocalDeltaReader.__init__(self, local_db=local_db, source_dialect=source_dialect)
        ArrowSQLSource.__init__(
            self,
            connection_string,
            source_dialect,
            utf8_strings=utf8_strings,
            partitions=partitions,
        )

    def source_write_sql_to_delta(
        self,
        sql: str,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
    ):
        reader = self._source_reader(sql, utf8_strings=self.utf8_strings)
        self._write_reader_to_delta(
            reader, delta_path, mode, allow_schema_drift, all_nullable=True
        )
