import logging
from typing import TYPE_CHECKING, Literal, Mapping, Optional, Union

import sqlglot.expressions as ex

from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.read_utils.delta_rs import _build_type, _get_type

from .local_reader import LocalDeltaReader

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
    from odbc2deltalake.metadata import InformationSchemaColInfo


def _conn_str_segments(connection_string: str) -> "list[str]":
    """Splits at the `;` between `key=value` pairs, not at one inside a `{...}` value (where `}}` is a `}`)."""
    cs = connection_string
    n = len(cs)
    segments: "list[str]" = []
    i = 0
    while True:
        eq = cs.find("=", i)
        semi = cs.find(";", i)
        j = i if eq == -1 or (semi != -1 and semi < eq) else eq + 1
        while j < n and cs[j] in " \t":
            j += 1
        if j > i and j < n and cs[j] == "{":
            j += 1
            while j < n:
                if cs[j] == "}":
                    if cs[j + 1 : j + 2] == "}":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
        end = cs.find(";", j)
        if end == -1:
            segments.append(cs[i:])
            return segments
        segments.append(cs[i:end])
        i = end + 1


def strip_odbc_driver(connection_string: str) -> str:
    """arrowsql speaks TDS itself: a `Driver=` keyword left over from an ODBC connection string is dropped."""
    return ";".join(
        s
        for s in _conn_str_segments(connection_string)
        if s.split("=", 1)[0].strip().lower() != "driver" or "=" not in s
    )


def _normalize_schema(schema: "pa.Schema") -> "pa.Schema":
    """Arrow types Delta can store: no large/time/uint8 types, UTC timestamps (the Rust stage in
    `normalize.rs`; time columns become `HH:MM:SS[.f...]` text)."""
    import pyarrow as pa
    from odbc2deltalake import _core

    return pa.schema(_core.normalize(pa.RecordBatchReader.from_batches(schema, [])))


def _normalize_batch(
    batch: "pa.RecordBatch",
    target: "pa.Schema",
    time_scales: Optional[Mapping[int, int]] = None,
) -> "pa.RecordBatch":
    """`batch` cast to `target` (from `_normalize_schema`). `time_scales`: the scale of time columns
    by position, time(7) if missing."""
    import pyarrow as pa
    from odbc2deltalake import _core

    out = pa.RecordBatchReader.from_stream(
        _core.normalize(batch, dict(time_scales or {}))
    ).read_next_batch()
    assert out.schema.equals(target)
    return out


class ArrowSQLReader(LocalDeltaReader):
    """Reads SQL Server through [arrowsql](https://pypi.org/project/arrowsql/) (TDS in Rust, no driver
    needed); Delta tables are written with delta-rs and queried locally with DuckDB.

    `utf8_strings`: transfer nvarchar/nchar/ntext columns of the data reads as UTF-8 (about half the
    bytes for mostly-ASCII text). arrowsql decides per query with a describe and a TOP 256 sample
    of the query, so the server runs part of every data query twice. Off by default.

    `partitions`: split the data reads of the (native) load into this many range queries on an
    integer primary key, read on parallel connections. 1 (off) by default.
    """

    def __init__(
        self,
        connection_string: str,
        local_db: str = ":memory:",
        source_dialect: str = "tsql",
        *,
        utf8_strings: bool = False,
        partitions: int = 1,
    ) -> None:
        super().__init__(local_db=local_db, source_dialect=source_dialect)
        self.connection_string = strip_odbc_driver(connection_string)
        self.utf8_strings = utf8_strings
        self.partitions = partitions

    def _source_arrow(
        self, sql: str, utf8_strings: bool = False, partition_on: Optional[str] = None
    ):
        """The raw Arrow stream of `sql` (arrowsql's types; the Rust load normalizes them). With
        `partitions` > 1 and an integer `partition_on` column, the read is split into that many
        range queries on parallel connections."""
        import arrowsql

        kwargs = {}
        if partition_on is not None and self.partitions > 1:
            kwargs = {"partition_on": partition_on, "partitions": self.partitions}
        return arrowsql.read_arrow_stream(  # pyright: ignore[reportPrivateImportUsage]
            self.connection_string, sql, utf8_strings=utf8_strings, **kwargs
        )

    def partitions_of(
        self, sql: str, column: str, n: int
    ) -> "list[tuple[Optional[int], Optional[int]]]":
        """`n` slices of `sql` by ranges of its integer `column` (MIN/MAX read from the source), for
        readers that read one slice per task, such as a Spark data source. See `read_partition`."""
        from odbc2deltalake import _core
        from ..query import sql_quote_name

        q = sql_quote_name(column, mode="tsql")
        row = self.source_sql_to_py(
            f"SELECT MIN(_p.{q}) AS lo, MAX(_p.{q}) AS hi FROM ({sql}) AS _p"
        )[0]
        if row["lo"] is None:
            return [(None, None)]
        return _core.partition_bounds(int(row["lo"]), int(row["hi"]), n)

    def read_partition(
        self,
        sql: str,
        column: str,
        bounds: "tuple[Optional[int], Optional[int]]",
    ):
        """One slice of `sql` (from `partitions_of`) as an Arrow C stream (`__arrow_c_stream__`)
        with types Delta can store; batches go from arrowsql through the Rust cast stage without a
        copy. A Spark Python data source can `yield from pyarrow.RecordBatchReader.from_stream(...)`."""
        from odbc2deltalake import _core

        import pyarrow as pa
        import pyarrow.types as pat

        lower, upper = bounds
        slice_sql = _core.partition_sql(self.source_dialect, sql, column, lower, upper, True)
        reader = pa.RecordBatchReader.from_stream(
            self._source_arrow(slice_sql, self.utf8_strings)
        )
        time_scales = (
            self._time_scales(slice_sql)
            if any(pat.is_time(f.type) for f in reader.schema)
            else None
        )
        return _core.normalize(reader, time_scales)

    def _source_reader(
        self, sql: str, utf8_strings: bool = False
    ) -> "pa.RecordBatchReader":
        import pyarrow as pa
        import pyarrow.types as pat
        from odbc2deltalake import _core

        reader = pa.RecordBatchReader.from_stream(self._source_arrow(sql, utf8_strings))
        time_scales = (
            self._time_scales(sql)
            if any(pat.is_time(f.type) for f in reader.schema)
            else None
        )
        return pa.RecordBatchReader.from_stream(_core.normalize(reader, time_scales))

    def _time_scales(self, sql: str) -> Optional[Mapping[int, int]]:
        """Scale of the time columns by position. Arrow's time64[ns] does not carry it,
        so ask the server for the result set metadata (no execution of the query)."""
        from ..query import sql_quote_value

        try:
            rows = self.source_sql_to_py(
                "SELECT d.column_ordinal, d.scale FROM sys.dm_exec_describe_first_result_set(N"
                + sql_quote_value(sql)
                + ", NULL, 0) AS d WHERE d.is_hidden = 0 AND d.system_type_id = 41"
            )
        except Exception as e:
            logger.warning(f"Could not read the scale of time columns, using 7: {e}")
            return None
        return {r["column_ordinal"] - 1: r["scale"] for r in rows}

    def source_schema_limit_one(
        self, sql: ex.Query
    ) -> "list[InformationSchemaColInfo]":
        from ..metadata import InformationSchemaColInfo

        sc = self._source_reader(sql.limit(0).sql(self.source_dialect)).schema
        return [
            InformationSchemaColInfo(
                column_name=n,
                data_type_str=str(sc.field(n).type),
                data_type=_build_type(_get_type(sc.field(n).type)),
                is_nullable=sc.field(n).nullable,
            )
            for n in sc.names
        ]

    def source_sql_to_py(self, sql: Union[str, ex.Query]) -> list[dict]:
        if isinstance(sql, ex.Query):
            sql = sql.sql(self.source_dialect)
        result: list[dict] = []
        for batch in self._source_reader(sql):
            result.extend(batch.to_pylist())
        return result

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
