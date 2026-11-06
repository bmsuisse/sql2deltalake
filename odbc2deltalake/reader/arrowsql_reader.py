import logging
import re
from typing import TYPE_CHECKING, Literal, Mapping, Optional, Union

import sqlglot.expressions as ex

from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.read_utils.delta_rs import _all_nullable, _build_type, _get_type

from .odbc_reader import ODBCReader

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
    from odbc2deltalake.metadata import InformationSchemaColInfo

_DRIVER_KEYWORD = re.compile(r"(?i)(?:^|(?<=;))\s*driver\s*=\s*(\{[^}]*\}|[^;]*)(;|$)")


def strip_odbc_driver(connection_string: str) -> str:
    """arrowsql speaks TDS itself, so an ODBC `Driver=` keyword must not be passed on."""
    return _DRIVER_KEYWORD.sub("", connection_string)


def _normalize_schema(schema: "pa.Schema") -> "pa.Schema":
    """Make arrowsql's Arrow types match what arrow-odbc produces, so Delta tables are identical."""
    import pyarrow as pa
    import pyarrow.types as pat

    def _t(t: "pa.DataType") -> "pa.DataType":
        if pat.is_large_string(t):
            return pa.string()
        if pat.is_large_binary(t):
            return pa.binary()
        if pat.is_time(t):
            return pa.string()  # Delta has no time type, ODBC delivers it as text
        if pat.is_timestamp(t) and t.tz is not None and t.tz != "UTC":
            return pa.timestamp(t.unit, tz="UTC")  # datetimeoffset, Delta wants UTC
        if pat.is_uint8(t):
            return pa.int16()  # tinyint is unsigned in T-SQL, see write_init.py
        return t

    return pa.schema(
        [pa.field(f.name, _t(f.type), f.nullable, f.metadata) for f in schema]
    )


_TIME_DEFAULT_SCALE = 7  # time without a length is time(7)
_NS_PER_UNIT = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}


def _format_time(col: "pa.Array", scale: int) -> "pa.Array":
    """`HH:MM:SS[.f...]` with `scale` fractional digits, the text the SQL Server ODBC driver
    returns for a time(scale) column. Works on the integer value, so time(7)'s 100ns digit is kept."""
    import pyarrow as pa
    import pyarrow.compute as pc

    ns = pc.multiply(col.cast(pa.int64()), _NS_PER_UNIT[col.type.unit])
    seconds = pc.divide(ns, 10**9)

    def _digits(values: "pa.Array", width: int) -> "pa.Array":
        return pc.utf8_lpad(values.cast(pa.string()), width, "0")

    def _mod(values: "pa.Array", by: int) -> "pa.Array":
        return pc.subtract(values, pc.multiply(pc.divide(values, by), by))

    text = pc.binary_join_element_wise(
        _digits(pc.divide(seconds, 3600), 2),
        _digits(_mod(pc.divide(seconds, 60), 60), 2),
        _digits(_mod(seconds, 60), 2),
        ":",
    )
    if scale > 0:
        fraction = pc.divide(_mod(ns, 10**9), 10 ** (9 - scale))
        text = pc.binary_join_element_wise(text, _digits(fraction, scale), ".")
    return text


def _normalize_batch(
    batch: "pa.RecordBatch",
    target: "pa.Schema",
    time_scales: Optional[Mapping[int, int]] = None,
) -> "pa.RecordBatch":
    """`time_scales`: the scale of time columns by position, time(7) if missing."""
    import pyarrow as pa
    import pyarrow.types as pat

    arrays = []
    for i, (col, field) in enumerate(zip(batch.columns, target)):
        if pat.is_time(col.type) and pat.is_string(field.type):
            scale = (time_scales or {}).get(i, _TIME_DEFAULT_SCALE)
            arrays.append(_format_time(col, scale).cast(field.type))
        else:
            arrays.append(col.cast(field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=target)


class ArrowSQLReader(ODBCReader):
    """Like `ODBCReader` (DuckDB + delta-rs locally), but every source read goes through
    [arrowsql](https://pypi.org/project/arrowsql/), which needs no ODBC driver.
    """

    def __init__(
        self,
        connection_string: str,
        local_db: str = ":memory:",
        source_dialect: str = "tsql",
    ) -> None:
        super().__init__(
            strip_odbc_driver(connection_string),
            local_db=local_db,
            source_dialect=source_dialect,
        )

    def _source_reader(self, sql: str) -> "pa.RecordBatchReader":
        import pyarrow as pa
        import pyarrow.types as pat
        import arrowsql

        stream = arrowsql.read_arrow_stream(  # pyright: ignore[reportPrivateImportUsage]
            self.connection_string, sql
        )
        reader = pa.RecordBatchReader.from_stream(stream)
        target = _normalize_schema(reader.schema)
        if target.equals(reader.schema):
            return reader
        time_scales = (
            self._time_scales(sql)
            if any(pat.is_time(f.type) for f in reader.schema)
            else None
        )
        return pa.RecordBatchReader.from_batches(
            target, (_normalize_batch(b, target, time_scales) for b in reader)
        )

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
        import pyarrow as pa
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError

        reader = self._source_reader(sql)
        dp, do = delta_path.as_path_options(flavor="object_store")
        cast_schema, schema_mode = self._handle_schema_drift(
            delta_path, allow_schema_drift, mode, reader.schema
        )
        if cast_schema:
            reader = pa.RecordBatchReader.from_batches(
                cast_schema, (b.cast(cast_schema) for b in reader)
            )
        try:
            write_deltalake(
                dp,
                reader,
                schema=_all_nullable(reader.schema),
                mode=mode,
                writer_properties=self.writer_properties,
                schema_mode=schema_mode,
                engine="rust",
                storage_options=do,
            )
        except DeltaError as e:
            if "No data source supplied to write command" in str(e):
                if mode == "overwrite":
                    self._write_empty_delta_table(reader.schema, dp, do)
            else:
                raise e
