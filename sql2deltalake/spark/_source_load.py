"""The source side of a load: metadata, the SQL of each pass and the schema probe.

The SQL comes from the same functions ``write_db_to_delta`` uses, so a load through the Spark
data source reads exactly what the Python load reads. No SparkSession is needed here; pyspark
is only imported to build the result's Spark schema.
"""

import dataclasses
import datetime
import logging
import numbers
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import sqlglot.expressions as ex

from sql2deltalake.db_to_delta import (
    _delta2_sql,
    _full_load_sql,
    _get_type_map,
    _get_update_sql,
    _primary_key_data_sql,
    _source_convert,
    _watermark_parts,
    _watermark_sql,
)
from sql2deltalake.metadata import InformationSchemaColInfo
from sql2deltalake.reader.arrowsql_reader import ArrowSQLSource
from sql2deltalake.write_init import (
    VALID_FROM_COL_NAME,
    WriteConfig,
    WriteConfigAndInfos,
    compat_name,
    resolve_columns,
)

from ._options import LoadOptions
from ._plan import Plan, Split, Unit, quote

logger = logging.getLogger("sql2deltalake.spark")


# SQLSTATEs of a failed login, a lost connection or a timeout (arrowsql reports a login that
# timed out as HYT00 too)
_CONNECTION_STATES = re.compile(r"\[(08\w{3}|HYT00|HYT01)\]")


def _is_connection_error(e: Exception) -> bool:
    return isinstance(e, TimeoutError) or bool(_CONNECTION_STATES.search(str(e)))


class _Log:
    """The logger interface ``WriteConfigAndInfos`` expects, on top of ``logging``."""

    def info(self, msg: str, **kwargs: Any) -> None:
        logger.info(msg)

    def warning(self, msg: str, **kwargs: Any) -> None:
        logger.warning(msg)

    def error(self, msg: str, **kwargs: Any) -> None:
        logger.error(msg)

    def flush(self) -> None:
        pass


class SourceLoad:
    def __init__(
        self,
        opts: LoadOptions,
        get_target_name: Optional[Callable[[InformationSchemaColInfo], str]] = None,
    ):
        self.opts = opts
        source = ArrowSQLSource(opts.url, partitions=opts.num_partitions)
        write_config = WriteConfig(
            dialect="tsql",
            primary_keys=opts.keys,
            delta_col=opts.delta_column,
            no_complex_entries_load=opts.no_complex_entries_load,
            no_trim=opts.no_trim,
            get_target_name=get_target_name or compat_name,
        )
        table_or_query = opts.table_or_query()
        cols, pk_cols, delta_col = resolve_columns(
            source,  # type: ignore[arg-type]  # only the source side is used
            table_or_query,
            write_config,
        )
        self.infos = WriteConfigAndInfos(
            col_infos=cols,
            pk_cols=pk_cols,
            delta_col=delta_col,
            write_config=write_config,
            destination=None,  # type: ignore[arg-type]
            source=source,  # type: ignore[arg-type]
            table_or_query=table_or_query,
            logger=_Log(),  # type: ignore[arg-type]
        )
        self.source = source
        self._last_timestamp: Optional[datetime.datetime] = None
        self._clock: Optional[datetime.datetime] = None

    # --- names ---------------------------------------------------------------------------

    def name(self, col: InformationSchemaColInfo) -> str:
        return self.infos.write_config.get_target_name(col)

    @property
    def key_names(self) -> List[str]:
        return [self.name(c) for c in self.infos.pk_cols]

    @property
    def delta_name(self) -> Optional[str]:
        return self.name(self.infos.delta_col) if self.infos.delta_col else None

    @property
    def column_names(self) -> List[str]:
        return [self.name(c) for c in self.infos.col_infos]

    def use_delta_column(self, col: InformationSchemaColInfo) -> None:
        self.infos = dataclasses.replace(self.infos, delta_col=col)

    # --- timestamps ----------------------------------------------------------------------

    def source_state(self, counts: bool = True, mar: bool = True) -> Dict[str, Any]:
        """The server clock and, before the load reads any data, what the plan needs of the
        source, in one round trip: ``counts``: MAX of the delta column and COUNT(*) (only a plan
        that compares them with the state needs them, they scan the table);
        ``mar``: MIN_ACTIVE_ROWVERSION() (rowversion tables, in the table's database; only a
        commit stores it).

        MIN_ACTIVE_ROWVERSION() is optional: ``mar`` is None if it cannot be read. Without
        ``counts`` the query reads no table and runs as an optional probe (bounded, never
        retried); if it fails, only the clock is read. With ``counts`` a server error reads the
        rest once more without it; a failed login or a timeout is raised as it is: running the
        same query again without MIN_ACTIVE_ROWVERSION() would not help."""
        infos = self.infos
        clock = "CAST(GETUTCDATE() AS datetime2(6)) AS ts"
        parts = _watermark_parts(infos) if mar else None
        if not counts:
            row = None
            if parts is not None:
                mar_expr, db = parts
                sql = f"SELECT {clock}, {mar_expr} AS mar"
                if db is not None:
                    sql = f"EXEC {db}.sys.sp_executesql N'" + sql.replace("'", "''") + "'"
                rows = self.source.optional_sql_to_py(
                    sql, "MIN_ACTIVE_ROWVERSION(), the next load has no no-updates shortcut"
                )
                row = rows[0] if rows else None
            if row is None:
                row = {**self.source.source_sql_to_py(f"SELECT {clock}")[0], "mar": None}
            self._set_clock(row["ts"])
            return {"max": None, "count": None, "mar": row["mar"]}

        extra = [f"{self._max_sql()} AS max_ts", "COUNT(*) AS cnt", clock]
        from_sql = infos.from_("t").sql("tsql")
        plain = f"SELECT {', '.join(extra)}, NULL AS mar {from_sql[len('SELECT '):]}"
        sql = _watermark_sql(infos, extra) if parts is not None else None
        if sql is None:
            row = self.source.source_sql_to_py(plain)[0]
        else:
            try:
                row = self.source.source_sql_to_py(sql)[0]
            except Exception as e:
                if _is_connection_error(e):
                    raise
                logger.warning(f"Could not read MIN_ACTIVE_ROWVERSION(), no shortcut: {e}")
                row = self.source.source_sql_to_py(plain)[0]
        self._set_clock(row["ts"])
        return {"max": row["max_ts"], "count": row["cnt"], "mar": row["mar"]}

    def _max_sql(self) -> str:
        dc = self.infos.delta_col
        if dc is None:
            return "NULL"
        wc = self.infos.write_config
        return ex.func(
            "MAX",
            _source_convert(
                dc.column_name,
                dc.data_type,
                dc.data_type_str,
                table_alias="t",
                type_map=_get_type_map(wc),
                no_trim=wc.no_trim,
            ),
        ).sql("tsql")

    def _set_clock(self, clock: Any) -> None:
        if isinstance(clock, str):
            clock = datetime.datetime.fromisoformat(clock)
        self._clock = clock.replace(tzinfo=None)

    def _read_clock(self) -> datetime.datetime:
        res = self.source.source_sql_to_py(
            "SELECT CAST(GETUTCDATE() AS datetime2(6)) AS ts"
        )[0]["ts"]
        if isinstance(res, str):
            res = datetime.datetime.fromisoformat(res)
        return res.replace(tzinfo=None)

    def next_timestamp(
        self, after: Optional[datetime.datetime] = None
    ) -> ex.Expression:
        """The ``__timestamp`` literal of the next pass: the source server's UTC clock (read once
        per load), later than every earlier pass of this load and than ``after`` (the target's
        latest)."""
        floor = self._last_timestamp
        if after is not None:
            after = after.replace(tzinfo=None)
            if floor is None or after > floor:
                floor = after
        if self._clock is None:
            self._clock = self._read_clock()
        value = self._clock
        if floor is not None and value <= floor:
            value = floor + datetime.timedelta(microseconds=1)
        self._last_timestamp = value
        return ex.cast(
            ex.Literal.string(value.strftime("%Y-%m-%d %H:%M:%S.%f")),
            ex.DataType(this="datetime2(6)"),
        ).as_(VALID_FROM_COL_NAME, quoted=True)

    # --- SQL -----------------------------------------------------------------------------

    def _with_timestamp(self, ts: Optional[ex.Expression]) -> WriteConfigAndInfos:
        return dataclasses.replace(self.infos, load_timestamp=ts)  # type: ignore[arg-type]

    def full_sql(self, ts: ex.Expression) -> str:
        return _full_load_sql(self._with_timestamp(ts))

    def criterion(self, value: Any, op: str) -> ex.Expression:
        dc = self.infos.delta_col
        assert dc is not None
        wc = self.infos.write_config
        expr = _source_convert(
            dc.column_name,
            dc.data_type,
            dc.data_type_str,
            table_alias="t",
            type_map=_get_type_map(wc),
            no_trim=wc.no_trim,
        )
        return expr >= ex.convert(value) if op == ">=" else expr > ex.convert(value)

    def update_sql(self, ts: ex.Expression, criterion: Optional[ex.Expression]) -> str:
        return _get_update_sql(
            cols=self.infos.col_infos,
            criterion=criterion,
            query=self.infos.from_("t"),
            write_config=self.infos.write_config,
            valid_from=ts,
        )

    def keys_sql(self, data: List[Dict[str, Any]], ts: ex.Expression) -> str:
        """The rows of the given primary keys (``p0``, ``p1``, ...), through OPENJSON."""
        import json

        return _delta2_sql(self._with_timestamp(ts), json.dumps(data, default=str))

    def pk_sql(self, newer_than: Any = None) -> str:
        sql = _primary_key_data_sql(self.infos)
        if newer_than is None:
            return sql
        dc = self.delta_name
        assert dc is not None
        return f"SELECT * FROM ({sql}) AS q WHERE {quote(dc)} > {ex.convert(newer_than).sql('tsql')}"

    # --- reading -------------------------------------------------------------------------

    def split(self, available: Sequence[str], updates: bool = False) -> Optional[Split]:
        """Slices of a query whose result has the columns ``available``: ranges of
        ``partitionColumn``, else ranges of an integer first key column (one index range per
        slice, where every hash slice scans the whole source), else hashes of the key columns.
        The reads of a delta load (``updates``) are only sliced by ranges of ``partitionColumn``:
        they are small."""
        opts = self.opts
        if opts.num_partitions <= 1:
            return None
        if opts.partition_column:
            col = self._column(opts.partition_column)
            if not col.data_type.is_type(*_RANGE_TYPES):
                raise ValueError(
                    f"partitionColumn {opts.partition_column!r} is {col.data_type_str or col.data_type.sql('tsql')}: "
                    f"range slices need an {_RANGE_TYPES_TEXT} column"
                )
            name = self.name(col)
            if name in available:
                bounds = self._range_bounds(col)
                return Split("range", [name], bounds=bounds) if bounds else None
        if updates:
            return None
        first = self.infos.pk_cols[0] if self.infos.pk_cols else None
        if (
            first is not None
            and first.data_type.is_type(*ex.DataType.INTEGER_TYPES)
            and self.name(first) in available
        ):
            bounds = self._range_bounds(first)
            if bounds:
                return Split("range", [self.name(first)], bounds=bounds)
        keys = self.key_names
        if keys and all(k in available for k in keys):
            return Split("hash", keys, count=opts.num_partitions)
        logger.warning(
            "numPartitions is set, but there are no key columns to hash and no partitionColumn: one slice"
        )
        return None

    def pk_snapshot(self) -> Any:
        """The primary keys and delta values of all source rows as a pyarrow Table, read on the
        driver through arrowsql and the Rust cast stage; on ``numPartitions`` parallel
        connections if the first key is an integer."""
        partition_on = None
        first = self.infos.pk_cols[0] if self.infos.pk_cols else None
        if (
            self.opts.num_partitions > 1
            and first is not None
            and first.data_type.is_type(*ex.DataType.INTEGER_TYPES)
        ):
            partition_on = self.name(first)
        return self.read_rows(self.pk_sql(), partition_on)

    def read_rows(self, sql: str, partition_on: Optional[str] = None) -> Any:
        return self.source._source_reader(sql, partition_on=partition_on).read_all()

    def _column(self, name: str) -> InformationSchemaColInfo:
        for c in self.infos.col_infos:
            if name.lower() in (c.column_name.lower(), self.name(c).lower()):
                return c
        raise ValueError(f"partitionColumn {name!r} is not a column of the source")

    def _range_bounds(self, col: InformationSchemaColInfo) -> List[str]:
        if not hasattr(self, "_bounds"):
            wc = self.infos.write_config
            expr = _source_convert(
                col.column_name,
                col.data_type,
                col.data_type_str,
                table_alias="t",
                type_map=_get_type_map(wc),
                no_trim=wc.no_trim,
            )
            sql = (
                self.infos.from_("t")
                .select(ex.func("MIN", expr).as_("lo"), ex.func("MAX", expr).as_("hi"))
                .sql("tsql")
            )
            row = self.source.source_sql_to_py(sql)[0]
            lo, hi = row["lo"], row["hi"]
            cuts = [] if lo is None else range_cuts(lo, hi, self.opts.num_partitions)
            self._bounds = [ex.convert(c).sql("tsql") for c in cuts]
        return self._bounds

    def probe(self, sql: str) -> Tuple[str, Dict[str, int]]:
        """Spark schema (JSON) and time(n) scales of the result of ``sql``, without reading rows.
        The scales are asked of the server only if the result has time columns, once; if that
        fails, they come from the column metadata."""
        import pyarrow as pa
        import pyarrow.types as pat

        from sql2deltalake.reader.arrowsql_reader import _normalize_schema

        raw = pa.RecordBatchReader.from_stream(
            self.source._source_arrow(f"SELECT TOP 0 * FROM ({sql}) AS q")
        )
        raw_schema = raw.schema
        for _ in raw:
            pass
        times = [i for i, f in enumerate(raw_schema) if pat.is_time(f.type)]
        scales: Dict[str, int] = {}
        if times:
            by_pos = self.source._time_scales(sql)
            if by_pos is None:
                known = self._metadata_time_scales()
                by_pos = {i: known[raw_schema.names[i]] for i in times if raw_schema.names[i] in known}
            scales = {raw_schema.names[i]: s for i, s in by_pos.items()}
        return spark_schema_json(_normalize_schema(raw_schema)), scales

    def _metadata_time_scales(self) -> Dict[str, int]:
        """The scale of each time(n) source column, by target name, from the column metadata."""
        scales = {}
        for c in self.infos.col_infos:
            if c.data_type.is_type(ex.DataType.Type.TIME):
                m = re.search(r"\((\d+)\)", c.data_type_str or "")
                scales[self.name(c)] = int(m.group(1)) if m else 7
        return scales

    def result_schema(self) -> str:
        """Spark schema (JSON) of a full load's result, without reading rows."""
        ts = ex.cast(
            ex.Literal.string("1900-01-01 00:00:00.000000"),
            ex.DataType(this="datetime2(6)"),
        ).as_(VALID_FROM_COL_NAME, quoted=True)
        return self.probe(self.full_sql(ts))[0]

    def plan(
        self,
        kind: str,
        units: List[Unit],
        sql: str,
        known: Optional[Tuple[str, Dict[str, int]]] = None,
        **kwargs: Any,
    ) -> Plan:
        """``known``: the schema and time scales of an earlier plan of the same source schema,
        instead of probing the source."""
        schema, scales = known if known is not None else self.probe(sql)
        return Plan(
            kind=kind,
            schema=schema,
            units=units,
            time_scales=scales,
            batch_size=self.opts.batch_size,
            utf8_strings=self.opts.utf8_strings,
            **kwargs,
        )

    def full_plan(self, info: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Plan:
        sql = self.full_sql(self.next_timestamp(kwargs.pop("after", None)))
        unit = Unit(sql=sql, split=self.split(self.column_names))
        return self.plan("full", [unit], sql, info=info or {}, **kwargs)


# bit has no MIN/MAX in SQL Server; text, binary, uniqueidentifier and time have no arithmetic
_RANGE_TYPES = (
    (ex.DataType.NUMERIC_TYPES - {ex.DataType.Type.BIT})
    | (ex.DataType.TEMPORAL_TYPES - {ex.DataType.Type.TIME, ex.DataType.Type.TIMETZ})
)
_RANGE_TYPES_TEXT = "integer, decimal, float, date or datetime"


def range_cuts(low: Any, high: Any, count: int) -> list:
    """Strictly ascending interior boundaries cutting [low, high] into ``count`` slices."""
    cuts: list[Any]
    if isinstance(low, bool) or not isinstance(low, (numbers.Number, datetime.date)):
        raise ValueError(
            f"range slices need {_RANGE_TYPES_TEXT} bounds, not {type(low).__name__}"
        )
    if isinstance(low, datetime.datetime):
        cuts = [low + (high - low) * k // count for k in range(1, count)]
    elif isinstance(low, datetime.date):
        cuts = [
            low + datetime.timedelta(days=(high - low).days * k // count)
            for k in range(1, count)
        ]
    elif isinstance(low, int):
        cuts = [low + (high - low) * k // count for k in range(1, count)]
    else:
        cuts = [low + (high - low) * k / count for k in range(1, count)]
    return sorted({c for c in cuts if low < c <= high})


def spark_schema_json(arrow_schema: Any) -> str:
    """Spark schema of an Arrow schema: zone-less timestamps as TimestampNTZ (the wall-clock value,
    as the Python load writes them), every column nullable (delete markers are NULL)."""
    import pyarrow as pa
    from pyspark.sql.pandas.types import from_arrow_schema
    from pyspark.sql.types import StructField, StructType, TimestampNTZType

    fields = []
    for spark_field, arrow_field in zip(
        from_arrow_schema(arrow_schema).fields, arrow_schema
    ):
        data_type = spark_field.dataType
        if pa.types.is_timestamp(arrow_field.type) and arrow_field.type.tz is None:
            data_type = TimestampNTZType()
        fields.append(StructField(spark_field.name, data_type, True))
    return StructType(fields).json()
