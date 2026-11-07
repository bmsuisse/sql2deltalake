"""The boundary to the Rust core (`odbc2deltalake._core`): sqlglot objects and user callables are
turned into plain values here, once per load, so the core needs no Python while it runs."""

from typing import TYPE_CHECKING, Any, Mapping, Union

import sqlglot.expressions as ex

from .sql_schema import is_string_type

if TYPE_CHECKING:
    from .metadata import InformationSchemaColInfo
    from .write_init import WriteConfig, WriteConfigAndInfos


def column_spec(
    c: "InformationSchemaColInfo", write_config: "WriteConfig"
) -> dict[str, Any]:
    dialect = write_config.dialect
    return {
        "name": c.column_name,
        "target": write_config.get_target_name(c),
        "type_key": ex.DataType(this=c.data_type.this).sql(dialect).lower(),
        "type_sql": c.data_type.sql(dialect),
        "orig_type": c.data_type_str or "",
        "is_string": bool(is_string_type(c.data_type)),
        "is_numeric": c.data_type.this
        in [ex.DataType.Type.BOOLEAN, *ex.DataType.NUMERIC_TYPES],
        "is_identity": bool(c.is_identity),
    }


def type_map_spec(write_config: "WriteConfig") -> dict[str, tuple[str, bool]]:
    from .write_init import DEFAULT_DATA_TYPE_MAP

    tm: Mapping[str, Union[ex.DataType, ex.DataType.Type]] = (
        DEFAULT_DATA_TYPE_MAP.get(write_config.dialect, {})
        if write_config.data_type_map == "default"
        else write_config.data_type_map
    )
    res: dict[str, tuple[str, bool]] = {}
    for key, mapped in tm.items():
        if not isinstance(mapped, (ex.DataType, ex.DataType.Type, str)):
            continue  # never looked up, e.g. a dialect-keyed map merged in by mistake
        to = ex.cast(ex.column("x"), mapped).to  # what sqlglot renders for CAST(x AS mapped)
        res[key] = (to.sql(write_config.dialect), bool(is_string_type(mapped)))
    return res


def source_spec(infos: "WriteConfigAndInfos") -> Union[list[str], str]:
    """The table name parts, or the query rendered in the source dialect."""
    toq = infos.table_or_query
    if isinstance(toq, ex.Query):
        return toq.sql(infos.write_config.dialect)
    if isinstance(toq, str):
        return [toq]
    return list(toq)


def fail_point(name: str) -> "Union[bool, None]":
    """Test hook, called by the load at a few named steps (`load_updates`, `additional_updates`,
    `restore_last_pk`, `write_delta2`, `local_pk_count`). Tests patch it to inject a failure or to
    change the source mid-load; returning False from `restore_last_pk` skips the restore."""
    return None


def enabled() -> bool:
    import os

    return os.getenv("SQL2DELTA_NATIVE", "1") not in ("0", "false", "False", "")


def supports(infos: "WriteConfigAndInfos") -> bool:
    """The Rust load handles readers with local Delta tables; for now SQL Server through arrowsql."""
    from .reader.arrowsql_reader import ArrowSQLReader

    return (
        enabled()
        and isinstance(infos.source, ArrowSQLReader)
        and infos.write_config.dialect in ("tsql", "mssql")
    )


def _overrides(reader, name: str) -> bool:
    """True if a subclass replaced the reader's own implementation of `name`."""
    from .reader.adbc_reader import ADBCReader
    from .reader.arrowsql_reader import ArrowSQLReader

    impl = getattr(type(reader), name)
    return all(
        impl is not getattr(base, name, None) for base in (ArrowSQLReader, ADBCReader)
    )


class Host:
    """What the Rust load calls back into: the source, the Delta tables below the destination and
    the logger. Data crosses as Arrow C streams."""

    def __init__(self, infos: "WriteConfigAndInfos"):
        self.infos = infos
        self.reader = infos.source
        self.dest = infos.destination
        self.logger = infos.logger
        self._copies = _overrides(self.reader, "source_write_sql_to_delta")

    # source
    def source_query(self, sql: str):
        reader = self.reader
        if hasattr(reader, "_source_arrow") and not _overrides(reader, "source_sql_to_py"):
            return reader._source_arrow(sql, utf8_strings=False)  # type: ignore
        import pyarrow as pa

        return pa.Table.from_pylist(reader.source_sql_to_py(sql))

    def source_connection(self):
        """(connection string, utf8_strings, partitions) if Rust reads the source itself through
        arrowsql's core, None if the reads go through the reader's Python methods."""
        from .reader.arrowsql_reader import ArrowSQLSource, core_reads

        reader = self.reader
        if (
            self._copies
            or not isinstance(reader, ArrowSQLSource)
            or _overrides(reader, "source_sql_to_py")
            or _overrides(reader, "_source_arrow")
            or not core_reads(reader.connection_string)
        ):
            return None
        return (reader.connection_string, reader.utf8_strings, reader.partitions)

    def source_stream(self, sql: str, partition_on=None):
        if self._copies or not hasattr(self.reader, "_source_arrow"):
            return None
        return self.reader._source_arrow(  # type: ignore
            sql,
            utf8_strings=getattr(self.reader, "utf8_strings", False),
            partition_on=partition_on,
        )

    def source_copy(self, sql: str, rel: str, mode, drift):
        self.reader.source_write_sql_to_delta(
            sql, self.dest / rel, mode, allow_schema_drift=drift
        )

    # target
    def kernel_root(self):
        """The target folder as a local path, whose Delta tables Rust reads itself (delta-kernel-rs)."""
        from .destination.file_system import FileSystemDestination

        if type(self.dest) is FileSystemDestination:
            return str(self.dest.path.absolute())
        return None

    def _dt(self, rel: str, version=None):
        from .reader.local_reader import _delta_table

        dt = _delta_table(self.dest / rel)
        if version is not None:
            dt.load_as_version(version)
        return dt

    def table_exists(self, rel: str) -> bool:
        return self.reader.local_delta_table_exists(self.dest / rel)

    def path_exists(self, rel: str) -> bool:
        return (self.dest / rel).exists()

    def version(self, rel: str) -> int:
        return self._dt(rel).version()

    def read(self, rel: str, version, columns):
        import pyarrow as pa

        ds = self._dt(rel, version).to_pyarrow_dataset()
        if columns is not None:
            names = {n.lower(): n for n in ds.schema.names}
            columns = [c if c in ds.schema.names else names.get(c.lower(), c) for c in columns]
        return ds.scanner(columns=columns).to_reader()

    def column_names(self, rel: str) -> "list[str]":
        return [f.name for f in self._dt(rel).schema().fields]

    def write(self, rel: str, stream, mode, drift):
        import pyarrow as pa

        self.reader._write_reader_to_delta(  # type: ignore
            pa.RecordBatchReader.from_stream(stream),
            self.dest / rel,
            mode,
            drift,
            all_nullable=True,
        )

    def restore(self, rel: str, version: int):
        self.reader.get_local_delta_ops(self.dest / rel).restore(version)

    def remove(self, rel: str, recursive: bool):
        (self.dest / rel).remove(recursive)

    def vacuum(self, rel: str):
        self.reader.get_local_delta_ops(self.dest / rel).vacuum()

    def mkdir(self, rel: str):
        (self.dest / rel).mkdir()

    def upload_str(self, rel: str, content: str):
        (self.dest / rel).upload_str(content)

    def file_age_seconds(self, rel: str):
        from datetime import datetime, timezone

        p = self.dest / rel
        if not p.exists():
            return None
        return (datetime.now(tz=timezone.utc) - p.modified_time()).total_seconds()

    def log(self, level: str, msg: str, kwargs: dict):
        getattr(self.logger, level)(msg, **kwargs)

    def fail_point(self, name: str):
        return fail_point(name)


def source_schema_fingerprint(infos: "WriteConfigAndInfos") -> str:
    from .db_to_delta import _source_schema_fingerprint

    return _source_schema_fingerprint(infos)


def load_spec(infos: "WriteConfigAndInfos") -> dict[str, Any]:
    from .db_to_delta import _shortcut_allowed, _watermark_parts, _watermark_sql

    wc = infos.write_config
    cols = list(infos.col_infos)

    def _index(c) -> int:
        for i, x in enumerate(cols):
            if x is c:
                return i
        for i, x in enumerate(cols):
            if x.column_name == c.column_name:
                return i
        raise ValueError(f"column {c.column_name} not in the columns of the load")

    return {
        "dialect": wc.dialect,
        "source": source_spec(infos),
        "type_map": type_map_spec(wc),
        "no_trim": wc.no_trim,
        "columns": [column_spec(c, wc) for c in cols],
        "pks": [_index(c) for c in infos.pk_cols],
        "delta_col": _index(infos.delta_col) if infos.delta_col is not None else None,
        "load_mode": wc.load_mode,
        "allow_schema_drift": wc.allow_schema_drift,
        "no_complex_entries_load": wc.no_complex_entries_load,
        "schema_fingerprint": source_schema_fingerprint(infos),
        "shortcut_allowed": _shortcut_allowed(infos),
        "watermark_sql": _watermark_sql(infos),
        "watermark_parts": _watermark_parts(infos),
    }


def _result(res: dict):
    from .load_result import (
        AppendOnlyLoadResult,
        DeltaLoadResult,
        FullLoadResult,
        NoLoadResult,
    )

    t = res["type"]
    if t == "full":
        return FullLoadResult()
    if t == "none":
        return NoLoadResult()
    if t == "append_only":
        return AppendOnlyLoadResult()
    r = DeltaLoadResult()
    r.starting_local_state = res["starting_local_state"]
    r.starting_source_state = res["starting_source_state"]
    if "end_source_state" in res:
        r.end_source_state = res["end_source_state"]
    r.dirty = res["dirty"]
    return r


def execute(infos: "WriteConfigAndInfos"):
    """Runs the load in Rust (`_core.run_load`); the GIL is released while it runs."""
    from . import _core

    try:
        return _result(_core.run_load(Host(infos), load_spec(infos)))
    finally:
        infos.logger.flush()


def restore_last_pk(infos: "WriteConfigAndInfos") -> bool:
    from . import _core

    return _core.restore_last_pk(Host(infos), load_spec(infos))
