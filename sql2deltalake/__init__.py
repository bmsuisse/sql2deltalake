"""sql2deltalake: incremental loads from SQL Server and Postgres into append-only SCD2 Delta tables.

The Spark data source lives in ``sql2deltalake.spark``.

The names below are imported when first used: the executors of the Spark data source import
``sql2deltalake._core`` and ``sql2deltalake.spark`` only, and on Databricks every module a fresh
Python worker imports from a notebook-scoped library costs time (the writer stack, sqlglot and
pydantic took about 19 s there)."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from .destination.destination import Destination
    from .load_lock import LoadAlreadyRunning
    from .reader.reader import DataSourceReader
    from .write_init import (
        DBDeltaPathConfigs,
        DEFAULT_DATA_TYPE_MAP,
        WriteConfig,
        WriteConfigAndInfos,
        make_writer,
    )

_LAZY = {
    "make_writer": ".write_init",
    "DBDeltaPathConfigs": ".write_init",
    "WriteConfig": ".write_init",
    "DEFAULT_DATA_TYPE_MAP": ".write_init",
    "WriteConfigAndInfos": ".write_init",
    "DataSourceReader": ".reader.reader",
    "Destination": ".destination.destination",
    "LoadAlreadyRunning": ".load_lock",
}


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value
    return value


def write_db_to_delta(
    source: "Union[DataSourceReader, str]",
    table_or_query: tuple[str, str],
    destination: "Union[Destination, Path]",
    write_config: "Union[WriteConfig, None]" = None,
):
    from .write_init import make_writer

    return make_writer(
        source=source,
        table_or_query=table_or_query,
        destination=destination,
        write_config=write_config,
    ).execute()


__all__ = [
    "DEFAULT_DATA_TYPE_MAP",
    "DataSourceReader",
    "DBDeltaPathConfigs",
    "Destination",
    "LoadAlreadyRunning",
    "WriteConfig",
    "WriteConfigAndInfos",
    "make_writer",
    "write_db_to_delta",
]
