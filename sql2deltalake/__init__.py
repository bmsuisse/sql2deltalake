"""sql2deltalake: incremental loads from SQL Server and Postgres into append-only SCD2 Delta tables.

The Spark data source lives in ``sql2deltalake.spark``."""

from .write_init import (
    make_writer,
    DBDeltaPathConfigs,
    WriteConfig,
    DEFAULT_DATA_TYPE_MAP,
    WriteConfigAndInfos,
)
from .reader.reader import DataSourceReader
from .destination.destination import Destination
from .load_lock import LoadAlreadyRunning
from pathlib import Path
from typing import Union


def write_db_to_delta(
    source: Union[DataSourceReader, str],
    table_or_query: tuple[str, str],
    destination: Union[Destination, Path],
    write_config: Union[WriteConfig, None] = None,
):
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
