"""sql2deltalake: the new name of odbc2deltalake. The Python API is the same; the Spark data
source lives in ``sql2deltalake.spark``."""

from odbc2deltalake import (
    DEFAULT_DATA_TYPE_MAP,
    DataSourceReader,
    DBDeltaPathConfigs,
    Destination,
    WriteConfig,
    WriteConfigAndInfos,
    make_writer,
    write_db_to_delta,
)

__all__ = [
    "DEFAULT_DATA_TYPE_MAP",
    "DataSourceReader",
    "DBDeltaPathConfigs",
    "Destination",
    "WriteConfig",
    "WriteConfigAndInfos",
    "make_writer",
    "write_db_to_delta",
]
