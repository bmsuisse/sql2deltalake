"""The Spark Python data source ``sql2deltalake`` (pyspark >= 4.0)."""

import json
from typing import Any, Iterator, List

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
except ImportError as exc:  # pragma: no cover - only without pyspark
    raise ImportError("sql2deltalake.spark needs pyspark>=4.0") from exc

from ._options import LoadOptions, normalize
from ._plan import Plan, read_slice

FORMAT = "sql2deltalake"


def _plan(options: Any) -> Plan:
    opts = normalize(options)
    if opts.get("plan"):
        return Plan.decode(opts["plan"])
    if opts.get("target"):
        raise ValueError(
            "the data source cannot read the target's state (Spark runs it without a "
            "SparkSession): pass the options returned by "
            "sql2deltalake.spark.prepare(spark, ...) instead of 'target'"
        )
    from ._source_load import SourceLoad

    # No target: a full load of the source, e.g. to look at what a first load writes.
    return SourceLoad(LoadOptions.parse(options)).full_plan()


def _arrow_schema(schema: Any) -> Any:
    from pyspark.sql.pandas.types import to_arrow_schema

    try:
        return to_arrow_schema(schema, timestamp_utc=True)
    except TypeError:  # pyspark 4.0
        return to_arrow_schema(schema)


class _Slice(InputPartition):
    def __init__(self, unit: int, index: int):
        self.unit, self.index = unit, index


class _Reader(DataSourceReader):
    def __init__(self, options: Any, schema: Any):
        from sql2deltalake.reader.arrowsql_reader import strip_odbc_driver

        self.url = strip_odbc_driver(str(normalize(options)["url"]))
        self.plan = _plan(options)
        self.schema = schema

    def partitions(self) -> List[InputPartition]:
        slices = [_Slice(u, s) for u, s in self.plan.slices()]
        return slices or [_Slice(-1, 0)]  # nothing to read

    def read(self, partition: InputPartition) -> Iterator[Any]:  # type: ignore[override]
        part: Any = partition
        if part.unit < 0:
            return
        target = _arrow_schema(self.schema)
        yield from read_slice(self.url, self.plan, part.unit, part.index, target)


class SqlToDeltaDataSource(DataSource):
    """``spark.read.format("sql2deltalake")``: the rows an incremental load appends to its Delta
    target. See ``sql2deltalake.spark``."""

    @classmethod
    def name(cls) -> str:
        return FORMAT

    def schema(self) -> Any:
        from pyspark.sql.types import StructType

        return StructType.fromJson(json.loads(_plan(self.options).schema))

    def reader(self, schema: Any) -> DataSourceReader:
        return _Reader(self.options, schema)
