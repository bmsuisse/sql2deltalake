"""Load plans of the Spark data source, and the executor side of reading them.

A plan is everything a load reads, decided on the driver: the source queries (each split into
slices, one Spark task per slice), delete markers, the Spark schema of the result and what
``commit`` needs afterwards. It travels as one string option, so the data source, which Spark
runs in Python worker processes without a SparkSession, needs no state of its own.

Nothing in this module imports pyspark.
"""

import base64
import json
import zlib
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

if TYPE_CHECKING:
    import pyarrow as pa

PLAN_VERSION = 2

IS_DELETED = "__is_deleted"
IS_FULL_LOAD = "__is_full_load"
TIMESTAMP = "__timestamp"


@dataclass
class Split:
    """How one source query is cut into slices. ``hash``: ``(CHECKSUM(columns) & 2147483647) %
    count = i`` over key columns only, so every slice query selects the same rows each time it runs.
    ``range``: half-open intervals between ``bounds`` (SQL literals) on one column; the first
    slice also reads the NULLs."""

    kind: str
    columns: List[str]
    count: int = 1
    bounds: List[str] = field(default_factory=list)

    @property
    def slices(self) -> int:
        return self.count if self.kind == "hash" else len(self.bounds) + 1

    def predicate(self, index: int) -> str:
        if self.kind == "hash":
            cols = ", ".join(quote(c) for c in self.columns)
            return f"(CHECKSUM({cols}) & 2147483647) % {self.count} = {index}"
        col = quote(self.columns[0])
        parts = []
        if index > 0:
            parts.append(f"{col} >= {self.bounds[index - 1]}")
        if index < len(self.bounds):
            parts.append(f"{col} < {self.bounds[index]}")
        where = " AND ".join(parts)
        return f"({where} OR {col} IS NULL)" if index == 0 else where


@dataclass
class Unit:
    """One part of the result: a source query (``sql``), or rows carried in the plan (``rows``:
    Arrow IPC of key columns, emitted as delete markers stamped ``timestamp``)."""

    sql: Optional[str] = None
    split: Optional[Split] = None
    rows: Optional[str] = None
    timestamp: Optional[str] = None

    @property
    def slices(self) -> int:
        return self.split.slices if self.split else 1

    def slice_sql(self, index: int) -> str:
        assert self.sql is not None
        if self.split is None:
            return self.sql
        return f"SELECT * FROM ({self.sql}) AS q WHERE {self.split.predicate(index)}"


@dataclass
class Plan:
    kind: str
    """full, delta, simple_delta, append_inserts, unchanged (nothing to load) or rows (an internal read)."""
    schema: str
    """The Spark schema of the result, as JSON."""
    units: List[Unit]
    time_scales: Dict[str, int] = field(default_factory=dict)
    """Scale of the time(n) columns by name (Arrow does not carry it), formatted as text like ODBC."""
    batch_size: int = 65536
    utf8_strings: bool = False
    commit: Optional[Dict[str, Any]] = None
    """What ``commit`` needs to advance the state, None if there is nothing to commit."""
    info: Dict[str, Any] = field(default_factory=dict)
    """Why the plan is what it is (for logs and tests)."""
    version: int = PLAN_VERSION

    def encode(self) -> str:
        raw = json.dumps(asdict(self), separators=(",", ":"), default=str).encode(
            "utf-8"
        )
        return base64.b64encode(zlib.compress(raw, 6)).decode("ascii")

    @classmethod
    def decode(cls, text: str) -> "Plan":
        data = json.loads(zlib.decompress(base64.b64decode(text)).decode("utf-8"))
        if data.get("version") != PLAN_VERSION:
            raise ValueError(
                f"plan version {data.get('version')} is not {PLAN_VERSION}: "
                "prepare() and the data source come from different sql2deltalake versions"
            )
        units = []
        for u in data.pop("units"):
            split = u.pop("split")
            units.append(Unit(split=Split(**split) if split else None, **u))
        return cls(units=units, **data)

    def slices(self) -> List[tuple]:
        """(unit index, slice index) of every Spark task."""
        return [(u, s) for u, unit in enumerate(self.units) for s in range(unit.slices)]


def quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def encode_rows(table: "pa.Table") -> str:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return base64.b64encode(zlib.compress(sink.getvalue().to_pybytes(), 6)).decode(
        "ascii"
    )


def decode_rows(text: str) -> "pa.Table":
    import pyarrow as pa

    return pa.ipc.open_stream(zlib.decompress(base64.b64decode(text))).read_all()


# --- executor side ---------------------------------------------------------------------


def read_slice(
    connection_string: str,
    plan: Plan,
    unit_index: int,
    slice_index: int,
    target: "pa.Schema",
) -> Iterator["pa.RecordBatch"]:
    """The Arrow batches of one slice, cast to ``target`` (Spark's Arrow schema of the result)."""
    unit = plan.units[unit_index]
    if unit.rows is not None:
        yield _delete_markers(decode_rows(unit.rows), unit.timestamp, target)
        return
    yield from _read_sql(connection_string, unit.slice_sql(slice_index), plan, target)


def _read_sql(
    connection_string: str, sql: str, plan: Plan, target: "pa.Schema"
) -> Iterator["pa.RecordBatch"]:
    """arrowsql's Arrow stream through the Rust cast stage (Delta types, then Spark's Arrow
    schema by position): the batches reach Spark without a copy or a Python loop over rows."""
    import arrowsql
    import pyarrow as pa

    from odbc2deltalake import _core

    stream = arrowsql.read_arrow_stream(  # pyright: ignore[reportPrivateImportUsage]
        connection_string,
        sql,
        batch_size=plan.batch_size,
        utf8_strings=plan.utf8_strings,
    )
    try:
        scales = {
            i: plan.time_scales[f.name]
            for i, f in enumerate(target)
            if f.name in plan.time_scales
        }
        yield from pa.RecordBatchReader.from_stream(
            _core.normalize(stream, scales, target)
        )
    finally:
        close = getattr(stream, "close", None)
        if close is not None:
            close()


def _delete_markers(
    keys: "pa.Table", timestamp: Optional[str], target: "pa.Schema"
) -> "pa.RecordBatch":
    """Key values, NULL in every other column, flagged as deleted."""
    import datetime

    import pyarrow as pa

    n = keys.num_rows
    columns = []
    for f in target:
        if f.name in keys.column_names:
            columns.append(keys.column(f.name).combine_chunks().cast(f.type))
        elif f.name == TIMESTAMP:
            assert timestamp is not None
            ts = datetime.datetime.fromisoformat(timestamp)
            columns.append(pa.array([ts] * n, pa.timestamp("us")).cast(f.type))
        elif f.name == IS_DELETED:
            columns.append(pa.array([True] * n, f.type))
        elif f.name == IS_FULL_LOAD:
            columns.append(pa.array([False] * n, f.type))
        else:
            columns.append(pa.nulls(n, f.type))
    return pa.RecordBatch.from_arrays(columns, schema=target)
