"""``prepare`` and ``commit``: the driver side of an incremental load, which needs a SparkSession.

Spark runs a Python data source (``schema()``, ``reader()``, ``partitions()``, ``read()`` and a
writer's ``commit()``) in Python worker processes that have no SparkSession, so the data source
cannot read the target or the state tables. ``prepare`` reads them on the driver, decides what the
load reads and hands that to the data source as the ``plan`` option. ``commit`` advances the state
after the DataFrame was appended to the target. ``run`` does all of it in one call.

State, next to the target (``<target>_state/latest_pk_version`` for a path,
``<target>_state_latest_pk_version`` for a name): the primary keys and delta value of every row
as of the last committed load, append-only: the rows with ``__state_seq`` 0 are the keys as of the
last rewrite, each later commit appends only the keys its load changed or deleted (``__state_seq``
the version it creates, ``__is_deleted`` for a deleted key), and a commit rewrites the table when
those appended rows outgrow a fifth of the keys. Its newest Delta commit carries the load state as ``userMetadata``: the
target version the state belongs to, MAX/COUNT of the delta values, the source schema
fingerprint, the watermark of the next load and MIN_ACTIVE_ROWVERSION() read before the load read
any data. ``DESCRIBE HISTORY`` reads it without a Spark scan, so an unchanged table costs two
small Spark jobs (the history of the target and of the state) and one source round trip.

Everything between the source and the Delta tables runs in Rust (``sql2deltalake._core``): the
primary-key snapshot is read on the driver through arrowsql, compared with ``latest_pk_version``
in a parallel hash pass, and the next ``latest_pk_version`` is merged from the appended rows.

The state is a cache of the target: when it does not belong to the target as it is now (no commit
after an append, a failed load, a crash between the writes), ``prepare`` rebuilds
``latest_pk_version`` from the target itself (latest row per key since the last full load).
"""

import datetime
import decimal
import json
import os
import threading
import uuid
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

from sql2deltalake.db_to_delta import (
    _comparable_ts,
    _is_rowversion,
    _shortcut_allowed,
    _source_schema_fingerprint,
)
from sql2deltalake.metadata import InformationSchemaColInfo

from ._options import LoadOptions, normalize
from ._plan import IS_DELETED, IS_FULL_LOAD, TIMESTAMP, Plan, Unit, encode_rows
from ._source_load import SourceLoad, logger

FORMAT = "sql2deltalake"

STATE_FORMAT = 3  # 3: latest_pk_version is append-only (__state_seq, __is_deleted)
_META = "sql2deltalake_state"
_STRANGE_UPDATES_BY_KEY = 1000  # more strange updates are re-read by MIN(delta column)
_HISTORY = 20
_SEQ = "__state_seq"
# userMetadata of a target commit that only changed column types (see _Planner._migrate)
_MIGRATION = "sql2deltalake_type_migration"
# a commit rewrites latest_pk_version when more rows than this were appended since the last
# rewrite: a fifth of the keys, at least _MIN_LOG_ROWS
_LOG_RATIO = 5
_MIN_LOG_ROWS = 10_000
# rows per file of latest_pk_version (keys and delta column: about 2 MB each), so a state of
# 1M keys is written by 4 tasks
_STATE_FILE_ROWS = 250_000
# Delta operations that keep the rows of a table as they are
_SAME_ROWS = frozenset(
    {
        "OPTIMIZE",
        "VACUUM START",
        "VACUUM END",
        "SET TBLPROPERTIES",
        "UNSET TBLPROPERTIES",
        "ADD CONSTRAINT",
        "DROP CONSTRAINT",
        "REORG",
        "UPGRADE PROTOCOL",
    }
)

# latest_pk_version as prepare() read it, for commit() in the same driver process: one version
# per state location, of the few locations used last (a long-lived driver loading many targets
# must not keep the key state of all of them)
_previous_latest: "OrderedDict[Tuple[str, int], Any]" = OrderedDict()
_previous_latest_lock = threading.Lock()
_STATE_CACHE_ENV = "SQL2DELTA_SPARK_STATE_CACHE"
_STATE_CACHE_DEFAULT = 4


_KEY_STATE_ENV = "SQL2DELTA_SPARK_KEY_STATE"
_DRIVER_KEYS_ENV = "SQL2DELTA_SPARK_DRIVER_KEYS"
_DRIVER_KEYS_DEFAULT = 2_000_000


def _key_state(keys: Optional[int]) -> str:
    """Where a load compares and merges the key state of a table of ``keys`` keys (None:
    unknown): "driver", as Arrow tables in Rust (a few Spark jobs; every key in driver memory, so
    only up to SQL2DELTA_SPARK_DRIVER_KEYS keys, default 2M: a 78M-row table ran a Databricks
    driver out of memory), or "spark", in DataFrames (no key on the driver, but some 30 Spark jobs
    more, which a small table pays for in seconds). SQL2DELTA_SPARK_KEY_STATE=spark or driver
    picks one for every table."""
    forced = os.environ.get(_KEY_STATE_ENV, "auto").strip().lower()
    if forced in ("spark", "driver"):
        return forced
    try:
        limit = int(os.environ.get(_DRIVER_KEYS_ENV, _DRIVER_KEYS_DEFAULT))
    except ValueError:
        limit = _DRIVER_KEYS_DEFAULT
    return "driver" if keys is not None and keys <= limit else "spark"


def _state_cache_size() -> int:
    """How many targets' key state a driver keeps (env SQL2DELTA_SPARK_STATE_CACHE)."""
    try:
        return max(0, int(os.environ.get(_STATE_CACHE_ENV, _STATE_CACHE_DEFAULT)))
    except ValueError:
        return _STATE_CACHE_DEFAULT


def register(spark: Any) -> None:
    """Register the ``sql2deltalake`` format with a SparkSession."""
    from ._source import SqlToDeltaDataSource

    spark.dataSource.register(SqlToDeltaDataSource)


# --- values in the state ---------------------------------------------------------------------


def _encode(value: Any) -> Optional[Dict[str, Any]]:
    """A delta value or timestamp as JSON, with its type."""
    if value is None:
        return None
    if isinstance(value, bool):
        return {"t": "bool", "v": value}
    if isinstance(value, int):
        return {"t": "int", "v": value}
    if isinstance(value, float):
        return {"t": "float", "v": value}
    if isinstance(value, decimal.Decimal):
        return {"t": "decimal", "v": str(value)}
    if isinstance(value, datetime.datetime):
        return {"t": "datetime", "v": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"t": "date", "v": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {"t": "bytes", "v": bytes(value).hex()}
    return {"t": "str", "v": str(value)}


def _decode(data: Optional[Dict[str, Any]]) -> Any:
    if data is None:
        return None
    t, v = data["t"], data["v"]
    if t == "decimal":
        return decimal.Decimal(v)
    if t == "datetime":
        return datetime.datetime.fromisoformat(v)
    if t == "date":
        return datetime.date.fromisoformat(v)
    if t == "bytes":
        return bytes.fromhex(v)
    return v


def _value(stream: Any) -> Any:
    """The value of a one-row table ``v`` from the Rust core."""
    import pyarrow as pa

    return pa.table(stream).column(0)[0].as_py()


# --- Delta tables through Spark ----------------------------------------------------------


class _Delta:
    """A Delta table given by path (contains ``/`` or ``:``) or by name."""

    def __init__(self, spark: Any, location: str):
        self.spark = spark
        self.location = location
        self.is_path = "/" in location or ":" in location

    @property
    def ref(self) -> str:
        if self.is_path:
            return "delta.`" + self.location.replace("`", "``") + "`"
        return self.location

    def history(self, limit: int = _HISTORY) -> List[Dict[str, Any]]:
        """The newest commits (version, operation, userMetadata), newest first; [] if the table
        does not exist. One small Spark job, no scan of the table."""
        from pyspark.errors import AnalysisException

        if not self.is_path and not self.spark.catalog.tableExists(self.location):
            return []
        try:
            rows = self.spark.sql(
                f"DESCRIBE HISTORY {self.ref} LIMIT {int(limit)}"
            ).collect()
        except AnalysisException as e:
            if self.is_path and _is_missing_table(e):
                return []
            raise
        return [
            {
                "version": int(r["version"]),
                "operation": r["operation"],
                "userMetadata": r["userMetadata"],
                "rows": _output_rows(r),
            }
            for r in rows
        ]

    def version(self) -> Optional[int]:
        rows = self.history(1)
        return rows[0]["version"] if rows else None

    def read(self, version: Optional[int] = None) -> Any:
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        return (
            reader.load(self.location) if self.is_path else reader.table(self.location)
        )

    def overwrite(self, df: Any, user_metadata: Optional[str] = None) -> None:
        self._write(
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true"),
            user_metadata,
        )

    def append(self, df: Any, user_metadata: Optional[str] = None) -> None:
        self._write(df.write.format("delta").mode("append"), user_metadata)

    def _write(self, writer: Any, user_metadata: Optional[str]) -> None:
        if user_metadata is not None:
            writer = writer.option("userMetadata", user_metadata)
        if self.is_path:
            writer.save(self.location)
        else:
            writer.saveAsTable(self.location)


def _output_rows(row: Any) -> Optional[int]:
    """The rows a commit wrote (``numOutputRows`` of its operation metrics), None if unknown."""
    try:
        value = (row["operationMetrics"] or {}).get("numOutputRows")
        return int(value) if value is not None else None
    except (KeyError, TypeError, ValueError):
        return None


def _is_missing_table(e: Exception) -> bool:
    text = str(e).lower()
    return any(
        s in text
        for s in (
            "is not a delta table",
            "doesn't exist",
            "does not exist",
            "path_not_found",
            "delta_missing_delta_table",
        )
    )


class _State:
    def __init__(self, spark: Any, location: str):
        def table(name: str) -> _Delta:
            sep = "/" if ("/" in location or ":" in location) else "_"
            return _Delta(spark, f"{location.rstrip('/')}{sep}{name}")

        self.latest = table("latest_pk_version")
        # the newest version of latest_pk_version as meta() saw it (None: no table)
        self.version: Optional[int] = None

    def meta(self) -> Optional[Tuple[int, Dict[str, Any]]]:
        """(version of latest_pk_version, load state) of the last commit, if no other write
        changed latest_pk_version since."""
        history = self.latest.history()
        self.version = history[0]["version"] if history else None
        for row in history:
            meta = _parse_meta(row["userMetadata"])
            if meta is not None:
                return row["version"], meta
            if row["operation"] not in _SAME_ROWS:
                return None
        return None


def _parse_meta(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        data = json.loads(text)
    except ValueError:
        return None
    if isinstance(data, dict) and data.get(_META) == STATE_FORMAT:
        return data
    return None


def _same_rows_since(history: List[Dict[str, Any]], version: int) -> bool:
    """True if the commits after ``version`` (newest first in ``history``) kept the rows."""
    return _writes_since(history, version) == 0


def _writes_since(history: List[Dict[str, Any]], version: int) -> Optional[int]:
    """How many commits after ``version`` (newest first in ``history``) changed rows; None if
    ``history`` does not reach back to ``version``."""
    writes = 0
    for row in history:
        if row["version"] == version:
            return writes
        if row["version"] < version:
            return None
        if row["operation"] not in _SAME_ROWS and not _is_migration(row):
            writes += 1
    if version < 0 and history and history[-1]["version"] == 0:
        return writes  # ``version`` -1: before the table existed
    return None


def _is_migration(row: Dict[str, Any]) -> bool:
    """A commit of ``_Planner._migrate``: the same rows, wider column types."""
    try:
        data = json.loads(row.get("userMetadata") or "")
    except ValueError:
        return False
    return isinstance(data, dict) and _MIGRATION in data


def _quoted(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _default_state(target: str) -> str:
    return target.rstrip("/") + "_state"


def _remember(location: str, version: int, table: Any) -> None:
    with _previous_latest_lock:
        for key in [k for k in _previous_latest if k[0] == location]:
            del _previous_latest[key]
        _previous_latest[(location, version)] = table
        while len(_previous_latest) > _state_cache_size():
            _previous_latest.popitem(last=False)


def _cached(location: str, version: int) -> Any:
    """The remembered key state of ``location`` at ``version`` (now the most recently used), or
    None."""
    with _previous_latest_lock:
        table = _previous_latest.get((location, version))
        if table is not None:
            _previous_latest.move_to_end((location, version))
        return table


def _forget(location: str, version: int) -> None:
    with _previous_latest_lock:
        _previous_latest.pop((location, version), None)


def _to_arrow(df: Any) -> Any:
    """``df.toArrow()``, planned first in this thread. Classic PySpark runs the whole action of
    ``toArrow()``, query planning included, in a JVM thread that serves the result over a socket;
    a fatal JVM error there (e.g. NoSuchMethodError when delta-spark was built for another Spark
    version and reads a checkpoint while planning) ends that thread silently and ``toArrow()``
    waits forever. Planned here, the same error is raised. The plan is kept, so it costs no
    second planning or Spark job. Spark Connect has no ``_jdf`` and reports errors itself."""
    jdf = getattr(df, "_jdf", None)
    if jdf is not None:
        try:
            jdf.queryExecution().executedPlan()
        except Exception as e:
            if "NoSuchMethodError" in str(e):
                raise RuntimeError(
                    f"reading a Delta table failed inside Spark ({_versions()}): delta-spark "
                    "does not match the Spark version, install the delta-spark release built "
                    "for it (delta-spark 4.1.0 needs pyspark 4.1.0)"
                ) from e
            raise
    return df.toArrow()


def _versions() -> str:
    from importlib.metadata import PackageNotFoundError, version

    found = []
    for name in ("pyspark", "delta-spark"):
        try:
            found.append(f"{name} {version(name)}")
        except PackageNotFoundError:
            pass
    return ", ".join(found) or "versions unknown"


def _read_latest(state: _State, version: int, keys: List[str], delta: str) -> Any:
    import pyarrow as pa

    from sql2deltalake import _core

    key = (state.latest.location, version)
    table = _cached(*key)
    if table is None:
        df = state.latest.read(version)
        if _SEQ in df.columns:
            table = pa.table(_core.replay_state(_to_arrow(df), keys, delta, _SEQ, IS_DELETED))
        else:
            table = _to_arrow(df.select(*keys, delta))
        _remember(key[0], version, table)
    return table


def _state_schema(key_schema: Any) -> Any:
    """The Spark schema of latest_pk_version: keys and delta column (nullable), ``__state_seq``,
    ``__is_deleted``."""
    from pyspark.sql.types import BooleanType, LongType, StructField, StructType

    return StructType(
        [StructField(f.name, f.dataType, True) for f in key_schema.fields]
        + [StructField(_SEQ, LongType(), True), StructField(IS_DELETED, BooleanType(), True)]
    )


def _state_rows(table: Any, seq: int, deleted: Any = False) -> Any:
    """``table`` (keys and delta column, or with ``__is_deleted``) as rows of latest_pk_version."""
    import pyarrow as pa

    n = table.num_rows
    flags = (
        table.column(IS_DELETED)
        if IS_DELETED in table.column_names
        else pa.repeat(pa.scalar(deleted, pa.bool_()), n)
    )
    columns = [c for c in table.column_names if c != IS_DELETED]
    return pa.table(
        [*(table.column(c) for c in columns), pa.repeat(pa.scalar(seq, pa.int64()), n), flags],
        names=[*columns, _SEQ, IS_DELETED],
    )


def _state_df(spark: Any, table: Any, seq: int, schema: Any) -> Any:
    """``table`` as a DataFrame of latest_pk_version rows. createDataFrame makes one partition
    per Arrow batch (spark.sql.execution.arrow.maxRecordsPerBatch, 10,000 rows by default), so
    5M keys would be written as 500 files: coalesce to files of _STATE_FILE_ROWS rows."""
    rows = _state_rows(table, seq)
    files = max(1, -(-rows.num_rows // _STATE_FILE_ROWS))
    return spark.createDataFrame(rows, schema=schema).coalesce(files)


def _lit(value: Any, data_type: Any) -> Any:
    """A literal of ``data_type``; a zone-less timestamp is given as text so no time zone applies."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampNTZType

    if isinstance(data_type, TimestampNTZType) and isinstance(value, datetime.datetime):
        return F.lit(value.isoformat(sep=" ")).cast(data_type)
    return F.lit(value).cast(data_type)


def _latest_per_key(df: Any, keys: List[str], delta: str) -> Any:
    """The latest row per key: by __timestamp, ties (tables written before every pass of a load
    had its own __timestamp) broken by the delta column."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy(*keys).orderBy(
        F.col(TIMESTAMP).desc_nulls_last(), F.col(delta).desc_nulls_last()
    )
    return (
        df.withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def _rebuild_latest(
    target_df: Any, keys: List[str], delta: str, last_full: Any
) -> Optional[Any]:
    """latest_pk_version from the target alone: the full load at ``last_full`` (the
    ``__timestamp`` of the last one) plus everything after it."""
    from pyspark.sql import functions as F

    if last_full is None:
        return None
    since = target_df.filter(
        F.col(TIMESTAMP) >= _lit(last_full, target_df.schema[TIMESTAMP].dataType)
    )
    latest = _latest_per_key(
        since.select(*keys, delta, TIMESTAMP, IS_DELETED), keys, delta
    )
    return latest.filter(~F.col(IS_DELETED)).select(*keys, delta)


# --- the key state in Spark (see _key_state) ---------------------------------------------


def _one_row(df: Any) -> Dict[str, Any]:
    """The values of a one-row DataFrame (an aggregate), converted as ``toArrow`` converts them,
    like the values the driver path reads from Arrow tables."""
    table = _to_arrow(df)
    return {name: table.column(name)[0].as_py() for name in table.column_names}


def _anti(left: Any, right: Any, columns: List[str]) -> Any:
    """The rows of ``left`` without a row of ``right`` equal on ``columns`` (NULL equals NULL)."""
    from functools import reduce

    from pyspark.sql import functions as F

    other = right.select(*[F.col(_quoted(c)).alias(f"__r{i}") for i, c in enumerate(columns)])
    on = reduce(
        lambda a, b: a & b,
        [F.col(_quoted(c)).eqNullSafe(F.col(f"__r{i}")) for i, c in enumerate(columns)],
    )
    return left.join(other, on, "left_anti")


def _last_per_key(df: Any, keys: List[str], order: List[Any], columns: List[str]) -> Any:
    """Per key the ``columns`` of the row with the largest ``order`` (compared left to right,
    NULL below every value): a hash aggregate, where a window would sort every row."""
    from pyspark.sql import functions as F

    picked = [c for c in columns if c not in keys]
    winner = F.max_by(
        F.struct(*[F.col(_quoted(c)).alias(c) for c in picked]),
        F.struct(*[o.alias(f"__o{i}") for i, o in enumerate(order)]),
    )
    return df.groupBy(*[F.col(_quoted(k)) for k in keys]).agg(winner.alias("__w")).select(
        *[F.col(_quoted(k)) for k in keys], *[F.col("__w").getField(c).alias(c) for c in picked]
    )


def _replay_df(df: Any, keys: List[str], delta: str) -> Any:
    """``replay_state`` in Spark: latest_pk_version from its append-only form. Per key the row of
    the highest ``__state_seq`` above 0 wins, a key whose last change is deleted is gone, rows of
    ``__state_seq`` 0 whose key no later load touched are kept as they are."""
    from pyspark.sql import functions as F

    names = [*keys, delta]
    if _SEQ not in df.columns:
        return df.select(*names)
    is_base = F.coalesce(F.col(_SEQ) == 0, F.lit(False))
    last = _last_per_key(df.filter(~is_base), keys, [F.col(_SEQ)], [delta, IS_DELETED])
    current = last.filter(~F.coalesce(F.col(IS_DELETED), F.lit(False))).select(*names)
    return _anti(df.filter(is_base).select(*names), last, keys).unionByName(current)


def _winners(loaded: Any, keys: List[str], delta: str) -> Any:
    """Per key the loaded row that wins (keys, delta value, ``__is_deleted``, ``__timestamp``):
    the latest ``__timestamp``; at the same ``__timestamp`` an update beats a delete marker,
    then the larger delta value."""
    from pyspark.sql import functions as F

    deleted = F.coalesce(F.col(IS_DELETED), F.lit(False))
    return _last_per_key(
        loaded.withColumn(IS_DELETED, deleted),
        keys,
        [F.col(TIMESTAMP), ~F.col(IS_DELETED), F.col(_quoted(delta))],
        [delta, IS_DELETED, TIMESTAMP],
    )


def _merge_df(previous: Any, winners: Any, keys: List[str], delta: str) -> Tuple[Any, Any]:
    """``merge_latest`` in Spark, from the ``_winners`` of the loaded rows: the next
    latest_pk_version and the keys the load touched (with ``__is_deleted``, and their row's
    ``__timestamp``)."""
    from pyspark.sql import functions as F

    deleted = F.col(IS_DELETED)
    names = [*keys, delta]
    changes = winners.select(*names, deleted.alias(IS_DELETED), TIMESTAMP)
    latest = winners.filter(~deleted).select(*names)
    if previous is None:
        return latest, changes
    types = {f.name: f.dataType for f in previous.schema.fields}
    latest = latest.select(*[F.col(_quoted(n)).cast(types[n]).alias(n) for n in names])
    return _anti(previous, winners, keys).unionByName(latest), changes


def _persisted(df: Any) -> Any:
    """``df`` kept by the executors once computed (memory, then disk); as it is where persisting
    is not supported (serverless compute)."""
    from pyspark import StorageLevel

    try:
        return df.persist(StorageLevel.MEMORY_AND_DISK)
    except Exception as e:
        logger.info(f"not persisting a DataFrame: {e}")
        return df


def _unpersist(df: Any) -> None:
    try:
        df.unpersist()
    except Exception as e:
        logger.info(f"unpersist failed: {e}")


def _state_frame(df: Any, seq: int, schema: Any) -> Any:
    """``df`` (keys and delta column, or with ``__is_deleted``) as rows of latest_pk_version of
    ``schema``."""
    from pyspark.sql import functions as F

    flag = F.col(IS_DELETED) if IS_DELETED in df.columns else F.lit(False)
    columns = {f.name: F.col(_quoted(f.name)) for f in schema.fields if f.name in df.columns}
    columns[_SEQ] = F.lit(seq)
    columns[IS_DELETED] = flag
    return df.select(*[columns[f.name].cast(f.dataType).alias(f.name) for f in schema.fields])


# --- prepare ------------------------------------------------------------------------------


def prepare(
    spark: Any,
    *,
    get_target_name: Optional[Callable[[InformationSchemaColInfo], str]] = None,
    **options: Any,
) -> Dict[str, str]:
    """Decide what the next load of ``target`` reads and return the options for
    ``spark.read.format("sql2deltalake")``.

    Takes the data source options (``url``, ``table`` or ``query``, ``target``, ``keys``,
    ``deltaColumn``, ``loadMode``, ``numPartitions``, ...). Reads the history of the target and
    the state through Spark, and compares the source's primary keys and delta column with the
    state, on the driver or in Spark by the size of the table (see ``_key_state``).
    Writes only the state, and the target only to widen a column whose type the source widened
    (the same rows, see ``_Planner._reconcile_types``).
    """
    opts = LoadOptions.parse(options)
    if not opts.target:
        raise ValueError(
            "prepare() needs the option 'target' (Delta table path or name)"
        )
    register(spark)
    load = SourceLoad(opts, get_target_name)
    plan = _Planner(spark, opts, load).plan()
    logger.info(f"sql2deltalake plan for {opts.target}: {plan.kind} {plan.info}")
    return {"url": opts.url, "plan": plan.encode()}


def run(
    spark: Any,
    *,
    get_target_name: Optional[Callable[[InformationSchemaColInfo], str]] = None,
    **options: Any,
) -> str:
    """One load: ``prepare``, append the rows to ``target`` and ``commit``. An unchanged
    table writes nothing. Returns the plan kind."""
    target = LoadOptions.parse(options).target
    load_options = prepare(spark, get_target_name=get_target_name, **options)
    plan = Plan.decode(load_options["plan"])
    if plan.kind == "unchanged":
        return plan.kind
    df = spark.read.format(FORMAT).options(**load_options).load()
    writer = df.write.format("delta").mode("append")
    if plan.info.get("merge_schema"):
        writer = writer.option("mergeSchema", "true")
    assert target is not None
    if _Delta(spark, target).is_path:
        writer.save(target)
    else:
        writer.saveAsTable(target)
    commit(spark, load_options)
    return plan.kind


class _Planner:
    def __init__(self, spark: Any, opts: LoadOptions, load: SourceLoad):
        self.spark = spark
        self.opts = opts
        self.load = load
        assert opts.target is not None
        self.target = _Delta(spark, opts.target)
        self.state_location = opts.state or _default_state(opts.target)
        self.state = _State(spark, self.state_location)
        self.target_version: Optional[int] = None
        self.meta: Optional[Dict[str, Any]] = None
        self.meta_version: Optional[int] = None
        self.mar: Optional[int] = None
        self._target_df: Any = None
        self._target_max_ts: Any = ()
        self._stats: Optional[Dict[str, Any]] = None
        # source columns whose values are cast to the (wider) type of the target column
        self._casts: Dict[str, Any] = {}
        # decided by the size of the state once its meta is read (see _decide)
        self.in_spark = True

    # --- the target, read only where the state cannot answer ---------------------------------

    @property
    def target_df(self) -> Any:
        if self._target_df is None:
            self._target_df = self.target.read(self.target_version)
        return self._target_df

    @property
    def target_max_ts(self) -> Optional[datetime.datetime]:
        if self._target_max_ts == ():
            if self.target_version is None:
                self._target_max_ts = None
            elif self.meta is not None:
                self._target_max_ts = _decode(self.meta.get("max_timestamp"))
            else:
                self._target_max_ts = self._target_stats().get("max_ts")
        return self._target_max_ts  # type: ignore[return-value]

    def _target_stats(self, last_full: bool = False) -> Dict[str, Any]:
        """MAX(__timestamp), MAX(delta column) and, with ``last_full``, the ``__timestamp`` of
        the last full load of the target: one Spark job for all that a plan needs of them."""
        from pyspark.sql import functions as F

        if self._stats is not None and (not last_full or "last_full" in self._stats):
            return self._stats
        columns = self.target_df.columns
        delta = self.load.delta_name
        aggs = []
        if TIMESTAMP in columns:
            aggs.append(F.max(TIMESTAMP).alias("max_ts"))
            if last_full and IS_FULL_LOAD in columns:
                aggs.append(
                    F.max(F.when(F.col(IS_FULL_LOAD), F.col(TIMESTAMP))).alias("last_full")
                )
        if delta is not None and delta in columns:
            aggs.append(F.max(delta).alias("max_delta"))
        first = self.target_df.agg(*aggs).first() if aggs else None
        stats: Dict[str, Any] = first.asDict() if first is not None else {}
        if last_full:
            stats.setdefault("last_full", None)
        self._stats = stats
        return stats

    # --- decisions -----------------------------------------------------------------------

    def plan(self) -> Plan:
        plan = self._decide()
        if plan.commit is not None:
            plan.info["key_state"] = plan.commit["key_state"]
        if self._casts:
            from pyspark.sql.types import StructField, StructType

            schema = StructType.fromJson(json.loads(plan.schema))
            plan.schema = StructType(
                [
                    StructField(f.name, self._casts.get(f.name, f.dataType), f.nullable)
                    for f in schema.fields
                ]
            ).json()
        return plan

    def _decide(self) -> Plan:
        load, opts = self.load, self.opts
        infos = load.infos
        history = self.target.history()
        self.target_version = history[0]["version"] if history else None
        state = self.state.meta() if history else None
        if state is not None and _same_rows_since(
            history, state[1].get("target_version", -1)
        ):
            self.meta_version, self.meta = state
        self.in_spark = (
            _key_state(self.meta.get("latest_count") if self.meta is not None else None) == "spark"
        )
        # The clock and, before the load reads any data, MAX/COUNT for a plan that compares them
        # with the state (the no-updates shortcut, simple_delta_check) and MIN_ACTIVE_ROWVERSION()
        # for a commit that stores it (the shortcut of the next load). A first load needs no
        # MAX/COUNT: nothing to compare with.
        counts = (
            self.target_version is not None
            and opts.load_mode not in ("force_full", "append_inserts")
            and (self.meta is not None or opts.load_mode == "simple_delta_check")
        )
        commits = bool(infos.pk_cols) and opts.load_mode != "append_inserts"
        self.source = load.source_state(counts=counts, mar=commits)
        self.mar = self.source["mar"]
        if self.target_version is None:
            return self._full("the target does not exist")
        migrated = self._reconcile_types()

        if opts.load_mode == "append_inserts":
            if (
                infos.delta_col is None
                and len(infos.pk_cols) == 1
                and infos.pk_cols[0].is_identity
            ):
                load.use_delta_column(
                    infos.pk_cols[0]
                )  # identity columns are usually increasing
            if load.delta_name is None:
                raise ValueError("load_mode append_inserts needs a delta column")
            return self._append_inserts(self._target_stats().get("max_delta"))
        if (
            infos.delta_col is None
            or not infos.pk_cols
            or opts.load_mode == "force_full"
        ):
            return self._full(
                "no delta column or no keys"
                if opts.load_mode != "force_full"
                else "force_full"
            )
        if self.meta is not None and self._unchanged(self.meta):
            return self._unchanged_plan(self.meta)
        columns = (
            self.meta["target_columns"]
            if self.meta is not None and self.meta.get("target_columns")
            else self.target_df.columns
        )
        existing = {c.lower() for c in columns}
        missing = [c for c in load.column_names if c.lower() not in existing]
        if missing:
            return self._full(
                f"new columns {missing}, append with mergeSchema", merge_schema=True
            )

        keys, delta = load.key_names, load.delta_name
        assert delta is not None
        widened = [c for c in migrated if c.lower() in {n.lower() for n in [*keys, delta]}]
        if widened:
            return self._full(f"the type of {widened} changed, the state has the old one")
        old = self._old_state(keys, delta)
        if old is None:
            return self._full("no state and no full load in the target")
        if not {k.lower() for k in keys}.issubset({c.lower() for c in old["columns"]}):
            return self._full("the state has other primary keys")
        # a rebuilt state: the delta values since the last full load, not MAX over the whole
        # (append-only) target, whose history can be ahead of what the source has now
        watermark = _decode(old["meta"]["watermark"]) if old["valid"] else old["max"]
        if watermark is None:
            return self._full("no delta value in the target")
        simple = opts.load_mode in ("simple_delta", "simple_delta_check")
        if opts.load_mode == "simple_delta_check" and not self._simple_counts_match(
            old, watermark
        ):
            logger.warning(
                "simple_delta_check: counts do not match, doing a delta load"
            )
            simple = False
        if simple:
            return self._simple(watermark, old)
        return self._delta(watermark, old)

    def _reconcile_types(self) -> List[str]:
        """The rule of ``SparkReader`` for a source column whose type differs from the target's
        (checked only when the source schema changed since the last commit): if the target
        column holds every value of the new type (int in the source, bigint in the target; a
        string column), the data source casts the values to it; if the new type holds every
        value of the target column (int to bigint, decimal(18,2) to decimal(20,4)), the target
        column is migrated to it; otherwise the load fails before anything is written. Returns
        the migrated columns."""
        from pyspark.sql.types import StructType

        from sql2deltalake.reader.spark_reader import holds_all_values

        if self._known_schema() is not None:
            return []
        source = StructType.fromJson(json.loads(self.load.result_schema()))
        target = {f.name.lower(): f for f in self.target_df.schema.fields}
        migrate: Dict[str, Any] = {}
        for sf in source.fields:
            tf = target.get(sf.name.lower())
            if tf is None or tf.dataType == sf.dataType:
                continue
            if holds_all_values(tf.dataType, sf.dataType):
                self._casts[sf.name] = tf.dataType
            elif holds_all_values(sf.dataType, tf.dataType):
                migrate[tf.name] = sf.dataType
            else:
                raise ValueError(
                    f"The type of column {tf.name} changed from {tf.dataType.simpleString()} to "
                    f"{sf.dataType.simpleString()} in the source, and neither type holds all "
                    "values of the other: converting the column would lose data. Convert the "
                    f"column of {self.opts.target} yourself, or load into a new target."
                )
        if migrate:
            self._migrate(migrate)
        return list(migrate)

    def _migrate(self, columns: Dict[str, Any]) -> None:
        """Rewrites the target with ``columns`` cast to their new (wider) types: the same rows,
        so the commit is marked as a migration and the state stays valid."""
        from pyspark.sql import functions as F

        df = self.target_df
        types = {name: t.simpleString() for name, t in columns.items()}
        logger.warning(f"sql2deltalake: migrating columns of {self.opts.target} to {types}")
        detail = self.spark.sql(f"DESCRIBE DETAIL {self.target.ref}").first()
        parts = list(detail["partitionColumns"] or []) if detail is not None else []
        writer = (
            df.select(
                *[
                    F.col(_quoted(f.name)).cast(columns[f.name]).alias(f.name)
                    if f.name in columns
                    else F.col(_quoted(f.name))
                    for f in df.schema.fields
                ]
            )
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("userMetadata", json.dumps({_MIGRATION: types}))
        )
        if parts:
            writer = writer.partitionBy(*parts)
        if self.target.is_path:
            writer.save(self.target.location)
        else:
            writer.saveAsTable(self.target.location)
        self.target_version = self.target.version()
        self._target_df = None
        self._stats = None

    def _unchanged(self, meta: Dict[str, Any]) -> bool:
        """The no-updates shortcut: the source has the MAX/COUNT the last load committed, the same
        schema and key/delta columns, and for rowversion every rowversion up to MAX was committed
        when that load ran. Never for a delta column whose MAX cannot tell (see
        ``_shortcut_allowed``). Any error here only means no shortcut."""
        try:
            infos = self.load.infos
            if not _shortcut_allowed(infos):
                return False
            if meta.get("source_schema") != _source_schema_fingerprint(infos):
                return False
            if meta.get("keys") != self.load.key_names or meta.get(
                "delta_column"
            ) != (self.load.delta_name):
                return False
            local_max = _comparable_ts(_decode(meta.get("latest_max")))
            source_max = self.source["max"]
            if source_max is None or local_max is None:
                return False
            if (local_max, meta.get("latest_count")) != (
                _comparable_ts(source_max),
                self.source["count"],
            ):
                return False
            if _is_rowversion(infos.delta_col, infos.write_config.dialect):
                bound = meta.get("min_active_rowversion")
                return bound is not None and local_max < bound
            return True
        except Exception as e:
            logger.warning(f"no-updates check failed, loading: {e}")
            return False

    def _old_state(self, keys: List[str], delta: str) -> Optional[Dict[str, Any]]:
        """The state as of the last committed load, or rebuilt from the target if it does not
        belong to the target as it is now."""
        import pyarrow.compute as pc

        if self.meta is not None:
            return {
                "version": self.meta_version,
                "valid": True,
                "meta": self.meta,
                "columns": [*self.meta["keys"], self.meta["delta_column"]],
            }
        target_df = self.target_df
        if not all(c in target_df.columns for c in [*keys, delta]):
            return None
        rebuilt = _rebuild_latest(
            target_df, keys, delta, self._target_stats(last_full=True)["last_full"]
        )
        if rebuilt is None:
            return None
        logger.info(
            "state does not match the target, rebuilt latest_pk_version from the target"
        )
        if self.in_spark:
            from pyspark.sql import functions as F

            self.state.latest.overwrite(_state_frame(rebuilt, 0, _state_schema(rebuilt.schema)))
            version = self.state.latest.version()
            assert version is not None
            self.state.version = version
            written = self.state.latest.read(version)
            return {
                "version": version,
                "valid": False,
                "meta": None,
                "columns": list(rebuilt.columns),
                "max": _one_row(written.agg(F.max(_quoted(delta)).alias("max")))["max"],
            }
        table = _to_arrow(rebuilt)
        self.state.latest.overwrite(
            _state_df(self.spark, table, 0, _state_schema(rebuilt.schema))
        )
        version = self.state.latest.version()
        assert version is not None
        self.state.version = version
        _remember(self.state.latest.location, version, table)
        return {
            "version": version,
            "valid": False,
            "meta": None,
            "columns": list(table.column_names),
            "max": pc.max(table.column(delta)).as_py() if table.num_rows else None,  # type: ignore[attr-defined]
        }

    def _warn_trimmed_keys(self, snapshot: Any) -> None:
        """Strings are trimmed (both ends) unless ``noTrim``: string keys that differ only by
        leading blanks, distinct in SQL Server, are one key in Delta. Warns if the snapshot has
        such keys; a hash pass over the keys, only for string keys."""
        import pyarrow as pa

        keys = self.load.key_names
        if self.opts.no_trim or not snapshot.num_rows:
            return
        types = [snapshot.schema.field(k).type for k in keys]
        if not any(pa.types.is_string(t) or pa.types.is_large_string(t) for t in types):
            return
        distinct = snapshot.select(keys).group_by(keys).aggregate([]).num_rows
        self._warn_repeated_keys(snapshot.num_rows - distinct)

    def _warn_repeated_keys(self, repeated: int) -> None:
        if repeated > 0:
            logger.warning(
                f"sql2deltalake: {repeated} keys of {self.opts.target} "
                "differ from another key only by leading blanks; trimmed, they are the same "
                "key in Delta. Load with noTrim=true to keep them apart (then the target "
                "needs a full load)."
            )

    def _previous(self, old: Dict[str, Any]) -> Any:
        """The key state as of ``old``: a DataFrame in Spark, else an Arrow table."""
        delta = self.load.delta_name
        assert delta is not None
        if self.in_spark:
            return _replay_df(self.state.latest.read(old["version"]), self.load.key_names, delta)
        return _read_latest(self.state, old["version"], self.load.key_names, delta)

    def _read_keys(self, sql: str) -> Any:
        """The keys and delta values ``sql`` selects, as a DataFrame read by the executors in
        slices through the data source (an internal "rows" plan): they never pass the driver."""
        from pyspark.sql.types import StructType

        load = self.load
        names = [*load.key_names, load.delta_name]
        known = self._known_schema()
        if known is not None:
            fields = {f.name: f for f in StructType.fromJson(json.loads(known[0])).fields}
            if all(n in fields for n in names):
                scales = {n: s for n, s in known[1].items() if n in names}
                known = (StructType([fields[n] for n in names]).json(), scales)
            else:
                known = None
        unit = Unit(sql=sql, split=load.split(names))
        plan = load.plan("rows", [unit], sql, known=known)
        return (
            self.spark.read.format(FORMAT)
            .options(url=self.opts.url, plan=plan.encode())
            .load()
        )

    def _simple_counts_match(self, old: Dict[str, Any], watermark: Any) -> bool:
        from sql2deltalake import _core

        previous = self._previous(old)
        sql = self.load.pk_sql(newer_than=watermark)
        if self.in_spark:
            new_keys = _anti(self._read_keys(sql), previous, self.load.key_names).count()
            old_count = old["meta"]["latest_count"] if old["valid"] else previous.count()
        else:
            changed = self.load.read_rows(sql)
            new_keys = _core.count_new_keys(changed, previous, self.load.key_names)
            old_count = (
                old["meta"]["latest_count"] if old["valid"] else previous.num_rows
            )
        return old_count + new_keys == self.source["count"]

    # --- plans ---------------------------------------------------------------------------

    def _known_schema(self) -> Optional[Tuple[str, Dict[str, int]]]:
        meta = self.meta
        if (
            meta is not None
            and meta.get("schema")
            and meta.get("source_schema") == _source_schema_fingerprint(self.load.infos)
        ):
            return meta["schema"], meta.get("time_scales") or {}
        return None

    def _diff_in_spark(self, previous: Any, watermark: Any) -> Dict[str, Any]:
        """``_core.pk_diff`` in Spark: the source's keys, read by the executors, against the state.
        The driver gets the counts, the two bounds, and only the keys a plan carries: the deletes
        up to maxInlineDeletes, the strange updates re-read by key. The count of keys that
        trimming made equal (``_warn_trimmed_keys``) comes from the same pass."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        keys, delta = self.load.key_names, self.load.delta_name
        assert delta is not None
        snapshot = _persisted(self._read_keys(self.load.pk_sql()))  # the source is read once
        per_key = None
        try:
            types = {f.name: f.dataType for f in snapshot.schema.fields}
            # the state's types are cast to the snapshot's, as pk_diff does
            prev = previous.select(
                *[F.col(_quoted(n)).cast(types[n]).alias(n) for n in [*keys, delta]]
            )
            value = F.col(_quoted(delta))
            strange_value = value.isNull() | (value <= _lit(watermark, types[delta]))
            # one pass for all counts: per key and delta value whether the snapshot and the state
            # have it, then per key; GROUP BY puts NULL with NULL as the anti-joins below do
            tagged = snapshot.select(*keys, delta, F.lit(1).alias("__s"), F.lit(0).alias("__p")).unionByName(
                prev.select(*keys, delta, F.lit(0).alias("__s"), F.lit(1).alias("__p"))
            )
            rows = tagged.groupBy(*keys, delta).agg(
                F.sum("__s").alias("__s"), F.max("__p").alias("__p")
            )
            changed = (F.col("__s") > 0) & (F.col("__p") == 0)
            # kept: the deletes come from it too, without replaying the state again
            per_key = _persisted(rows.groupBy(*keys).agg(
                F.max("__s").alias("__s"),
                F.max("__p").alias("__p"),
                F.sum(F.when(changed & strange_value, F.col("__s")).otherwise(0)).alias("__n"),
                F.min(F.when(changed & strange_value, value)).alias("__min"),
                F.max(F.when(F.col("__s") > 0, value)).alias("__max"),
                F.sum("__s").alias("__ns"),
            ))
            counts = _one_row(
                per_key.agg(
                    F.sum(F.when((F.col("__p") > 0) & (F.col("__s") == 0), 1).otherwise(0)).alias("deletes"),
                    F.sum("__n").alias("n"),
                    F.min("__min").alias("min"),
                    F.max("__max").alias("max"),
                    F.sum(F.when(F.col("__ns") > 1, F.col("__ns") - 1).otherwise(0)).alias("repeated"),
                )
            )
            if not self.opts.no_trim and any(isinstance(types[k], StringType) for k in keys):
                self._warn_repeated_keys(counts["repeated"] or 0)
            n_deletes, n_strange = counts["deletes"] or 0, counts["n"] or 0
            # the keys a plan carries: exact rows of the state and snapshot, as pk_diff gives them
            # (the state has each key once, so its keys without a snapshot row are per_key's)
            deletes = per_key.filter((F.col("__p") > 0) & (F.col("__s") == 0)).select(*keys)
            strange = _anti(snapshot, prev, [*keys, delta]).filter(strange_value)
            by_key = (
                n_strange <= _STRANGE_UPDATES_BY_KEY
                and not self.load.infos.write_config.no_complex_entries_load
            )
            return {
                "deletes": _to_arrow(deletes)
                if 0 < n_deletes <= self.opts.max_inline_deletes
                else None,
                "n_deletes": n_deletes,
                "strange": _to_arrow(strange.select(*keys)) if n_strange and by_key else None,
                "n_strange": n_strange,
                "strange_min": counts["min"],
                "snapshot_max": counts["max"],
            }
        finally:
            if per_key is not None:
                _unpersist(per_key)
            _unpersist(snapshot)

    def _plan(self, kind: str, units: List[Unit], sql: str, **kwargs: Any) -> Plan:
        return self.load.plan(kind, units, sql, known=self._known_schema(), **kwargs)

    def _commit_info(
        self, kind: str, timestamps: List[Any], **extra: Any
    ) -> Dict[str, Any]:
        return {
            "kind": kind,
            # names the state commit of this plan: a second commit() of it is a no-op
            "plan_id": uuid.uuid4().hex,
            "target": self.opts.target,
            "state": self.state_location,
            "target_version": -1
            if self.target_version is None
            else self.target_version,
            "state_version": self.state.version,
            # rows appended to latest_pk_version since its last rewrite
            "state_log_rows": (self.meta.get("log_rows") or 0)
            if self.meta is not None
            else 0,
            "timestamps": [str(t) for t in timestamps],
            "keys": self.load.key_names,
            "delta_column": self.load.delta_name,
            "source_schema": _source_schema_fingerprint(self.load.infos),
            "min_active_rowversion": self.mar,
            "max_timestamp": _encode(self.target_max_ts),
            "target_columns": self.meta.get("target_columns")
            if self.meta is not None
            else None,
            "key_state": "spark" if self.in_spark else "driver",
            # the keys of the state this load starts from (commit() adds the appended rows)
            "state_count": self.meta.get("latest_count") if self.meta is not None else None,
            **extra,
        }

    def _ts(self) -> Any:
        return self.load.next_timestamp(after=self.target_max_ts)

    def _ts_value(self, ts: Any) -> datetime.datetime:
        from sql2deltalake.db_to_delta import _load_timestamp_value

        value = _load_timestamp_value(ts)
        assert value is not None
        return value

    def _full(self, reason: str, merge_schema: bool = False) -> Plan:
        load = self.load
        ts = self._ts()
        sql = load.full_sql(ts)
        commit = None
        if load.key_names and load.delta_name:
            commit = self._commit_info("full", [self._ts_value(ts)])
        unit = Unit(sql=sql, split=load.split(load.column_names))
        info: Dict[str, Any] = {"reason": reason}
        if merge_schema:
            info["merge_schema"] = True
            return load.plan("full", [unit], sql, commit=commit, info=info)
        return self._plan("full", [unit], sql, commit=commit, info=info)

    def _append_inserts(self, watermark: Any) -> Plan:
        load = self.load
        ts = self._ts()
        criterion = load.criterion(watermark, ">") if watermark is not None else None
        sql = load.update_sql(ts, criterion)
        unit = Unit(sql=sql, split=load.split(load.column_names, updates=True))
        return self._plan(
            "append_inserts", [unit], sql, info={"watermark": str(watermark)}
        )

    def _unchanged_plan(self, meta: Dict[str, Any]) -> Plan:
        known = self._known_schema()
        sql = "" if known is not None else self.load.full_sql(self._ts())
        return self.load.plan(
            "unchanged", [], sql, known=known, info={"reason": "no updates"}
        )

    def _simple(self, watermark: Any, old: Dict[str, Any]) -> Plan:
        load = self.load
        ts = self._ts()
        sql = load.update_sql(ts, load.criterion(watermark, ">"))
        unit = Unit(sql=sql, split=load.split(load.column_names, updates=True))
        commit = self._commit_info(
            "simple", [self._ts_value(ts)], old_version=old["version"]
        )
        return self._plan(
            "simple_delta",
            [unit],
            sql,
            commit=commit,
            info={"watermark": str(watermark)},
        )

    def _delta(self, watermark: Any, old: Dict[str, Any]) -> Plan:
        import pyarrow as pa

        from sql2deltalake import _core

        load, opts = self.load, self.opts
        keys, delta = load.key_names, load.delta_name
        assert delta is not None
        # Delete markers get the first timestamp of the load: a key deleted and inserted again
        # while the load runs is then still current after it.
        ts_deletes, ts_updates = self._ts(), self._ts()

        previous = self._previous(old)
        if self.in_spark:
            diff = self._diff_in_spark(previous, watermark)
        else:
            snapshot = load.pk_snapshot()
            self._warn_trimmed_keys(snapshot)
            raw = _core.pk_diff(
                snapshot, previous, keys, delta, pa.table({"v": [watermark]})
            )
            diff = {
                "deletes": pa.table(raw["deletes"]),
                "strange": pa.table(raw["strange"]),
                "strange_min": _value(raw["strange_min"]),
                "snapshot_max": _value(raw["snapshot_max"]),
            }
            diff["n_deletes"] = diff["deletes"].num_rows
            diff["n_strange"] = diff["strange"].num_rows
        deletes, strange = diff["deletes"], diff["strange"]
        n_deletes, n_strange = diff["n_deletes"], diff["n_strange"]

        units: List[Unit] = []
        if n_deletes > opts.max_inline_deletes:
            return self._full(f"{n_deletes} deletes, more than maxInlineDeletes")
        if n_deletes:
            units.append(
                Unit(
                    rows=encode_rows(deletes),
                    timestamp=str(self._ts_value(ts_deletes)),
                )
            )
        sql = load.update_sql(ts_updates, load.criterion(watermark, ">"))
        units.append(
            Unit(sql=sql, split=load.split(load.column_names, updates=True))
        )
        timestamps = [self._ts_value(ts_updates)]
        info: Dict[str, Any] = {
            "watermark": str(watermark),
            "deletes": n_deletes,
            "strange_updates": n_strange,
            "state_rebuilt": not old["valid"],
        }
        if n_strange:
            ts_strange = (
                self._ts()
            )  # a later pass: re-read rows sort after the first read
            timestamps.append(self._ts_value(ts_strange))
            min_ts = diff["strange_min"]
            if (
                n_strange > _STRANGE_UPDATES_BY_KEY
                or load.infos.write_config.no_complex_entries_load
            ) and min_ts is not None:
                units.append(
                    Unit(
                        sql=load.update_sql(ts_strange, load.criterion(min_ts, ">=")),
                        split=load.split(load.column_names, updates=True),
                    )
                )
                info["strange_updates_from"] = str(min_ts)
            else:
                data = [
                    {f"p{i}": row[k] for i, k in enumerate(keys)}
                    for row in strange.select(keys).to_pylist()
                ]
                units.append(Unit(sql=load.keys_sql(data, ts_strange)))
        commit = self._commit_info(
            "delta",
            timestamps,
            old_version=old["version"],
            watermark=_encode(diff["snapshot_max"]),
            delete_timestamp=str(self._ts_value(ts_deletes)) if n_deletes else None,
        )
        return self._plan("delta", units, sql, commit=commit, info=info)


# --- commit ------------------------------------------------------------------------------


class StalePlanError(RuntimeError):
    """``commit()`` of a plan that another load overtook: committing it would overwrite the
    other load's key state."""


def _committed(c: Dict[str, Any], state_history: List[Dict[str, Any]]) -> bool:
    """Whether the newest load state in ``state_history`` is the one of this plan: a second
    ``commit()`` of it."""
    if not c.get("plan_id"):
        return False
    for row in state_history:
        meta = _parse_meta(row["userMetadata"])
        if meta is not None:
            return meta.get("plan_id") == c["plan_id"]
        if row["operation"] not in _SAME_ROWS:
            return False
    return False


def _check_not_stale(
    c: Dict[str, Any],
    history: List[Dict[str, Any]],
    state: _State,
    state_history: List[Dict[str, Any]],
) -> None:
    """The target has at most this load's append since ``prepare()``, and the state no other
    commit; otherwise the plan is stale. The state is then left as it is: it does not belong to
    the target any more, so the next ``prepare()`` rebuilds it from the target."""
    writes = _writes_since(history, c["target_version"])
    if writes is None or writes > 1:
        raise StalePlanError(
            f"{c['target']} changed since prepare() (version {c['target_version']}, now "
            f"{history[0]['version']}): another load ran in between, prepare() again"
        )
    state.version = state_history[0]["version"] if state_history else None
    if "state_version" not in c or c["target_version"] < 0:
        return  # a first load does not read the state
    if c["state_version"] is None:
        stale = bool(state_history)
    else:
        stale = _writes_since(state_history, c["state_version"]) != 0
    if stale:
        raise StalePlanError(
            f"the state of {c['target']} changed since prepare(): another load committed in "
            "between, prepare() again"
        )


def commit(spark: Any, options: Dict[str, str]) -> Optional[str]:
    """Advance the state after the DataFrame of ``options`` (from ``prepare``) was appended to
    the target. Returns the plan kind, or None if the plan has nothing to commit. Committing the
    same plan again is a no-op (returns the plan kind again), so a retry after an error is safe.

    The new state comes from the rows of this load found in the target (by their
    ``__timestamp``), so a commit without the append (or with a failed one) only keeps the old
    state for the keys of this load, and the next load reads them again."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyspark.errors import AnalysisException
    from pyspark.sql.types import StructType

    from sql2deltalake import _core

    plan = Plan.decode(normalize(options)["plan"])
    c = plan.commit
    if c is None:
        return None
    target = _Delta(spark, c["target"])
    history = target.history()
    if not history:
        raise RuntimeError(
            f"{c['target']} does not exist: append the DataFrame first, then commit()"
        )
    version = history[0]["version"]
    if version == c["target_version"] and c["kind"] == "full":
        return None  # nothing appended: the next load starts from the same state again
    state = _State(spark, c["state"])
    state_history = state.latest.history()
    if _committed(c, state_history):
        logger.info(f"commit(): this plan of {c['target']} is committed already")
        return plan.kind
    _check_not_stale(c, history, state, state_history)
    if _commit_key_state(c, history) == "spark":
        return _commit_in_spark(spark, plan, target, state, version)
    keys, delta = c["keys"], c["delta_column"]
    previous = (
        None
        if c["kind"] == "full"
        else _read_latest(state, c["old_version"], keys, delta)
    )
    target_columns = c.get("target_columns")
    if version != c["target_version"]:
        target_df = target.read(version)
        loaded = _to_arrow(_loaded_rows(c, target_df))
        target_columns = target_df.columns
        if c["kind"] == "full" and loaded.num_rows == 0:
            # another write, not this load's append: no state, the next load rebuilds it
            logger.warning(
                f"commit(): no rows of this full load in {c['target']}, the state is rebuilt by the next load"
            )
            return None
    else:
        assert previous is not None
        loaded = previous.slice(0, 0).append_column(
            TIMESTAMP, pa.array([], pa.timestamp("us"))
        ).append_column(IS_DELETED, pa.array([], pa.bool_()))
    merged = _core.merge_latest(previous, loaded, keys, delta)
    latest = pa.table(merged["latest"])
    changes = pa.table(merged["changes"])
    # append only this load's changes, unless that makes the appended rows too many
    log_rows = (c.get("state_log_rows") or 0) + changes.num_rows
    append = previous is not None and log_rows <= max(
        merged["count"] // _LOG_RATIO, _MIN_LOG_ROWS
    )
    latest_max = _value(merged["max"])
    max_ts = _decode(c.get("max_timestamp"))
    if loaded.num_rows:
        loaded_max = pc.max(loaded.column(TIMESTAMP)).as_py()  # type: ignore[attr-defined]
        if loaded_max is not None:
            loaded_max = loaded_max.replace(tzinfo=None)
            if max_ts is None or loaded_max > max_ts:
                max_ts = loaded_max
    meta = {
        _META: STATE_FORMAT,
        "plan_id": c.get("plan_id"),
        "target_version": version,
        "target_columns": target_columns,
        "keys": keys,
        "delta_column": delta,
        "source_schema": c["source_schema"],
        "min_active_rowversion": c["min_active_rowversion"],
        "latest_max": _encode(latest_max),
        "latest_count": merged["count"],
        "watermark": c["watermark"] if "watermark" in c else _encode(latest_max),
        "max_timestamp": _encode(max_ts),
        "schema": plan.schema,
        "time_scales": plan.time_scales,
        "log_rows": log_rows if append else 0,
    }
    schema = StructType.fromJson(json.loads(plan.schema))
    state_schema = _state_schema(StructType([schema[n] for n in [*keys, delta]]))
    # the Delta version this commit creates: later than every __state_seq before it
    seq = 0 if state.version is None else state.version + 1
    if append:
        try:
            state.latest.append(
                _state_df(spark, changes, seq, state_schema),
                user_metadata=json.dumps(meta, separators=(",", ":")),
            )
        except AnalysisException as e:  # e.g. key types of a state rebuilt from the target
            logger.warning(f"commit(): appending to the state failed, rewriting it: {e}")
            append = False
    if not append:
        meta["log_rows"] = 0
        state.latest.overwrite(
            _state_df(spark, latest, 0, state_schema),
            user_metadata=json.dumps(meta, separators=(",", ":")),
        )
    _forget(state.latest.location, c.get("old_version", -1))
    # The next prepare() in this process compares with this table instead of reading it back
    # (several Spark jobs). Its version follows the one _check_not_stale saw: had another
    # write come in between, this commit is newer than it, and prepare() takes the newest.
    _remember(state.latest.location, 0 if state.version is None else state.version + 1, latest)
    return plan.kind


def _commit_key_state(c: Dict[str, Any], history: List[Dict[str, Any]]) -> str:
    """``_key_state`` for a commit: the keys of the state it starts from (none for a full load)
    plus the rows this load appended, from the target's history (the rows its append wrote)."""
    appended: Optional[int] = 0
    for row in history:
        if row["version"] <= c["target_version"]:
            break
        if appended is not None:
            appended = None if row.get("rows") is None else appended + row["rows"]
    base = 0 if c["kind"] == "full" else c.get("state_count")
    keys = None if appended is None or base is None else base + appended
    return _key_state(keys)


def _loaded_rows(c: Dict[str, Any], target_df: Any) -> Any:
    """The keys, delta value, ``__timestamp`` and ``__is_deleted`` of the rows this load appended
    to the target (found by the ``__timestamp`` values of its plan)."""
    from pyspark.sql import functions as F

    ts_type = target_df.schema[TIMESTAMP].dataType

    def at(timestamps: List[str]) -> Any:
        return F.col(TIMESTAMP).isin(
            *[_lit(datetime.datetime.fromisoformat(t), ts_type) for t in timestamps]
        )

    rows = at(c["timestamps"]) & ~F.col(IS_DELETED)
    if c.get("delete_timestamp"):
        rows = rows | (at([c["delete_timestamp"]]) & F.col(IS_DELETED))
    return target_df.filter(rows).select(*c["keys"], c["delta_column"], TIMESTAMP, IS_DELETED)


def _commit_in_spark(spark: Any, plan: Plan, target: _Delta, state: _State, version: int) -> Optional[str]:
    """The rest of ``commit()`` with the key state in Spark DataFrames: the same state and meta as
    the driver path; only counts and maxima reach the driver."""
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, StructType, TimestampNTZType

    c = plan.commit
    assert c is not None
    keys, delta = c["keys"], c["delta_column"]
    schema = StructType.fromJson(json.loads(plan.schema))
    state_schema = _state_schema(StructType([schema[n] for n in [*keys, delta]]))
    previous = (
        None
        if c["kind"] == "full"
        else _replay_df(state.latest.read(c["old_version"]), keys, delta)
    )
    target_columns = c.get("target_columns")
    if version != c["target_version"]:
        target_df = target.read(version)
        loaded = _loaded_rows(c, target_df)
        target_columns = target_df.columns
    else:
        assert previous is not None
        loaded = previous.limit(0).select(
            *previous.columns,
            F.lit(None).cast(TimestampNTZType()).alias(TIMESTAMP),
            F.lit(None).cast(BooleanType()).alias(IS_DELETED),
        )
    # the per-key winners are aggregated once (a full load: every key of the table), then
    # counted, merged and written from the executors' copy
    winners = _persisted(_winners(loaded, keys, delta))
    live = ~F.col(IS_DELETED)
    try:
        # the winner of a key has its latest __timestamp: their maximum is the load's
        appended = _one_row(
            winners.agg(
                F.count(F.lit(1)).alias("n"),
                F.max(TIMESTAMP).alias("ts"),
                F.count(F.when(live, 1)).alias("live"),
                F.max(F.when(live, F.col(_quoted(delta)))).alias("max"),
            )
        )
    except BaseException:
        _unpersist(winners)
        raise
    if c["kind"] == "full" and not appended["n"]:
        _unpersist(winners)
        # another write, not this load's append: no state, the next load rebuilds it
        logger.warning(
            f"commit(): no rows of this full load in {c['target']}, the state is rebuilt by the next load"
        )
        return None
    latest, changes = _merge_df(previous, winners, keys, delta)
    changes = changes.drop(TIMESTAMP)
    if previous is not None:
        latest = _persisted(latest)  # counted, then written
    try:
        if previous is None:
            stats = {"n": appended["live"], "max": appended["max"]}
        else:
            stats = _one_row(
                latest.agg(F.count(F.lit(1)).alias("n"), F.max(_quoted(delta)).alias("max"))
            )
        # append only this load's changes, unless that makes the appended rows too many
        log_rows = (c.get("state_log_rows") or 0) + (
            appended["n"] if previous is not None else 0
        )
        append = previous is not None and log_rows <= max(stats["n"] // _LOG_RATIO, _MIN_LOG_ROWS)
        max_ts = _decode(c.get("max_timestamp"))
        if appended["ts"] is not None:
            loaded_max = appended["ts"].replace(tzinfo=None)
            if max_ts is None or loaded_max > max_ts:
                max_ts = loaded_max
        latest_max = _encode(stats["max"])
        meta = {
            _META: STATE_FORMAT,
            "plan_id": c.get("plan_id"),
            "target_version": version,
            "target_columns": target_columns,
            "keys": keys,
            "delta_column": delta,
            "source_schema": c["source_schema"],
            "min_active_rowversion": c["min_active_rowversion"],
            "latest_max": latest_max,
            "latest_count": stats["n"],
            "watermark": c["watermark"] if "watermark" in c else latest_max,
            "max_timestamp": _encode(max_ts),
            "schema": plan.schema,
            "time_scales": plan.time_scales,
            "log_rows": log_rows if append else 0,
        }
        seq = 0 if state.version is None else state.version + 1
        if append:
            try:
                state.latest.append(
                    _state_frame(changes, seq, state_schema),
                    user_metadata=json.dumps(meta, separators=(",", ":")),
                )
            except AnalysisException as e:  # e.g. key types of a state rebuilt from the target
                logger.warning(f"commit(): appending to the state failed, rewriting it: {e}")
                append = False
        if not append:
            meta["log_rows"] = 0
            state.latest.overwrite(
                _state_frame(latest, 0, state_schema),
                user_metadata=json.dumps(meta, separators=(",", ":")),
            )
    finally:
        if previous is not None:
            _unpersist(latest)
        _unpersist(winners)
    _forget(state.latest.location, c.get("old_version", -1))
    return plan.kind
