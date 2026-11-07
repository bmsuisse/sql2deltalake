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
# a commit rewrites latest_pk_version when more rows than this were appended since the last
# rewrite: a fifth of the keys, at least _MIN_LOG_ROWS
_LOG_RATIO = 5
_MIN_LOG_ROWS = 10_000
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
        if row["operation"] not in _SAME_ROWS:
            writes += 1
    if version < 0 and history and history[-1]["version"] == 0:
        return writes  # ``version`` -1: before the table existed
    return None


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


def _read_latest(state: _State, version: int, keys: List[str], delta: str) -> Any:
    import pyarrow as pa

    from sql2deltalake import _core

    key = (state.latest.location, version)
    table = _cached(*key)
    if table is None:
        df = state.latest.read(version)
        if _SEQ in df.columns:
            table = pa.table(_core.replay_state(df.toArrow(), keys, delta, _SEQ, IS_DELETED))
        else:
            table = df.select(*keys, delta).toArrow()
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
    flags = table.column(IS_DELETED) if IS_DELETED in table.column_names else pa.array([deleted] * n, pa.bool_())
    columns = [c for c in table.column_names if c != IS_DELETED]
    return pa.table(
        [*(table.column(c) for c in columns), pa.array([seq] * n, pa.int64()), flags],
        names=[*columns, _SEQ, IS_DELETED],
    )


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
    the state through Spark, and the source's primary keys and delta column on the driver.
    Writes only the state, never the target.
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
        load, opts = self.load, self.opts
        infos = load.infos
        history = self.target.history()
        self.target_version = history[0]["version"] if history else None
        state = self.state.meta() if history else None
        if state is not None and _same_rows_since(
            history, state[1].get("target_version", -1)
        ):
            self.meta_version, self.meta = state
        # MAX/COUNT, MIN_ACTIVE_ROWVERSION() and the clock, before the load reads any data
        self.source = load.source_state()
        self.mar = self.source["mar"]
        if self.target_version is None:
            return self._full("the target does not exist")

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
        table = rebuilt.toArrow()
        self.state.latest.overwrite(
            self.spark.createDataFrame(
                _state_rows(table, 0), schema=_state_schema(rebuilt.schema)
            )
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

    def _previous(self, old: Dict[str, Any]) -> Any:
        delta = self.load.delta_name
        assert delta is not None
        return _read_latest(self.state, old["version"], self.load.key_names, delta)

    def _simple_counts_match(self, old: Dict[str, Any], watermark: Any) -> bool:
        from sql2deltalake import _core

        previous = self._previous(old)
        changed = self.load.read_rows(self.load.pk_sql(newer_than=watermark))
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

    def _plan(self, kind: str, units: List[Unit], sql: str, **kwargs: Any) -> Plan:
        return self.load.plan(kind, units, sql, known=self._known_schema(), **kwargs)

    def _commit_info(
        self, kind: str, timestamps: List[Any], **extra: Any
    ) -> Dict[str, Any]:
        return {
            "kind": kind,
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
        snapshot = load.pk_snapshot()
        diff = _core.pk_diff(
            snapshot, previous, keys, delta, pa.table({"v": [watermark]})
        )
        deletes = pa.table(diff["deletes"])
        strange = pa.table(diff["strange"])
        n_deletes, n_strange = deletes.num_rows, strange.num_rows

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
            min_ts = _value(diff["strange_min"])
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
            watermark=_encode(_value(diff["snapshot_max"])),
            delete_timestamp=str(self._ts_value(ts_deletes)) if n_deletes else None,
        )
        return self._plan("delta", units, sql, commit=commit, info=info)


# --- commit ------------------------------------------------------------------------------


class StalePlanError(RuntimeError):
    """``commit()`` of a plan that another load overtook: committing it would overwrite the
    other load's key state."""


def _check_not_stale(
    c: Dict[str, Any], history: List[Dict[str, Any]], state: _State
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
    state_history = state.latest.history()
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
    the target. Returns the plan kind, or None if the plan has nothing to commit.

    The new state comes from the rows of this load found in the target (by their
    ``__timestamp``), so a commit without the append (or with a failed one) only keeps the old
    state for the keys of this load, and the next load reads them again."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F
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
    _check_not_stale(c, history, state)
    keys, delta = c["keys"], c["delta_column"]
    previous = (
        None
        if c["kind"] == "full"
        else _read_latest(state, c["old_version"], keys, delta)
    )
    target_columns = c.get("target_columns")
    if version != c["target_version"]:
        target_df = target.read(version)
        ts_type = target_df.schema[TIMESTAMP].dataType

        def at(timestamps: List[str]) -> Any:
            return F.col(TIMESTAMP).isin(
                *[_lit(datetime.datetime.fromisoformat(t), ts_type) for t in timestamps]
            )

        rows = at(c["timestamps"]) & ~F.col(IS_DELETED)
        if c.get("delete_timestamp"):
            rows = rows | (at([c["delete_timestamp"]]) & F.col(IS_DELETED))
        loaded = (
            target_df.filter(rows).select(*keys, delta, TIMESTAMP, IS_DELETED).toArrow()
        )
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
                spark.createDataFrame(_state_rows(changes, seq), schema=state_schema),
                user_metadata=json.dumps(meta, separators=(",", ":")),
            )
        except AnalysisException as e:  # e.g. key types of a state rebuilt from the target
            logger.warning(f"commit(): appending to the state failed, rewriting it: {e}")
            append = False
    if not append:
        meta["log_rows"] = 0
        state.latest.overwrite(
            spark.createDataFrame(_state_rows(latest, 0), schema=state_schema),
            user_metadata=json.dumps(meta, separators=(",", ":")),
        )
    _forget(state.latest.location, c.get("old_version", -1))
    # The next prepare() in this process compares with this table instead of reading it back
    # (several Spark jobs). Its version follows the one _check_not_stale saw: had another
    # write come in between, this commit is newer than it, and prepare() takes the newest.
    _remember(state.latest.location, 0 if state.version is None else state.version + 1, latest)
    return plan.kind
