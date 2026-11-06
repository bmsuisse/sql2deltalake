"""``prepare`` and ``commit``: the driver side of an incremental load, which needs a SparkSession.

Spark runs a Python data source (``schema()``, ``reader()``, ``partitions()``, ``read()`` and a
writer's ``commit()``) in Python worker processes that have no SparkSession, so the data source
cannot read the target or the state tables. ``prepare`` reads them on the driver, decides what the
load reads and hands that to the data source as the ``plan`` option. ``commit`` advances the state
after the DataFrame was appended to the target.

State, next to the target (``<target>_state/`` for a path, ``<target>_state_*`` tables for a name):

* ``latest_pk_version``: primary keys and delta column of every row as of the last committed load.
* ``primary_keys_ts``: the snapshot of primary keys and delta column read by the last ``prepare``.
* ``last_load_state``: one row written by ``commit``: the latest_pk_version version, the target's
  MAX(__timestamp) and COUNT(*) it belongs to, the source schema fingerprint, the watermark for the
  next load and MIN_ACTIVE_ROWVERSION() read before the load read any data.

The state is a cache of the target: when ``last_load_state`` does not match the target (no commit
after an append, a failed load, a crash between the writes), ``prepare`` rebuilds
``latest_pk_version`` from the target itself (latest row per key since the last full load).
"""

import datetime
from typing import Any, Callable, Dict, List, Optional

from odbc2deltalake.db_to_delta import (
    _comparable_ts,
    _fetch_source_watermark,
    _is_rowversion,
    _source_schema_fingerprint,
)
from odbc2deltalake.load_infos import retrieve_source_ts_cnt
from odbc2deltalake.metadata import InformationSchemaColInfo

from ._options import LoadOptions, normalize
from ._plan import IS_DELETED, IS_FULL_LOAD, TIMESTAMP, Plan, Unit, encode_rows
from ._source_load import SourceLoad, logger

FORMAT = "sql2deltalake"

_STRANGE_UPDATES_BY_KEY = 1000  # more strange updates are re-read by MIN(delta column)


def register(spark: Any) -> None:
    """Register the ``sql2deltalake`` format with a SparkSession."""
    from ._source import SqlToDeltaDataSource

    spark.dataSource.register(SqlToDeltaDataSource)


# --- Delta tables through Spark ----------------------------------------------------------


class _Delta:
    """A Delta table given by path (contains ``/`` or ``:``) or by name."""

    def __init__(self, spark: Any, location: str):
        self.spark = spark
        self.location = location
        self.is_path = "/" in location or ":" in location

    @property
    def ref(self) -> str:
        return f"delta.`{self.location}`" if self.is_path else self.location

    def exists(self) -> bool:
        return self.version() is not None

    def version(self) -> Optional[int]:
        from pyspark.errors import AnalysisException

        if not self.is_path and not self.spark.catalog.tableExists(self.location):
            return None
        try:
            rows = self.spark.sql(f"DESCRIBE HISTORY {self.ref} LIMIT 1").collect()
        except AnalysisException as e:
            if self.is_path and _is_missing_table(e):
                return None
            raise
        return int(rows[0]["version"]) if rows else None

    def read(self, version: Optional[int] = None) -> Any:
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        return (
            reader.load(self.location) if self.is_path else reader.table(self.location)
        )

    def overwrite(self, df: Any) -> int:
        writer = (
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        )
        if self.is_path:
            writer.save(self.location)
        else:
            writer.saveAsTable(self.location)
        version = self.version()
        assert version is not None
        return version


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
        self.snapshot = table("primary_keys_ts")
        self.meta = table("last_load_state")


def _default_state(target: str) -> str:
    return target.rstrip("/") + "_state"


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


def _rebuild_latest(target_df: Any, keys: List[str], delta: str) -> Optional[Any]:
    """latest_pk_version from the target alone: the last full load plus everything after it."""
    from pyspark.sql import functions as F

    last_full = (
        target_df.filter(F.col(IS_FULL_LOAD))
        .agg(F.max(TIMESTAMP).alias("ts"))
        .first()["ts"]
    )
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
    ``deltaColumn``, ``loadMode``, ``numPartitions``, ...). Reads the target and the state tables
    through Spark, and the source's primary keys and delta column through the data source.
    Writes only the state tables, never the target.
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
        self.target_max_ts: Optional[datetime.datetime] = None

    # --- decisions -----------------------------------------------------------------------

    def plan(self) -> Plan:
        load, opts = self.load, self.opts
        infos = load.infos
        mar = _fetch_source_watermark(infos)  # before this load reads any data
        self.mar = mar
        self.target_version = self.target.version()
        if self.target_version is None:
            return self._full("the target does not exist")
        from pyspark.sql import functions as F

        target_df = self.target.read(self.target_version)
        self.target_df = target_df
        stats = target_df.agg(
            F.max(TIMESTAMP).alias("max_ts"), F.count(F.lit(1)).alias("cnt")
        ).first()
        self.target_max_ts, self.target_count = stats["max_ts"], stats["cnt"]

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
            return self._append_inserts(self._target_max(load.delta_name))
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
        existing = {c.lower() for c in target_df.columns}
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
        watermark = (
            old["meta"]["watermark"] if old["valid"] else self._target_max(delta)
        )
        if watermark is None:
            return self._full("no delta value in the target")
        source_max, source_count = retrieve_source_ts_cnt(infos)
        if old["valid"] and self._unchanged(old["meta"], source_max, source_count):
            return self._unchanged_plan()
        simple = opts.load_mode in ("simple_delta", "simple_delta_check")
        if opts.load_mode == "simple_delta_check" and not self._simple_counts_match(
            old, watermark, source_count
        ):
            logger.warning(
                "simple_delta_check: counts do not match, doing a delta load"
            )
            simple = False
        if simple:
            return self._simple(watermark, old)
        return self._delta(watermark, old)

    def _target_max(self, column: str) -> Any:
        from pyspark.sql import functions as F

        if column not in self.target_df.columns:
            return None
        return self.target_df.agg(F.max(column).alias("m")).first()["m"]

    def _old_state(self, keys: List[str], delta: str) -> Optional[Dict[str, Any]]:
        """The state as of the last committed load: the cached latest_pk_version if
        last_load_state says it belongs to the target as it is now, else rebuilt from the target."""
        latest_version = self.state.latest.version()
        meta = None
        if latest_version is not None and self.state.meta.exists():
            rows = self.state.meta.read().collect()
            meta = rows[0].asDict() if len(rows) == 1 else None
        if (
            meta is not None
            and meta.get("latest_pk_version") == latest_version
            and meta.get("target_max_timestamp") == self.target_max_ts
            and meta.get("target_count") == self.target_count
        ):
            columns = self.state.latest.read(latest_version).columns
            return {
                "version": latest_version,
                "valid": True,
                "meta": meta,
                "columns": columns,
            }
        if not all(c in self.target_df.columns for c in [*keys, delta]):
            return None
        rebuilt = _rebuild_latest(self.target_df, keys, delta)
        if rebuilt is None:
            return None
        logger.info(
            "state does not match the target, rebuilt latest_pk_version from the target"
        )
        version = self.state.latest.overwrite(rebuilt)
        return {
            "version": version,
            "valid": False,
            "meta": None,
            "columns": rebuilt.columns,
        }

    def _unchanged(
        self, meta: Dict[str, Any], source_max: Any, source_count: int
    ) -> bool:
        """The no-updates shortcut: the source has the MAX/COUNT the last load committed, the same
        schema, and for rowversion every rowversion up to MAX was committed when that load ran."""
        infos = self.load.infos
        if meta.get("source_schema") != _source_schema_fingerprint(infos):
            return False
        local_max = _comparable_ts(meta.get("latest_max"))
        if source_max is None or local_max is None:
            return False
        if (local_max, meta.get("latest_count")) != (
            _comparable_ts(source_max),
            source_count,
        ):
            return False
        if _is_rowversion(infos.delta_col, infos.write_config.dialect):
            bound = meta.get("min_active_rowversion")
            return bound is not None and local_max < bound
        return True

    def _simple_counts_match(
        self, old: Dict[str, Any], watermark: Any, source_count: int
    ) -> bool:
        keys = self.load.key_names
        old_df = self.state.latest.read(old["version"])
        changed = self._read_rows(self.load.pk_sql(newer_than=watermark))
        new_keys = (
            changed.select(*keys).join(old_df.select(*keys), keys, "left_anti").count()
        )
        old_count = old["meta"]["latest_count"] if old["valid"] else old_df.count()
        return old_count + new_keys == source_count

    # --- plans ---------------------------------------------------------------------------

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
            "timestamps": [str(t) for t in timestamps],
            "keys": self.load.key_names,
            "delta_column": self.load.delta_name,
            "source_schema": _source_schema_fingerprint(self.load.infos),
            "min_active_rowversion": self.mar,
            **extra,
        }

    def _ts(self) -> Any:
        ts = self.load.next_timestamp(after=self.target_max_ts)
        return ts

    def _ts_value(self, ts: Any) -> datetime.datetime:
        from odbc2deltalake.db_to_delta import _load_timestamp_value

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
        info = {"reason": reason}
        if merge_schema:
            info["merge_schema"] = True
        return load.plan("full", [unit], sql, commit=commit, info=info)

    def _append_inserts(self, watermark: Any) -> Plan:
        load = self.load
        ts = self._ts()
        criterion = load.criterion(watermark, ">") if watermark else None
        sql = load.update_sql(ts, criterion)
        unit = Unit(sql=sql, split=load.split(load.column_names))
        return load.plan(
            "append_inserts", [unit], sql, info={"watermark": str(watermark)}
        )

    def _unchanged_plan(self) -> Plan:
        load = self.load
        sql = load.full_sql(self._ts())
        return load.plan("unchanged", [], sql, info={"reason": "no updates"})

    def _simple(self, watermark: Any, old: Dict[str, Any]) -> Plan:
        load = self.load
        ts = self._ts()
        sql = load.update_sql(ts, load.criterion(watermark, ">"))
        unit = Unit(sql=sql, split=load.split(load.column_names))
        commit = self._commit_info(
            "simple", [self._ts_value(ts)], old_version=old["version"]
        )
        return load.plan(
            "simple_delta",
            [unit],
            sql,
            commit=commit,
            info={"watermark": str(watermark)},
        )

    def _read_rows(self, sql: str) -> Any:
        """A source query as a DataFrame, read in slices through the data source."""
        load = self.load
        plan = load.plan(
            "rows",
            [Unit(sql=sql, split=load.split([*load.key_names, load.delta_name]))],
            sql,
        )
        return (
            self.spark.read.format(FORMAT)
            .option("url", self.opts.url)
            .option("plan", plan.encode())
            .load()
        )

    def _delta(self, watermark: Any, old: Dict[str, Any]) -> Plan:
        from pyspark.sql import functions as F

        load, opts = self.load, self.opts
        keys, delta = load.key_names, load.delta_name
        assert delta is not None
        # Delete markers get the first timestamp of the load: a key deleted and inserted again
        # while the load runs is then still current after it.
        ts_deletes, ts_updates = self._ts(), self._ts()

        snapshot_version = self.state.snapshot.overwrite(self._read_rows(load.pk_sql()))
        snapshot = self.state.snapshot.read(snapshot_version)
        old_df = self.state.latest.read(old["version"])
        delta_type = snapshot.schema[delta].dataType
        same_row = [snapshot[c].eqNullSafe(old_df[c]) for c in [*keys, delta]]
        strange = snapshot.join(old_df, same_row, "left_anti").filter(
            (F.col(delta) <= _lit(watermark, delta_type)) | F.col(delta).isNull()
        )
        deletes = old_df.select(*keys).join(snapshot.select(*keys), keys, "left_anti")

        units: List[Unit] = []
        n_deletes = deletes.count()
        if n_deletes > opts.max_inline_deletes:
            return self._full(f"{n_deletes} deletes, more than maxInlineDeletes")
        if n_deletes:
            units.append(
                Unit(
                    rows=encode_rows(deletes.toArrow()),
                    timestamp=str(self._ts_value(ts_deletes)),
                )
            )
        sql = load.update_sql(ts_updates, load.criterion(watermark, ">"))
        units.append(Unit(sql=sql, split=load.split(load.column_names)))
        timestamps = [self._ts_value(ts_updates)]
        n_strange = strange.count()
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
            min_ts = strange.agg(F.min(delta).alias("m")).first()["m"]
            if (
                n_strange > _STRANGE_UPDATES_BY_KEY
                or load.infos.write_config.no_complex_entries_load
            ) and min_ts is not None:
                units.append(
                    Unit(
                        sql=load.update_sql(ts_strange, load.criterion(min_ts, ">=")),
                        split=load.split(load.column_names),
                    )
                )
                info["strange_updates_from"] = str(min_ts)
            else:
                data = [
                    {f"p{i}": row[k] for i, k in enumerate(keys)}
                    for row in strange.select(*keys).collect()
                ]
                units.append(Unit(sql=load.keys_sql(data, ts_strange)))
        commit = self._commit_info(
            "delta",
            timestamps,
            old_version=old["version"],
            snapshot_version=snapshot_version,
            delete_timestamp=str(self._ts_value(ts_deletes)) if n_deletes else None,
        )
        return load.plan("delta", units, sql, commit=commit, info=info)


# --- commit ------------------------------------------------------------------------------


def commit(spark: Any, options: Dict[str, str]) -> Optional[str]:
    """Advance the state after the DataFrame of ``options`` (from ``prepare``) was appended to
    the target. Returns the plan kind, or None if the plan has nothing to commit.

    The new state comes from the rows of this load found in the target (by their
    ``__timestamp``), so a commit without the append (or with a failed one) only keeps the old
    state for the keys of this load, and the next load reads them again."""
    from pyspark.sql import functions as F

    plan = Plan.decode(normalize(options)["plan"])
    c = plan.commit
    if c is None:
        return None
    target = _Delta(spark, c["target"])
    version = target.version()
    if version is None:
        raise RuntimeError(
            f"{c['target']} does not exist: append the DataFrame first, then commit()"
        )
    state = _State(spark, c["state"])
    keys, delta = c["keys"], c["delta_column"]
    target_df = target.read(version)
    ts_type = target_df.schema[TIMESTAMP].dataType

    def at(timestamps: List[str]) -> Any:
        return F.col(TIMESTAMP).isin(
            *[_lit(datetime.datetime.fromisoformat(t), ts_type) for t in timestamps]
        )

    loaded = _latest_per_key(
        target_df.filter(at(c["timestamps"]) & ~F.col(IS_DELETED)).select(
            *keys, delta, TIMESTAMP
        ),
        keys,
        delta,
    ).select(*keys, delta)
    if c["kind"] == "full":
        latest = loaded
        watermark_df = target_df
    else:
        old = state.latest.read(c["old_version"]).select(*keys, delta)
        kept = old.join(loaded, keys, "left_anti")
        if c.get("delete_timestamp"):
            deleted = target_df.filter(at([c["delete_timestamp"]]) & F.col(IS_DELETED))
            kept = kept.join(deleted.select(*keys), keys, "left_anti")
        latest = loaded.unionByName(kept)
        watermark_df = (
            state.snapshot.read(c["snapshot_version"])
            if c["kind"] == "delta"
            else target_df
        )
    latest_version = state.latest.overwrite(latest.select(*keys, delta))
    meta = (
        state.latest.read(latest_version)
        .agg(F.max(delta).alias("latest_max"), F.count(F.lit(1)).alias("latest_count"))
        .crossJoin(
            target_df.agg(
                F.max(TIMESTAMP).alias("target_max_timestamp"),
                F.count(F.lit(1)).alias("target_count"),
            )
        )
        .crossJoin(watermark_df.agg(F.max(delta).alias("watermark")))
        .select(
            F.lit(latest_version).cast("long").alias("latest_pk_version"),
            F.lit(c["source_schema"]).alias("source_schema"),
            F.lit(c["min_active_rowversion"])
            .cast("long")
            .alias("min_active_rowversion"),
            "*",
        )
    )
    state.meta.overwrite(meta)
    return plan.kind
