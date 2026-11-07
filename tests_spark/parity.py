"""Runs the same load twice, through the Python ``write_db_to_delta`` (arrowsql reader, DuckDB,
delta-rs) and through the Spark data source, against the same source state, and compares the
two Delta tables value for value.

``__timestamp`` differs between the two (each load reads the server clock), so rows are compared
in the order of their history (by key, then ``__timestamp``) with ``__timestamp`` left out.
"""

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from odbc2deltalake import WriteConfig, make_writer
from odbc2deltalake.destination.file_system import FileSystemDestination
from odbc2deltalake.reader.arrowsql_reader import ArrowSQLReader
from sql2deltalake.spark import FORMAT, commit, prepare
from sql2deltalake.spark._plan import Plan


class Pair:
    def __init__(
        self,
        spark: Any,
        conn_str: str,
        table: Union[Tuple[str, str], str],
        base: Path,
        write_config: Optional[WriteConfig] = None,
        **spark_options: Any,
    ):
        self.spark = spark
        self.conn_str = conn_str
        self.table = table
        self.write_config = write_config
        name = table if isinstance(table, str) else "_".join(table)
        name = "".join(ch for ch in name if ch.isalnum() or ch == "_")
        (base / "py").mkdir(parents=True, exist_ok=True)
        self.reader = ArrowSQLReader(conn_str, str(base / "py" / f"{name}.duckdb"))
        self.py_dest = FileSystemDestination(base / "py" / name)
        self.target = str(base / "spark" / name / "delta")
        self.state = str(base / "spark" / name / "delta_state")
        options: Dict[str, Any] = {"numPartitions": 4}
        if write_config is not None:
            if write_config.load_mode != "append":
                options["loadMode"] = write_config.load_mode
            if write_config.primary_keys:
                options["keys"] = ",".join(write_config.primary_keys)
            if write_config.delta_col:
                options["deltaColumn"] = write_config.delta_col
            if write_config.no_complex_entries_load:
                options["noComplexEntriesLoad"] = "true"
        options.update(spark_options)
        self.options = options
        self.plans: List[Plan] = []

    # --- the two loads ---------------------------------------------------------------------

    def python_load(self, on_info: Optional[Callable[[str], None]] = None) -> Any:
        writer = make_writer(
            self.reader, self.table, self.py_dest, write_config=self.write_config
        )
        if on_info is not None:
            orig = writer.logger.info

            def info(*args: Any, **kwargs: Any) -> None:
                on_info(args[0])
                orig(*args, **kwargs)

            writer.logger.info = info  # type: ignore[method-assign]
        return writer.execute()

    def spark_prepare(self) -> Dict[str, str]:
        table = (
            self.table
            if isinstance(self.table, str)
            else ".".join(f"[{p}]" for p in self.table)
        )
        return prepare(
            self.spark,
            url=self.conn_str,
            table=table,
            target=self.target,
            **self.options,
        )

    def spark_finish(self, options: Dict[str, str]) -> Plan:
        plan = Plan.decode(options["plan"])
        df = self.spark.read.format(FORMAT).options(**options).load()
        writer = df.write.format("delta").mode("append")
        if plan.info.get("merge_schema"):
            writer = writer.option("mergeSchema", "true")
        writer.save(self.target)
        commit(self.spark, options)
        self.plans.append(plan)
        return plan

    def load(self) -> Tuple[Any, Plan]:
        result = self.python_load()
        return result, self.spark_finish(self.spark_prepare())

    # --- comparison ------------------------------------------------------------------------

    def py_df(self) -> Any:
        return self.spark.read.format("delta").load(str(self.py_dest.path / "delta"))

    def spark_df(self) -> Any:
        return self.spark.read.format("delta").load(self.target)

    def history(self, df: Any) -> List[tuple]:
        keys = self._keys(df)
        ordered = df.orderBy(*keys, "__timestamp").drop("__timestamp")
        return [tuple(r) for r in ordered.collect()]

    def _keys(self, df: Any) -> List[str]:
        plan = next((p for p in reversed(self.plans) if p.commit), None)
        if plan is not None:
            return list(plan.commit["keys"])  # type: ignore[index]
        return [c for c in df.columns if not c.startswith("__")]

    def assert_same(self, latest_pk: bool = True) -> None:
        py, sp = self.py_df(), self.spark_df()
        assert py.columns == sp.columns
        py_types = {f.name: f.dataType.simpleString() for f in py.schema.fields}
        sp_types = {f.name: f.dataType.simpleString() for f in sp.schema.fields}
        assert py_types == sp_types
        py_rows, sp_rows = self.history(py), self.history(sp)
        assert py_rows == sp_rows, _diff(py_rows, sp_rows)
        if latest_pk:
            self.assert_same_latest_pk()

    def latest_pk(self, which: str) -> Optional[List[tuple]]:
        """latest_pk_version of the Python or the Spark load, None if there is none."""
        path = (
            self.py_dest.path / "delta_load" / "latest_pk_version"
            if which == "py"
            else Path(self.state) / "latest_pk_version"
        )
        if not (path / "_delta_log").exists():
            return None
        df = self.spark.read.format("delta").load(str(path))
        if "__state_seq" in df.columns:
            # append-only: keys, delta column, __state_seq, __is_deleted
            import pyarrow as pa

            from odbc2deltalake import _core

            keys, delta = df.columns[:-3], df.columns[-3]
            table = pa.table(_core.replay_state(df.toArrow(), keys, delta))
            df = self.spark.createDataFrame(table, schema=df.select(*keys, delta).schema)
        return [tuple(r) for r in df.orderBy(*df.columns).collect()]

    def assert_same_latest_pk(self) -> None:
        assert self.latest_pk("py") == self.latest_pk("spark")


def _diff(a: List[tuple], b: List[tuple]) -> str:
    lines = ["python vs spark:"]
    for i in range(max(len(a), len(b))):
        left = a[i] if i < len(a) else None
        right = b[i] if i < len(b) else None
        mark = "  " if left == right else "!="
        lines.append(f"{mark} {left}\n   {right}")
    return "\n".join(lines)
