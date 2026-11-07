"""Incremental loads from SQL Server into Delta, with Spark doing all the writing.

    from sql2deltalake.spark import prepare, commit

    options = prepare(spark, url=conn_str, table="dbo.orders", target="/lake/orders")
    df = spark.read.format("sql2deltalake").options(**options).load()
    df.write.format("delta").mode("append").save("/lake/orders")
    commit(spark, options)

or ``run(spark, url=conn_str, table="dbo.orders", target="/lake/orders")`` for all four steps
(an unchanged table writes nothing).

``prepare`` decides on the driver what the load reads (full load, delta load, nothing) from the
target and its state; the ``sql2deltalake`` data source reads it from SQL Server through arrowsql,
in parallel slices, and returns the rows to append, stamped with ``__timestamp``,
``__is_deleted`` and ``__is_full_load``. ``commit`` advances the state once the append is
committed. Needs pyspark >= 4.0 (Python data sources) and arrowsql on the driver and executors.

pyspark is imported only when one of the names below is used.
"""

from typing import Any

FORMAT = "sql2deltalake"

__all__ = [
    "FORMAT",
    "commit",
    "prepare",
    "register",
    "run",
    "SqlToDeltaDataSource",
    "StalePlanError",
]


def __getattr__(name: str) -> Any:
    if name in ("prepare", "commit", "register", "run", "StalePlanError"):
        from . import _driver

        return getattr(_driver, name)
    if name == "SqlToDeltaDataSource":
        from ._source import SqlToDeltaDataSource

        return SqlToDeltaDataSource
    raise AttributeError(name)
