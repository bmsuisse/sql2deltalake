"""Timings and Spark jobs of the Spark data source (`run`: prepare, read and append, commit) on a
1M-row table: full load, unchanged reload, reload after 1% of the rows changed. Opt-in (slow):

    SQL2DELTA_PERF=1 scripts/run_spark_tests_local.sh tests_spark/test_40_perf_spark.py -s

`SQL2DELTA_PERF_ROWS` sets the row count (default 1000000), `SQL2DELTA_PERF_ROUNDS` the rounds
(default 3, fresh targets each round); the median of each phase is printed, with the Spark jobs
of the last round.
"""

import os
import statistics
import time
from typing import Any, Callable, Dict, List

import pytest

from .conftest import execute
from .jobs import count_jobs

ROWS = int(os.getenv("SQL2DELTA_PERF_ROWS", "1000000"))
ROUNDS = int(os.getenv("SQL2DELTA_PERF_ROUNDS", "3"))
PARTITIONS = 4

pytestmark = pytest.mark.skipif(
    os.getenv("SQL2DELTA_PERF") != "1", reason="performance test, set SQL2DELTA_PERF=1"
)

TABLE = f"""
CREATE TABLE dbo.perf_1m (
    id int NOT NULL PRIMARY KEY,
    code varchar(20) NOT NULL,
    name nvarchar(100) NULL,
    amount decimal(18, 4) NULL,
    created datetime2(6) NOT NULL,
    qty int NULL,
    flag bit NULL,
    rv rowversion);
WITH n AS (
    SELECT TOP ({ROWS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
    FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
INSERT INTO dbo.perf_1m (id, code, name, amount, created, qty, flag)
SELECT i, CONCAT('C', i % 1000), CONCAT(N'name ', i), i * 1.25,
    DATEADD(second, i, '2020-01-01'), i % 97, CAST(i % 2 AS bit) FROM n
"""


def _datasource(spark: Any, conn_str: str, target: str) -> Callable[[], str]:
    from sql2deltalake.spark import run

    def load() -> str:
        return run(
            spark,
            url=conn_str,
            table="dbo.perf_1m",
            target=target,
            numPartitions=PARTITIONS,
        )

    return load


def test_datasource_1m(spark, source, tmp_path):
    execute(source, TABLE)
    variants = {"datasource": _datasource}
    times: Dict[str, Dict[str, List[float]]] = {
        v: {"full": [], "unchanged": [], "1% changed": []} for v in variants
    }
    jobs: Dict[str, Dict[str, int]] = {v: {} for v in variants}
    kinds: Dict[str, Dict[str, str]] = {v: {} for v in variants}
    for r in range(ROUNDS):
        for name, make in variants.items():
            load = make(spark, source, str(tmp_path / f"{name}_{r}"))
            for phase in ("full", "unchanged", "1% changed"):
                if phase == "1% changed":
                    execute(source, f"UPDATE dbo.perf_1m SET qty = qty + 1 WHERE id % 100 = {r}")
                with count_jobs(spark) as j:
                    t0 = time.perf_counter()
                    kinds[name][phase] = load()
                    times[name][phase].append(time.perf_counter() - t0)
                jobs[name][phase] = j.count
            # the next variant starts from the same source state
            execute(source, f"UPDATE dbo.perf_1m SET qty = qty - 1 WHERE id % 100 = {r}")
    print(f"\n{ROWS} rows, {ROUNDS} rounds, median seconds (Spark jobs, result)")
    for name in variants:
        cells = [
            f"{phase} {statistics.median(times[name][phase]):.2f} s ({jobs[name][phase]} jobs, {kinds[name][phase]})"
            for phase in times[name]
        ]
        print(f"{name:12} " + " | ".join(cells))
    assert kinds["datasource"] == {"full": "full", "unchanged": "unchanged", "1% changed": "delta"}
    assert jobs["datasource"]["unchanged"] <= 2
