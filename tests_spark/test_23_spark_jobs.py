"""How many Spark jobs a load runs: on Databricks every job costs scheduling time, so the state is
read from the Delta history (no scans) and the rows are read and appended in one job. The key
snapshot is compared in Spark (a few joins and aggregates), or with SQL2DELTA_SPARK_KEY_STATE=driver
in Rust on the driver (no job)."""

from sql2deltalake.spark import FORMAT, _driver, commit, run
from sql2deltalake.spark._plan import Plan

from .conftest import execute
from .jobs import count_jobs
from .parity import Pair

CHANGE = "UPDATE [dbo].[user2$] SET LastName='wayne-hösch' WHERE LastName='wayne'"


def _phases(pair: Pair, spark):
    with count_jobs(spark) as prepare_jobs:
        options = pair.spark_prepare()
    plan = Plan.decode(options["plan"])
    with count_jobs(spark) as write_jobs:
        if plan.kind != "unchanged":
            df = spark.read.format(FORMAT).options(**options).load()
            df.write.format("delta").mode("append").save(pair.target)
    with count_jobs(spark) as commit_jobs:
        commit(spark, options)
    print(
        f"\n{plan.kind}: prepare {prepare_jobs.count}, read+append {write_jobs.count}, commit {commit_jobs.count}"
    )
    return plan.kind, prepare_jobs.count, write_jobs.count, commit_jobs.count


def test_jobs_per_phase(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    in_spark = _driver._key_state_in_spark()
    kind, prepare, write, commit_ = _phases(pair, spark)
    assert kind == "full"
    assert prepare == 0  # no target yet: nothing to read
    assert write <= 2
    # in Spark: this load's rows read back and merged, counted, written (adaptive execution runs
    # every shuffle stage as a job)
    assert commit_ <= (16 if in_spark else 7)

    kind, prepare, write, commit_ = _phases(pair, spark)
    assert (kind, write, commit_) == ("unchanged", 0, 0)
    assert prepare <= 2  # the history of the target and of the state, no scans

    execute(source, CHANGE)
    kind, prepare, write, commit_ = _phases(pair, spark)
    assert kind == "delta"
    assert write == 1  # one job reads the slices and appends them
    if in_spark:
        # the history; the source's keys read and compared with the state in one aggregate,
        # and the changed keys collected
        assert prepare <= 16
        # the history, this load's rows merged with the state, counted, the changes appended
        assert commit_ <= 26
    else:
        # the history of the target and of the state; latest_pk_version comes from the last
        # commit() of this process, the key comparison runs in Rust
        assert prepare <= 2
        # the history of the target and of the state, this load's rows read back, the append
        # of the changed keys to the state (no rewrite of latest_pk_version)
        assert commit_ <= 6

    kind, prepare, _, _ = _phases(pair, spark)
    assert kind == "unchanged" and prepare <= 2


def test_run(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    options = dict(url=source, table="[dbo].[user2$]", target=pair.target, **pair.options)

    def load(expected: str) -> None:
        pair.python_load()
        assert run(spark, **options) == expected
        pair.assert_same(latest_pk=True)

    load("full")
    with count_jobs(spark) as jobs:
        assert run(spark, **options) == "unchanged"
    assert jobs.count <= 2
    execute(source, CHANGE)
    load("delta")
    execute(source, "DELETE FROM dbo.[user2$] WHERE LastName='Anders'")
    load("delta")
