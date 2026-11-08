"""The state of the Spark data source: the no-updates shortcut, commit semantics and rebuilding
the state from the target."""

from sql2deltalake.load_result import NoLoadResult
from sql2deltalake.spark import FORMAT, commit

from .conftest import execute
from .parity import Pair

CHANGE = """INSERT INTO [dbo].[user2$] (FirstName, LastName, Age, companyid) SELECT 'Markus', 'Müller', 27, 'c2';
            DELETE FROM dbo.[user2$] WHERE LastName='Anders';
            UPDATE [dbo].[user2$] SET LastName='wayne-hösch' WHERE LastName='wayne'"""


def test_unchanged_load_reads_nothing(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    pair.load()
    result, plan = pair.load()
    assert isinstance(result, NoLoadResult)
    assert plan.kind == "unchanged"
    assert plan.units == []
    pair.assert_same()
    execute(source, CHANGE)
    _, plan = pair.load()
    assert plan.kind == "delta"
    pair.assert_same()


def test_timestamps_of_a_load(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    pair.load()
    first = pair.spark_df().agg({"__timestamp": "max"}).first()[0]
    execute(source, CHANGE, "UPDATE dbo.user4 SET Age = 99")  # user4 is not loaded
    options = pair.spark_prepare()
    plan = pair.spark_finish(options)
    rows = pair.spark_df().filter(f"__timestamp > '{first}'").collect()
    deleted = {r["__timestamp"] for r in rows if r["__is_deleted"]}
    updated = {r["__timestamp"] for r in rows if not r["__is_deleted"]}
    assert len(deleted) == 1 and len(updated) == 1
    # delete markers first: a key deleted and inserted again during the load stays current
    assert min(deleted) < min(updated)
    assert plan.commit is not None
    assert [str(min(updated))] == [t.replace("T", " ") for t in plan.commit["timestamps"]]


def test_no_commit_rebuilds_the_state(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    pair.load()
    execute(source, CHANGE)
    pair.python_load()
    options = pair.spark_prepare()
    df = spark.read.format(FORMAT).options(**options).load()
    df.write.format("delta").mode("append").save(pair.target)  # no commit()
    pair.assert_same(latest_pk=False)
    execute(source, "UPDATE dbo.[user2$] SET Age = Age + 1 WHERE FirstName = 'Peter'")
    _, plan = pair.load()
    assert plan.kind == "delta"
    assert plan.info["state_rebuilt"] is True
    pair.assert_same()
    _, plan = pair.load()
    assert plan.kind == "unchanged"


def test_commit_without_append_loses_nothing(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    pair.load()
    execute(source, CHANGE)
    commit(spark, pair.spark_prepare())  # the append failed or never happened
    pair.load()
    pair.assert_same()


def test_state_appends_changes_and_compacts(spark, source, tmp_path, monkeypatch):
    """A delta load appends only the keys it changed or deleted to latest_pk_version; once the
    appended rows outgrow the limit, a commit rewrites it. A new process (no table kept by
    commit()) replays the appended rows."""
    from sql2deltalake.spark import _driver

    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    pair.load()
    state = "delta.`" + pair.state + "/latest_pk_version`"

    monkeypatch.setattr(_driver, "_MIN_LOG_ROWS", 5)
    changes = [
        CHANGE,
        "UPDATE dbo.[user2$] SET Age = Age + 1 WHERE FirstName = 'Peter'",
        "UPDATE dbo.[user2$] SET Age = Age + 1",
    ]
    for change in changes:
        execute(source, change)
        _driver._previous_latest.clear()  # as in a new driver process
        _, plan = pair.load()
        assert plan.kind == "delta"
        pair.assert_same()
    # the full load, two appends of a few keys, then a rewrite: every key changed
    history = spark.sql(f"DESCRIBE HISTORY {state}").orderBy("version").collect()
    modes = [r["operationParameters"]["mode"] for r in history]
    assert modes == ["Overwrite", "Append", "Append", "Overwrite"]
    _driver._previous_latest.clear()
    _, plan = pair.load()
    assert plan.kind == "unchanged"


def test_key_state_cache_is_bounded(monkeypatch):
    """A long-lived driver loading many targets keeps the key state of only the few targets it
    used last (SQL2DELTA_SPARK_STATE_CACHE, default 4; 0 keeps none)."""
    from sql2deltalake.spark import _driver

    monkeypatch.setattr(_driver, "_previous_latest", type(_driver._previous_latest)())
    monkeypatch.setenv("SQL2DELTA_SPARK_STATE_CACHE", "2")
    tables = {f"t{i}": object() for i in range(3)}
    _driver._remember("t0", 1, tables["t0"])
    _driver._remember("t1", 1, tables["t1"])
    assert _driver._cached("t0", 1) is tables["t0"]  # t0 is now the most recent
    _driver._remember("t2", 1, tables["t2"])
    assert _driver._cached("t1", 1) is None
    assert _driver._cached("t0", 1) is tables["t0"]
    assert _driver._cached("t2", 1) is tables["t2"]
    _driver._remember("t2", 2, tables["t2"])  # a newer version replaces the older one
    assert _driver._cached("t2", 1) is None and len(_driver._previous_latest) == 2
    monkeypatch.delenv("SQL2DELTA_SPARK_STATE_CACHE")
    for i in range(10):
        _driver._remember(f"u{i}", 1, object())
    assert len(_driver._previous_latest) == 4
    monkeypatch.setenv("SQL2DELTA_SPARK_STATE_CACHE", "0")
    _driver._remember("v", 1, object())
    assert len(_driver._previous_latest) == 0


def test_state_written_in_few_files(spark, source, tmp_path, monkeypatch):
    """createDataFrame makes one partition per Arrow batch: latest_pk_version is coalesced to
    files of _STATE_FILE_ROWS rows instead of one file per batch."""
    from sql2deltalake.spark import _driver

    monkeypatch.setattr(_driver, "_STATE_FILE_ROWS", 3)
    key = "spark.sql.execution.arrow.maxRecordsPerBatch"
    old = spark.conf.get(key)
    spark.conf.set(key, "1")
    try:
        pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
        pair.load()
    finally:
        spark.conf.set(key, old)
    pair.assert_same()
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{pair.state}/latest_pk_version`").first()
    rows = spark.read.format("delta").load(pair.state + "/latest_pk_version").count()
    assert rows > 3
    assert detail["numFiles"] == -(-rows // 3)
