"""commit() against what happened to the target since prepare(): other loads in between, no
append, the watermark after a full load over an older history, and target paths that need
quoting in Spark SQL."""

import shutil

import pytest

from sql2deltalake.spark import FORMAT, StalePlanError, commit, prepare, run
from sql2deltalake.spark._driver import _Delta, _State
from sql2deltalake.spark._plan import Plan

from .conftest import execute

TABLE = """CREATE TABLE dbo.wm (id int NOT NULL PRIMARY KEY, ver int NOT NULL, val nvarchar(20));
           INSERT INTO dbo.wm (id, ver, val) VALUES (1, 90, N'a'), (2, 100, N'b'), (3, 95, N'c')"""


def _options(source, target, **extra):
    return dict(url=source, table="dbo.wm", target=target, keys="id", deltaColumn="ver", **extra)


def _append(spark, options, target):
    spark.read.format(FORMAT).options(**options).load().write.format("delta").mode(
        "append"
    ).save(target)


def _current(spark, target):
    """id -> (ver, val) of the latest non-deleted row per key."""
    rows = spark.read.format("delta").load(target).collect()
    latest = {}
    for r in sorted(rows, key=lambda r: (r["__timestamp"], r["ver"])):
        latest[r["id"]] = None if r["__is_deleted"] else (r["ver"], r["val"])
    return {k: v for k, v in latest.items() if v is not None}


@pytest.mark.parametrize("lose_state", [False, True], ids=["committed", "rebuilt"])
def test_full_load_watermark_ignores_older_history(spark, source, tmp_path, lose_state):
    """The target's history reached ver 100; the source starts over at 10 and a full load
    reloads it. A later update to 11 must be found by simple_delta, also when the state is
    rebuilt from the target (MAX over the whole target would be 100)."""
    execute(source, TABLE)
    target = str(tmp_path / "wm")
    assert run(spark, **_options(source, target)) == "full"
    execute(source, "UPDATE dbo.wm SET ver = ver - 90")  # 0, 10, 5
    assert run(spark, **_options(source, target, loadMode="force_full")) == "full"
    if lose_state:
        shutil.rmtree(target + "_state")
    execute(source, "UPDATE dbo.wm SET ver = 11, val = N'new' WHERE id = 1")
    assert run(spark, **_options(source, target, loadMode="simple_delta")) == "simple_delta"
    assert _current(spark, target)[1] == (11, "new")


def test_commit_without_rows_of_a_full_load_keeps_no_state(spark, source, tmp_path):
    """A full plan whose rows never reached the target, while another write changed it: no
    state that claims the target has no keys."""
    execute(source, TABLE)
    target = str(tmp_path / "wm")
    run(spark, **_options(source, target))
    options = prepare(spark, **_options(source, target, loadMode="force_full"))
    assert Plan.decode(options["plan"]).kind == "full"
    df = spark.read.format("delta").load(target)
    df.filter("id = 2").write.format("delta").mode("append").save(target)  # another write
    assert commit(spark, options) is None
    meta = _State(spark, target + "_state").meta()
    assert meta is None or meta[1]["target_version"] != _Delta(spark, target).version()
    # the next load rebuilds the state and finds the rows
    execute(source, "UPDATE dbo.wm SET ver = 101, val = N'x' WHERE id = 3")
    run(spark, **_options(source, target))
    assert _current(spark, target) == {1: (90, "a"), 2: (100, "b"), 3: (101, "x")}


def test_overlapping_loads_cannot_commit_a_stale_plan(spark, source, tmp_path):
    """Two loads prepared against the same target version: once both appended, neither commit
    may write a state that misses the other load's rows."""
    execute(source, TABLE)
    target = str(tmp_path / "wm")
    run(spark, **_options(source, target))
    execute(source, "UPDATE dbo.wm SET ver = 101, val = N'x' WHERE id = 1")
    first = prepare(spark, **_options(source, target))
    execute(source, "UPDATE dbo.wm SET ver = 102, val = N'y' WHERE id = 2")
    second = prepare(spark, **_options(source, target))
    _append(spark, first, target)
    _append(spark, second, target)
    with pytest.raises(StalePlanError):
        commit(spark, first)
    with pytest.raises(StalePlanError):
        commit(spark, second)
    # the state does not belong to the target any more: the next load rebuilds it
    assert run(spark, **_options(source, target)) in ("delta", "unchanged")
    assert _current(spark, target) == {1: (101, "x"), 2: (102, "y"), 3: (95, "c")}


def test_commit_after_another_commit_is_stale(spark, source, tmp_path):
    """A load commits between prepare() and commit() of another one (that did not append):
    the second commit would put back the state before the first load."""
    execute(source, TABLE)
    target = str(tmp_path / "wm")
    run(spark, **_options(source, target))
    execute(source, "UPDATE dbo.wm SET ver = 101, val = N'x' WHERE id = 1")
    idle = prepare(spark, **_options(source, target))
    assert run(spark, **_options(source, target)) == "delta"
    with pytest.raises(StalePlanError):
        commit(spark, idle)
    execute(source, "UPDATE dbo.wm SET ver = 102, val = N'y' WHERE id = 2")
    assert run(spark, **_options(source, target)) == "delta"
    assert _current(spark, target) == {1: (101, "x"), 2: (102, "y"), 3: (95, "c")}


def test_target_path_with_backtick(spark, source, tmp_path):
    execute(source, TABLE)
    target = str(tmp_path / "we`ird" / "wm")
    assert run(spark, **_options(source, target)) == "full"
    assert run(spark, **_options(source, target)) == "unchanged"
    execute(source, "UPDATE dbo.wm SET ver = 101 WHERE id = 1")
    assert run(spark, **_options(source, target)) == "delta"


def _state_keys(spark, target):
    """The keys of latest_pk_version after replaying its appended changes."""
    import pyarrow as pa

    from sql2deltalake import _core

    df = spark.read.format("delta").load(target + "_state/latest_pk_version")
    if "__state_seq" in df.columns:
        table = pa.table(_core.replay_state(df.toArrow(), ["id"], "ver"))
    else:
        table = df.select("id", "ver").toArrow()
    return set(table.column("id").to_pylist())


def test_state_takes_the_later_of_an_update_and_a_delete(spark, source, tmp_path):
    """A key with an update and a delete marker in the same load: the later row decides, in
    the state as in the target. 1 is updated, then gets a delete marker (a later pass of the
    load found it deleted); 2 gets a delete marker, then is inserted again before the read."""
    import datetime

    import pyarrow as pa

    from sql2deltalake.spark._plan import Unit, encode_rows

    execute(source, TABLE)
    target = str(tmp_path / "wm")
    run(spark, **_options(source, target))

    execute(source, "UPDATE dbo.wm SET ver = 101, val = N'x' WHERE id = 1")
    options = prepare(spark, **_options(source, target))
    plan = Plan.decode(options["plan"])
    assert plan.kind == "delta" and plan.commit is not None
    last = max(datetime.datetime.fromisoformat(t) for t in plan.commit["timestamps"])
    later = str(last + datetime.timedelta(seconds=1))
    keys = pa.table({"id": pa.array([1], pa.int32())})
    plan.units.append(Unit(rows=encode_rows(keys), timestamp=later))
    plan.commit["delete_timestamp"] = later
    options = {**options, "plan": plan.encode()}
    _append(spark, options, target)
    commit(spark, options)
    assert set(_current(spark, target)) == {2, 3}
    assert _state_keys(spark, target) == {2, 3}

    execute(source, "DELETE FROM dbo.wm WHERE id = 2")
    options = prepare(spark, **_options(source, target))  # snapshot without 2
    execute(source, "INSERT INTO dbo.wm (id, ver, val) VALUES (2, 102, N'again')")
    _append(spark, options, target)
    commit(spark, options)
    assert _current(spark, target)[2] == (102, "again")
    assert 2 in _state_keys(spark, target)



def test_reordered_query_columns_keep_their_values(spark, source, tmp_path):
    """The select list of the query changes order (SELECT id, a, b -> SELECT id, b, a, same
    types): the planner must not reuse the schema of the old order, and the stream is matched to
    Spark's schema by name, so a and b keep their values."""
    execute(
        source,
        """CREATE TABLE dbo.reorder (id int NOT NULL PRIMARY KEY, a int, b int, ver rowversion);
           INSERT INTO dbo.reorder (id, a, b) VALUES (1, 10, 20), (2, 11, 21)""",
    )
    target = str(tmp_path / "reorder")

    def load(select, **extra):
        return run(
            spark,
            url=source,
            query=f"SELECT {select} FROM dbo.reorder",
            target=target,
            keys="id",
            deltaColumn="ver",
            **extra,
        )

    def current():
        rows = spark.read.format("delta").load(target).collect()
        latest = {}
        for r in sorted(rows, key=lambda r: r["__timestamp"]):
            latest[r["id"]] = None if r["__is_deleted"] else (r["a"], r["b"])
        return {k: v for k, v in latest.items() if v is not None}

    assert load("id, a, b, ver") == "full"
    assert current() == {1: (10, 20), 2: (11, 21)}
    assert load("id, b, a, ver", loadMode="force_full") == "full"
    assert current() == {1: (10, 20), 2: (11, 21)}
    execute(source, "UPDATE dbo.reorder SET a = 12, b = 22 WHERE id = 2")
    load("id, b, a, ver")
    assert current() == {1: (10, 20), 2: (12, 22)}
