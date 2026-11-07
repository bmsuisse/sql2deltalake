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
