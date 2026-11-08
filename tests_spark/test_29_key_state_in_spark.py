"""The key state compared and merged in Spark (the default): the same Delta tables as the Python
load, and no key of the state or of the source snapshot passes the driver. A 78M-row table ran a
Databricks driver out of memory when commit() and prepare() held every key there."""

import pytest

from sql2deltalake import WriteConfig
from sql2deltalake.spark import _driver
from sql2deltalake.spark._source_load import SourceLoad

from .conftest import execute
from .parity import Pair

COLUMNS = "[User - iD], FirstName, LastName, Age, companyid, nbr"
CHANGES = [
    # updates, a delete, an insert
    f"""SELECT {COLUMNS} INTO dbo.user2_deleted FROM dbo.[user2$] WHERE LastName='Anders';
       UPDATE dbo.[user2$] SET LastName='wayne-hösch' WHERE LastName='wayne';
       DELETE FROM dbo.[user2$] WHERE LastName='Anders';
       INSERT INTO dbo.[user2$] (FirstName, LastName, Age, companyid) SELECT 'Markus', 'Müller', 27, 'c2'""",
    # the deleted key comes back
    f"""SET IDENTITY_INSERT dbo.[user2$] ON;
       INSERT INTO dbo.[user2$] ({COLUMNS}) SELECT {COLUMNS} FROM dbo.user2_deleted;
       SET IDENTITY_INSERT dbo.[user2$] OFF""",
    "UPDATE dbo.[user2$] SET Age = Age + 1",
]


@pytest.fixture
def in_spark(monkeypatch):
    """Spark mode, and every driver-side state path fails the test if it is used. Collects are
    recorded: each must be small."""
    from sql2deltalake import _core

    monkeypatch.delenv(_driver._KEY_STATE_ENV, raising=False)

    def forbidden(name):
        def fail(*args, **kwargs):
            raise AssertionError(f"{name} holds the key state on the driver")

        return fail

    # (replay_state is not on the list: Pair.assert_same compares the states with it)
    for name in ("pk_diff", "merge_latest", "count_new_keys"):
        monkeypatch.setattr(_core, name, forbidden(f"_core.{name}"))
    monkeypatch.setattr(SourceLoad, "pk_snapshot", forbidden("SourceLoad.pk_snapshot"))
    monkeypatch.setattr(_driver, "_read_latest", forbidden("_read_latest"))
    collected = []
    to_arrow = _driver._to_arrow

    def recording(df):
        table = to_arrow(df)
        collected.append(table.num_rows)
        return table

    monkeypatch.setattr(_driver, "_to_arrow", recording)
    return collected


def test_delta_loads_in_spark(spark, source, tmp_path, monkeypatch, in_spark):
    monkeypatch.setattr(_driver, "_MIN_LOG_ROWS", 3)  # appends, then a rewrite of the state
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    _, plan = pair.load()
    assert (plan.kind, plan.info["key_state"]) == ("full", "spark")
    pair.assert_same()
    for change in CHANGES:
        execute(source, change)
        _, plan = pair.load()
        assert (plan.kind, plan.info["key_state"]) == ("delta", "spark")
        pair.assert_same()
    _, plan = pair.load()
    assert plan.kind == "unchanged"
    state = "delta.`" + pair.state + "/latest_pk_version`"
    modes = [
        r["operationParameters"]["mode"]
        for r in spark.sql(f"DESCRIBE HISTORY {state}").orderBy("version").collect()
    ]
    assert modes[0] == "Overwrite" and "Append" in modes and modes.count("Overwrite") >= 2
    assert in_spark and max(in_spark) <= 2  # one-row aggregates and the one delete


@pytest.mark.parametrize(
    "by_min_timestamp", [False, True], ids=["by_key", "by_min_timestamp"]
)
def test_strange_updates_in_spark(spark, source, tmp_path, by_min_timestamp, in_spark):
    pair = Pair(
        spark,
        source,
        ("dbo", "user3"),
        tmp_path,
        WriteConfig(no_complex_entries_load=by_min_timestamp),
    )
    pair.load()
    # as in test_04: user4 changes first, then newer changes of user3
    execute(
        source,
        """UPDATE dbo.user4 SET FirstName='Vreni' where LastName='Anders';
           UPDATE dbo.user4 SET companyid='c2' where LastName='Johniingham';
           DELETE FROM dbo.user3 where LastName='Anders';
           UPDATE dbo.user3 SET LastName='wayne-hösch' where LastName='wayne'""",
    )
    pair.load()
    # swap in user4: its rowversions are older than the ones already loaded
    execute(
        source,
        "exec sp_rename 'dbo.user3', 'user3_'; exec sp_rename 'dbo.user4', 'user3';",
    )
    _, plan = pair.load()
    assert plan.kind == "delta" and plan.info["strange_updates"] > 0
    assert ("strange_updates_from" in plan.info) == by_min_timestamp
    pair.assert_same()


def test_too_many_deletes_in_spark(spark, source, tmp_path, in_spark):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path, maxInlineDeletes=1)
    pair.load()
    execute(source, "DELETE FROM dbo.[user2$] WHERE LastName IN ('Anders', 'wayne')")
    _, plan = pair.load()
    assert plan.kind == "full" and "deletes" in plan.info["reason"]
    assert max(in_spark) <= 1  # counted, not collected


def test_driver_mode_still_works(spark, source, tmp_path, monkeypatch):
    monkeypatch.setenv(_driver._KEY_STATE_ENV, "driver")
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    _, plan = pair.load()
    assert plan.info["key_state"] == "driver"
    execute(source, CHANGES[0])
    _, plan = pair.load()
    assert (plan.kind, plan.info["key_state"]) == ("delta", "driver")
    pair.assert_same()
