"""tests/test_04_strange_delta.py through the Spark data source, compared with the Python load."""

import pytest

from sql2deltalake import WriteConfig

from .conftest import execute
from .parity import Pair


@pytest.mark.parametrize(
    "by_min_timestamp,keys_per_unit",
    [(False, None), (False, 1), (True, None)],
    ids=["by_key", "by_key_units", "by_min_timestamp"],
)
def test_strange_delta(spark, source, tmp_path, monkeypatch, by_min_timestamp, keys_per_unit):
    # by_min_timestamp: strange updates are re-read by MIN(delta column) in a later pass instead
    # of by key (what more than _STRANGE_UPDATES_BY_KEY of them get). by_key_units: one key per
    # read unit, so the keys are re-read in several units.
    if keys_per_unit is not None:
        monkeypatch.setattr("sql2deltalake.spark._driver._STRANGE_KEYS_PER_UNIT", keys_per_unit)
    pair = Pair(
        spark,
        source,
        ("dbo", "user3"),
        tmp_path,
        WriteConfig(no_complex_entries_load=by_min_timestamp),
    )
    pair.load()
    pair.assert_same()
    execute(
        source,
        """UPDATE dbo.user4 SET FirstName='Vreni' where LastName='Anders';
           UPDATE dbo.user4 SET companyid='c2' where LastName='Johniingham';
           INSERT INTO dbo.user3 (FirstName, LastName, Age, companyid)
               SELECT 'Markus', 'Müller', 27, 'c2' UNION ALL SELECT 'Heiri', 'Meier', 27.98, 'c2';
           DELETE FROM dbo.user3 where LastName='Anders';
           UPDATE dbo.user3 SET LastName='wayne-hösch' where LastName='wayne'""",
    )
    _, plan = pair.load()
    assert plan.kind == "delta"
    pair.assert_same()
    # swap in user4: its rowversions are older than the ones already loaded
    execute(
        source,
        "exec sp_rename 'dbo.user3', 'user3_'; exec sp_rename 'dbo.user4', 'user3';",
    )
    _, plan = pair.load()
    assert plan.kind == "delta"
    assert plan.info["strange_updates"] > 0
    assert ("strange_updates_from" in plan.info) == by_min_timestamp
    if keys_per_unit is not None:  # the update read plus one unit per strange key
        assert len(plan.units) == 1 + plan.info["strange_updates"] + (plan.info["deletes"] > 0)
    pair.assert_same()
    assert _current(
        pair.spark_df(), "User_-_iD", ["FirstName", "LastName", "companyid"]
    ) == [
        ("Vreni", "Anders", "c1"),
        ("Peter", "Johniingham", "c2"),
        ("Petra", "wayne", "c1"),
    ]


def test_strange_delta_sys(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "company2"), tmp_path)
    _, plan = pair.load()  # empty
    assert plan.kind == "full"
    execute(
        source,
        "insert into dbo.company2(id, name) select id, name from dbo.company where id <> 'c300'",
    )
    _, plan = pair.load()  # full again: nothing in the target to go from
    assert plan.kind == "full"
    pair.assert_same()
    execute(
        source,
        "ALTER TABLE dbo.company2 drop PERIOD FOR SYSTEM_TIME",
        """INSERT INTO dbo.company2(id, name, SysStartTime, SysEndTime)
               select 'c299', 'The 299th company', '2022-01-01', (select max(SysEndTime) from dbo.company2);
           UPDATE dbo.company2 set SysStartTime='2000-01-01', name = 'The 1 company - renamed' where id='c1'""",
        "ALTER TABLE dbo.company2 add PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)",
    )
    _, plan = pair.load()
    assert plan.kind == "delta"
    assert plan.info["strange_updates"] == 2
    pair.assert_same()
    assert _current(pair.spark_df(), "id", ["id", "name"]) == [
        ("c1", "The 1 company - renamed"),
        ("c2", "The Second company"),
        ("c299", "The 299th company"),
    ]


def _current(df, key: str, columns: list) -> list:
    """The latest version of every key that is not deleted."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(key).orderBy(F.col("__timestamp").desc())
    rows = (
        df.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1 and not __is_deleted")
        .orderBy(key)
        .select(*columns)
        .collect()
    )
    return [tuple(r) for r in rows]
