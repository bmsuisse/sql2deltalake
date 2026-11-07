"""tests/test_06_append_inserts.py, test_07_simple_delta.py and test_08_simple_delta_check.py
through the Spark data source, compared with the Python load."""

from sql2deltalake import WriteConfig

from .conftest import execute
from .parity import Pair
from .test_04_strange_delta import _current


def test_append_inserts(spark, source, tmp_path):
    pair = Pair(
        spark, source, ("dbo", "log"), tmp_path, WriteConfig(load_mode="append_inserts")
    )
    _, plan = pair.load()
    assert plan.kind == "full"
    pair.assert_same()
    execute(
        source, "INSERT INTO [dbo].[log] ([message]) SELECT 'The second log message'"
    )
    _, plan = pair.load()
    assert plan.kind == "append_inserts"
    assert plan.commit is None
    pair.assert_same()
    rows = pair.spark_df().orderBy("__timestamp").select("id", "message").collect()
    assert [tuple(r) for r in rows] == [
        (1, "The first log message"),
        (2, "The second log message"),
    ]


def test_simple_delta(spark, source, tmp_path):
    pair = Pair(
        spark,
        source,
        ("dbo", "company3"),
        tmp_path,
        WriteConfig(load_mode="simple_delta"),
    )
    pair.load()
    pair.assert_same()
    execute(
        source,
        """insert into dbo.[company3](id, name) select 'c300', 'The 300 company';
           UPDATE dbo.[company3] SET name='Die zwooti firma' where id='c2'""",
    )
    _, plan = pair.load()
    assert plan.kind == "simple_delta"
    pair.assert_same()
    assert _current(pair.spark_df(), "id", ["name"]) == [
        ("The First company",),
        ("Die zwooti firma",),
        ("The 300 company",),
    ]
    # switching to a normal delta load later on works from the state of the simple loads
    pair.write_config = WriteConfig()
    pair.options.pop("loadMode")
    _, plan = pair.load()
    assert plan.kind in ("delta", "unchanged")
    pair.assert_same()


def test_simple_delta_check(spark, source, tmp_path):
    pair = Pair(
        spark,
        source,
        ("dbo", "company3"),
        tmp_path,
        WriteConfig(load_mode="simple_delta_check"),
    )
    pair.load()
    pair.assert_same()
    execute(
        source,
        """insert into dbo.[company3](id, name) select 'c400', 'The 400 company';
           UPDATE dbo.[company3] SET name='Die zwooti firma 2.0', date_timer=getdate() where id='c2';
           insert into dbo.[company3](id, name) select 'c500', 'The 500 company'""",
    )
    _, plan = pair.load()
    assert plan.kind == "simple_delta"  # the counts match
    pair.assert_same()
    execute(source, "delete from dbo.company3 where id='c400'")
    _, plan = pair.load()
    assert plan.kind == "delta"  # a delete: the counts do not match
    assert plan.info["deletes"] == 1
    pair.assert_same()
    assert _current(pair.spark_df(), "id", ["id", "name"]) == [
        ("c1", "The First company"),
        ("c2", "Die zwooti firma 2.0"),
        ("c500", "The 500 company"),
    ]
    pair.write_config = WriteConfig()
    pair.options.pop("loadMode")
    _, plan = pair.load()
    pair.assert_same()


def test_append_inserts_with_a_zero_watermark(spark, source, tmp_path):
    """The target's largest delta value is 0: append_inserts still reads only rows above it."""
    execute(
        source,
        """CREATE TABLE dbo.zero_wm (id int NOT NULL PRIMARY KEY, seq int NOT NULL, msg nvarchar(20));
           INSERT INTO dbo.zero_wm (id, seq, msg) VALUES (1, 0, N'zero')""",
    )
    cfg = WriteConfig(load_mode="append_inserts", delta_col="seq")
    pair = Pair(spark, source, ("dbo", "zero_wm"), tmp_path, cfg)
    pair.load()
    _, plan = pair.load()
    assert plan.kind == "append_inserts"
    pair.assert_same(latest_pk=False)
    assert [r["seq"] for r in pair.spark_df().collect()] == [0]
