"""tests/test_03_delta.py through the Spark data source, compared with the Python load."""

from .conftest import execute
from .parity import Pair


def test_delta(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "user2$"), tmp_path)
    _, plan = pair.load()
    assert plan.kind == "full"
    pair.assert_same()
    execute(
        source,
        """INSERT INTO [dbo].[user2$] (FirstName, LastName, Age, companyid)
           SELECT 'Markus', 'Müller', 27, 'c2' UNION ALL SELECT 'Heiri', 'Meier', 27.98, 'c2';
           DELETE FROM dbo.[user2$] WHERE LastName='Anders';
           UPDATE [dbo].[user2$] SET LastName='wayne-hösch' WHERE LastName='wayne'""",
    )
    result, plan = pair.load()
    assert result.executed_type == "delta"
    assert plan.kind == "delta"
    assert plan.info["deletes"] == 1
    pair.assert_same()
    rows = (
        pair.spark_df()
        .orderBy("User_-_iD", "__timestamp")
        .select("FirstName", "LastName", "__is_deleted")
        .collect()
    )
    assert [tuple(r) for r in rows] == [
        ("John", "Anders", False),
        (None, None, True),
        ("Peter", "Johniingham", False),
        ("Petra", "wayne", False),
        ("Petra", "wayne-hösch", False),
        ("Markus", "Müller", False),
        ("Heiri", "Meier", False),
    ]
    nbr = next(f for f in pair.spark_df().schema.fields if f.name == "nbr")
    assert nbr.dataType.simpleString() == "smallint"


def test_delta_sys(spark, source, tmp_path):
    pair = Pair(spark, source, ("dbo", "company"), tmp_path)
    pair.load()
    pair.assert_same()
    execute(
        source,
        "insert into dbo.[company](id, name) select 'c300', 'The 300 company'",
        "update dbo.[company] set id='c2 ' where id='c2'",
    )
    _, plan = pair.load()
    assert plan.kind == "delta"
    pair.assert_same()
    latest = pair.latest_pk("spark")
    assert [r[0] for r in latest] == ["c1", "c2", "c300"]
