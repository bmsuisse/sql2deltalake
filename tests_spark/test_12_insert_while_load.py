"""tests/test_12_insert_while_load.py through the Spark data source, compared with the Python load.

A row inserted before the load and deleted after its primary key snapshot, before the delta read:
neither load may write it, nor keep it in latest_pk_version. Both loads take their snapshot
before the delete."""

from sql2deltalake import WriteConfig

from .conftest import execute
from .parity import Pair


def test_insert_while_load(spark, source, tmp_path):
    cfg = WriteConfig(primary_keys=["User_-_iD"], delta_col="time stamp")
    pair = Pair(spark, source, ("dbo", "user8"), tmp_path, cfg)
    pair.load()
    pair.assert_same()
    execute(
        source,
        """set identity_insert dbo.user8 on;
           INSERT into dbo.user8(FirstName, LastName, [User - iD], companyid) SELECT 'Francis', 'Peterson', 98, 'c1'""",
    )
    options = pair.spark_prepare()  # snapshot with 98

    def on_info(msg: str) -> None:
        if msg.startswith("Start delta step 2"):  # after the Python load's snapshot
            execute(source, 'delete from dbo.user8 where "User - iD"=98')

    pair.python_load(on_info)
    plan = pair.spark_finish(options)
    assert plan.kind == "delta"
    pair.assert_same()
    assert pair.spark_df().filter("`User_-_iD` = 98").count() == 0
    assert all(r[0] != 98 for r in pair.latest_pk("spark"))
    # and the next load is consistent with the source again
    pair.load()
    pair.assert_same()
