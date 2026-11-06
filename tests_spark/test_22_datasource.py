"""The data source without a plan, its options and the plan encoding."""

import subprocess
import sys

import pytest

from sql2deltalake.spark import FORMAT, register
from sql2deltalake.spark._options import LoadOptions, normalize, parse_table
from sql2deltalake.spark._plan import Plan, Split, Unit


def test_importing_does_not_need_pyspark():
    code = (
        "import sys, sql2deltalake, sql2deltalake.spark, sql2deltalake.spark._plan, "
        "sql2deltalake.spark._options; assert 'pyspark' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.parametrize(
    "text,parts",
    [
        ("dbo.user", ("dbo", "user")),
        ("[dbo].[user2$]", ("dbo", "user2$")),
        ("[long schema].[long table name]", ("long schema", "long table name")),
        ("db.[a]]b].c", ("db", "a]b", "c")),
        ("user", ("user",)),
    ],
)
def test_parse_table(text, parts):
    assert parse_table(text) == parts


def test_options():
    opts = LoadOptions.parse(
        {"URL": "Server=x", "dbtable": "dbo.t", "deltaColumn": "rv", "num_partitions": "4", "keys": "a, b"}
    )
    assert (opts.table, opts.delta_column, opts.num_partitions, opts.keys) == (
        ("dbo", "t"),
        "rv",
        4,
        ["a", "b"],
    )
    with pytest.raises(ValueError, match="unknown option"):
        normalize({"deltaColumns": "x"})
    with pytest.raises(ValueError, match="exactly one"):
        LoadOptions.parse({"url": "x", "table": "a", "query": "select 1"})
    with pytest.raises(ValueError, match="load_mode"):
        LoadOptions.parse({"url": "x", "table": "a", "loadMode": "overwrite"})


def test_plan_roundtrip_and_slices():
    plan = Plan(
        kind="delta",
        schema="{}",
        units=[
            Unit(rows="abc", timestamp="2024-01-01 00:00:00"),
            Unit(sql="SELECT 1 AS id", split=Split("hash", ["id"], count=3)),
            Unit(sql="SELECT 2 AS id", split=Split("range", ["id"], bounds=["10", "20"])),
        ],
    )
    back = Plan.decode(plan.encode())
    assert back == plan
    assert back.slices() == [(0, 0), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2)]
    rng = back.units[2].split
    assert rng is not None
    assert [rng.predicate(i) for i in range(3)] == [
        "([id] < 10 OR [id] IS NULL)",
        "[id] >= 10 AND [id] < 20",
        "[id] >= 20",
    ]
    assert back.units[1].slice_sql(2) == (
        "SELECT * FROM (SELECT 1 AS id) AS q WHERE (CHECKSUM([id]) & 2147483647) % 3 = 2"
    )


def test_full_read_without_target(spark, source):
    register(spark)
    df = (
        spark.read.format(FORMAT)
        .option("url", source)
        .option("table", "dbo.company")
        .option("numPartitions", 2)
        .load()
    )
    assert df.columns[-3:] == ["__timestamp", "__is_deleted", "__is_full_load"]
    rows = df.orderBy("id").collect()
    assert [r["id"] for r in rows] == ["c1", "c2"]
    assert all(r["__is_full_load"] and not r["__is_deleted"] for r in rows)
    assert len({r["__timestamp"] for r in rows}) == 1
    # the connection string does not show up in query plans
    assert "pwd=" not in df._jdf.queryExecution().toString().lower()


def test_target_without_prepare_fails(spark, source):
    register(spark)
    reader = (
        spark.read.format(FORMAT)
        .option("url", source)
        .option("table", "dbo.company")
        .option("target", "/tmp/nowhere")
    )
    with pytest.raises(Exception, match="prepare"):
        reader.load().collect()
