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


def test_executors_import_only_the_reader():
    """A fresh Python worker on the executors imports the reader, the plan and ``_core``, not the
    writer stack: on Databricks, sqlglot, pydantic and the writer modules of a notebook-scoped
    library took about 19 s to import in every new worker."""
    code = (
        "import sys\n"
        "from sql2deltalake.spark._source import SqlToDeltaDataSource, _Reader\n"
        "from sql2deltalake.spark._plan import read_slice\n"
        "from sql2deltalake._conn_str import core_reads, strip_odbc_driver\n"
        "core_reads(strip_odbc_driver('Driver={x};Server=y'))\n"
        "heavy = [m for m in sys.modules if m.split('.')[0] in ('sqlglot', 'pydantic', 'arrowsql', 'deltalake')\n"
        "         or m.startswith(('sql2deltalake.reader', 'sql2deltalake.write_init', 'sql2deltalake.db_to_delta'))]\n"
        "assert not heavy, heavy\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_lazy_package_names():
    import sql2deltalake

    assert sql2deltalake.WriteConfig.__name__ == "WriteConfig"
    assert callable(sql2deltalake.make_writer)
    with pytest.raises(AttributeError):
        sql2deltalake.no_such_name  # noqa: B018


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


def test_format_without_register(spark, source):
    """The installed module pyspark_sql2deltalake registers the format in every SparkSession
    (PySpark >= 4.0 looks up the modules pyspark_*), also in one where nothing called register()
    or prepare()."""
    session = spark.newSession()
    df = session.read.format(FORMAT).option("url", source).option("table", "dbo.company").load()
    assert sorted(r["id"] for r in df.collect()) == ["c1", "c2"]


def test_prefetched_batches():
    """The read task fetches ahead in a thread: same order, the source's error reaches the
    task, and a task that stops early leaves no thread behind."""
    import threading

    from sql2deltalake.spark._plan import _prefetched

    assert list(_prefetched(iter(range(10)), 2)) == list(range(10))

    def failing():
        yield 1
        raise ValueError("source failed")

    out = []
    with pytest.raises(ValueError, match="source failed"):
        for item in _prefetched(failing(), 2):
            out.append(item)
    assert out == [1]

    before = threading.active_count()
    batches = _prefetched(iter(range(1000)), 2)
    assert next(batches) == 0
    batches.close()
    assert threading.active_count() == before
