"""A source column whose type changed after the first load: widened (the target column is
migrated), narrowed (the values are cast to the target's type) or neither (a clear error before
anything is written)."""

import decimal

import pytest

from sql2deltalake.spark import FORMAT, commit, prepare
from sql2deltalake.spark._driver import _Delta
from sql2deltalake.spark._plan import Plan

from .conftest import execute

TABLE = """CREATE TABLE dbo.tc (id int NOT NULL CONSTRAINT pk_tc PRIMARY KEY, qty int, big bigint,
                                val decimal(18, 2), rv rowversion);
           INSERT INTO dbo.tc (id, qty, big, val)
           VALUES (1, 10, 100, 1.25), (2, 20, 200, 2.50), (3, NULL, NULL, NULL)"""


def _load(spark, source, target):
    options = prepare(spark, url=source, table="dbo.tc", target=target)
    plan = Plan.decode(options["plan"])
    if plan.kind != "unchanged":
        writer = spark.read.format(FORMAT).options(**options).load().write.format("delta")
        writer = writer.mode("append")
        if plan.info.get("merge_schema"):
            writer = writer.option("mergeSchema", "true")
        writer.save(target)
        commit(spark, options)
    return plan


def _current(spark, target):
    rows = spark.read.format("delta").load(target).collect()
    latest = {}
    for r in sorted(rows, key=lambda r: r["__timestamp"]):
        latest[r["id"]] = None if r["__is_deleted"] else (r["qty"], r["big"], r["val"])
    return {k: v for k, v in latest.items() if v is not None}


def _types(spark, target):
    return {f.name: f.dataType.simpleString() for f in spark.read.format("delta").load(target).schema}


@pytest.mark.parametrize(
    "alter, column, new_type, kind",
    [
        ("ALTER COLUMN qty bigint NULL", "qty", "bigint", "delta"),
        ("ALTER COLUMN val decimal(20, 4) NULL", "val", "decimal(20,4)", "delta"),
        # narrowed: the target keeps its type, the new values are cast to it
        ("ALTER COLUMN big int NULL", "big", "bigint", "delta"),
        # a key column: the state holds the old type, so a full load
        (
            "DROP CONSTRAINT pk_tc; ALTER TABLE dbo.tc ALTER COLUMN id bigint NOT NULL; "
            "ALTER TABLE dbo.tc ADD CONSTRAINT pk_tc PRIMARY KEY (id)",
            "id",
            "bigint",
            "full",
        ),
    ],
    ids=["int-bigint", "decimal-wider", "bigint-int", "key-int-bigint"],
)
def test_changed_column_type_keeps_loading(spark, source, tmp_path, alter, column, new_type, kind):
    execute(source, TABLE)
    target = str(tmp_path / "tc")
    assert _load(spark, source, target).kind == "full"
    execute(
        source,
        f"ALTER TABLE dbo.tc {alter}",
        "UPDATE dbo.tc SET qty = 11, val = 1.5 WHERE id = 1",
        "INSERT INTO dbo.tc (id, qty, big, val) VALUES (4, 40, 400, 4.75)",
    )
    assert _load(spark, source, target).kind == kind
    assert _types(spark, target)[column] == new_type
    execute(source, "UPDATE dbo.tc SET qty = 21 WHERE id = 2", "DELETE FROM dbo.tc WHERE id = 3")
    assert _load(spark, source, target).kind == "delta"
    assert _load(spark, source, target).kind == "unchanged"
    d = decimal.Decimal
    assert {k: (q, b, v and v.normalize()) for k, (q, b, v) in _current(spark, target).items()} == {
        1: (11, 100, d("1.5")),
        2: (21, 200, d("2.5")),
        4: (40, 400, d("4.75")),
    }


def test_type_change_that_would_lose_values_fails_before_writing(spark, source, tmp_path):
    """decimal(18,2) to decimal(18,4): neither type holds every value of the other."""
    execute(source, TABLE)
    target = str(tmp_path / "tc")
    _load(spark, source, target)
    version = _Delta(spark, target).version()
    execute(
        source,
        "ALTER TABLE dbo.tc ALTER COLUMN val decimal(18, 4) NULL",
        "UPDATE dbo.tc SET qty = 11 WHERE id = 1",
    )
    with pytest.raises(ValueError, match="val changed from decimal\\(18,2\\) to decimal\\(18,4\\)"):
        prepare(spark, url=source, table="dbo.tc", target=target)
    assert _Delta(spark, target).version() == version
