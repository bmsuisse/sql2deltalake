"""String keys are trimmed by default (both ends, like the Python load): keys that differ only by
leading blanks, distinct in SQL Server, become the same key in Delta. prepare() warns about it,
``noTrim`` keeps them apart."""

import logging

from sql2deltalake.spark import run

from .conftest import execute

TABLE = """CREATE TABLE dbo.tk (k varchar(20) NOT NULL PRIMARY KEY, v int NOT NULL, rv rowversion);
           INSERT INTO dbo.tk (k, v) VALUES ('lead', 1), (' lead', 2), ('other', 3)"""


def _keys(spark, target):
    return sorted(r["k"] for r in spark.read.format("delta").load(target).collect())


def test_keys_differing_by_leading_blanks_warn(spark, source, tmp_path, caplog):
    execute(source, TABLE)
    target = str(tmp_path / "tk")
    run(spark, url=source, table="dbo.tk", target=target)
    assert _keys(spark, target) == ["lead", "lead", "other"]  # collided
    execute(source, "UPDATE dbo.tk SET v = 4 WHERE k = 'other'")
    with caplog.at_level(logging.WARNING, logger="sql2deltalake.spark"):
        assert run(spark, url=source, table="dbo.tk", target=target) == "delta"
    assert any("noTrim" in r.getMessage() for r in caplog.records), caplog.text


def test_no_trim_keeps_keys_apart(spark, source, tmp_path, caplog):
    execute(source, TABLE)
    target = str(tmp_path / "tk")
    run(spark, url=source, table="dbo.tk", target=target, noTrim="true")
    assert _keys(spark, target) == [" lead", "lead", "other"]
    execute(source, "UPDATE dbo.tk SET v = 4 WHERE k = ' lead'")
    with caplog.at_level(logging.WARNING, logger="sql2deltalake.spark"):
        assert run(spark, url=source, table="dbo.tk", target=target, noTrim="true") == "delta"
    assert not any("noTrim" in r.getMessage() for r in caplog.records), caplog.text
    latest = {}
    for r in sorted(spark.read.format("delta").load(target).collect(), key=lambda r: r["__timestamp"]):
        latest[r["k"]] = r["v"]
    assert latest == {" lead": 4, "lead": 1, "other": 3}
