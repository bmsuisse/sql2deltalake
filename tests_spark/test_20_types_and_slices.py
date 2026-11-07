"""Data types and sliced reads through the Spark data source, compared with the Python load."""

import pytest

from .conftest import execute
from .parity import Pair

TYPES_TABLE = """
CREATE TABLE dbo.types (
    id int NOT NULL PRIMARY KEY,
    t0 time(0), t3 time(3), t7 time,
    dto datetimeoffset(3), dt datetime, dt2 datetime2(7), d date,
    tiny tinyint, small smallint, big bigint, flag bit,
    dec decimal(18, 4), num numeric(38, 10), money_ money, fl float, re real,
    guid uniqueidentifier, bin varbinary(20), fixed char(5), txt nvarchar(50), vtxt varchar(max),
    rv rowversion
);
INSERT INTO dbo.types (id, t0, t3, t7, dto, dt, dt2, d, tiny, small, big, flag, dec, num, money_, fl, re,
                       guid, bin, fixed, txt, vtxt)
VALUES
 (1, '01:02:03', '01:02:03.456', '23:59:59.9999999', '2024-05-06 07:08:09.123 +02:00',
  '2024-01-02 03:04:05.997', '2024-01-02 03:04:05.1234567', '2024-02-29', 255, -32768,
  9223372036854775807, 1, 12345678901234.5678, 1234567890123456789012345678.0123456789, 922337203685477.5807,
  1.5e300, 3.25, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 0x00FF10, 'ab', N'Grüezi  ', 'x'),
 (2, NULL, NULL, '00:00:00', NULL, NULL, NULL, NULL, 0, NULL, NULL, 0, -0.0001, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL),
 (3, '12:00:00', '00:00:00.001', '12:34:56.0000001', '1900-01-01 00:00:00 -08:00', '1753-01-01', '0001-01-01',
  '9999-12-31', 1, 1, 1, 0, 0, 0, 0, 0, 0, '00000000-0000-0000-0000-000000000000', 0x, '', N'', '');
"""


def test_types(spark, source, tmp_path):
    execute(source, TYPES_TABLE)
    pair = Pair(spark, source, ("dbo", "types"), tmp_path)
    _, plan = pair.load()
    assert plan.kind == "full"
    pair.assert_same()
    execute(
        source,
        """UPDATE dbo.types SET t3 = '22:22:22.222', txt = N'neu ', dto = SYSDATETIMEOFFSET() WHERE id = 2;
           DELETE FROM dbo.types WHERE id = 3;
           INSERT INTO dbo.types (id, t7, tiny) VALUES (4, '01:00:00.1000000', 7)""",
    )
    _, plan = pair.load()
    assert plan.kind == "delta"
    pair.assert_same()
    row = pair.spark_df().filter("id = 1").first()
    assert (row["t0"], row["t3"], row["t7"]) == ("01:02:03", "01:02:03.456", "23:59:59.9999999")
    assert row["txt"] == "Grüezi"  # trimmed like the Python load
    assert row["tiny"] == 255


@pytest.mark.parametrize("slicing", ["hash", "range"])
def test_sliced_reads(spark, source, tmp_path, slicing):
    execute(
        source,
        """CREATE TABLE dbo.many (id int NOT NULL PRIMARY KEY, grp int, val nvarchar(20), rv rowversion);
           WITH n AS (SELECT TOP 5000 ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS i
                      FROM sys.all_objects a CROSS JOIN sys.all_objects b)
           INSERT INTO dbo.many (id, grp, val) SELECT i, i % 7, CONCAT(N'v', i) FROM n""",
    )
    options = {"numPartitions": 4}
    if slicing == "range":
        options["partitionColumn"] = "id"
    pair = Pair(spark, source, ("dbo", "many"), tmp_path, **options)
    _, plan = pair.load()
    split = plan.units[0].split
    assert split is not None and split.kind == slicing and split.slices == 4
    pair.assert_same()
    df = pair.spark_df()
    assert df.rdd.getNumPartitions() >= 1
    assert df.select("__timestamp").distinct().count() == 1  # one timestamp for all slices
    execute(
        source,
        """UPDATE dbo.many SET val = CONCAT(val, N'!') WHERE id % 50 = 0;
           DELETE FROM dbo.many WHERE id % 500 = 1;
           INSERT INTO dbo.many (id, grp, val) VALUES (6000, 1, N'new'), (6001, NULL, NULL)""",
    )
    _, plan = pair.load()
    assert plan.kind == "delta"
    assert plan.info["deletes"] == 10
    pair.assert_same()
    assert pair.spark_df().count() == 5000 + 100 + 10 + 2


@pytest.mark.parametrize("column", ["txt", "guid", "bin", "t7", "flag"])
def test_range_slices_on_an_unsupported_column_fail_in_prepare(spark, source, tmp_path, column):
    from sql2deltalake.spark import prepare

    execute(source, TYPES_TABLE)
    with pytest.raises(ValueError, match=f"partitionColumn '{column}'.*integer, decimal, float, date or datetime"):
        prepare(
            spark,
            url=source,
            table="dbo.types",
            target=str(tmp_path / "types"),
            numPartitions=4,
            partitionColumn=column,
        )


@pytest.mark.parametrize("column", ["big", "dec", "money_", "fl", "d", "dt", "dt2", "dto"])
def test_range_slices_on_numbers_and_dates(spark, source, tmp_path, column):
    from sql2deltalake.spark import prepare
    from sql2deltalake.spark._plan import Plan

    execute(source, TYPES_TABLE)
    options = prepare(
        spark,
        url=source,
        table="dbo.types",
        target=str(tmp_path / "types"),
        numPartitions=4,
        partitionColumn=column,
    )
    split = Plan.decode(options["plan"]).units[0].split
    assert split is not None and split.kind == "range" and split.slices > 1


def test_range_cuts_reject_values_without_order_arithmetic():
    import datetime

    from sql2deltalake.spark._source_load import range_cuts

    assert range_cuts(0, 100, 4) == [25, 50, 75]
    for low, high in [("a", "z"), (b"a", b"z"), (datetime.time(1), datetime.time(9))]:
        with pytest.raises(ValueError, match="integer, decimal, float, date or datetime"):
            range_cuts(low, high, 4)
