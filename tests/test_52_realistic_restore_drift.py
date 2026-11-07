"""Realistic native load tests, scenario 3: a restored source database (rowversions go backwards) and
schema drift (add, widen, drop, rename column, int -> string).

    SQL2DELTA_REALISTIC=1 scripts/run_tests_local.sh tests/test_52_realistic_restore_drift.py -x -s
"""

import os
import random
import time

import pytest

from .rt_support import DB_PREFIX, Src, check_state, connect, ensure_db, fresh_dest, load, needs_realistic

pytestmark = [pytest.mark.realistic, needs_realistic]
SEED = int(os.getenv("SQL2DELTA_RT_SEED", str(random.randrange(10**6))))
RDB = DB_PREFIX + "restore"
BAK = f"/var/opt/mssql/data/{RDB}.bak"


def _master(*stmts: str):
    c = connect("master")
    cur = c.cursor()
    for s in stmts:
        cur.execute(s)
    c.close()


def _fresh_restore_db():
    assert RDB.startswith("s2d_")
    _master(
        f"IF DB_ID('{RDB}') IS NOT NULL BEGIN ALTER DATABASE {RDB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {RDB}; END",
        f"CREATE DATABASE {RDB}",
    )


def _restore_from_backup():
    _master(
        f"ALTER DATABASE {RDB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
        f"RESTORE DATABASE {RDB} FROM DISK = '{BAK}' WITH REPLACE",
        f"ALTER DATABASE {RDB} SET MULTI_USER",
    )


def _ld(name, dest, **kw):
    return load(name, "dbo.rs", dest, db=RDB, **kw)


def test_source_restored_from_older_backup():
    _fresh_restore_db()
    s = Src(RDB)
    s.x(
        "CREATE TABLE dbo.rs (id int NOT NULL PRIMARY KEY, val nvarchar(50), n int, rv rowversion)",
        "INSERT INTO dbo.rs (id, val, n) SELECT TOP 20000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), N'orig😀', 0 FROM sys.all_objects a CROSS JOIN sys.all_objects b",
    )
    _master(f"BACKUP DATABASE {RDB} TO DISK = '{BAK}' WITH INIT, FORMAT")
    dbts_backup = s.q("SELECT CAST(@@DBTS AS bigint)")[0][0]
    dest = fresh_dest("rs")
    _ld("rs", dest)  # target synced to the state of the backup
    rng = random.Random(SEED)
    for rnd in range(3):
        # life goes on after the backup: updates, deletes, inserts, new rowversions far above the backup's
        for _ in range(4):
            s.x(
                "UPDATE dbo.rs SET val = CONCAT(N'later', id), n = n + 1 WHERE id % 5 = 0",
                "DELETE TOP (500) FROM dbo.rs WHERE id % 7 = 3",
                f"INSERT INTO dbo.rs (id, val, n) SELECT TOP 800 {100000 * (rnd + 1) + 1000 * _} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), N'new', 1 FROM sys.all_objects",
            )
            _ld("rs", dest)
            check_state(s, dest, "dbo.rs", ["id"], label=f"round {rnd} before restore")
        assert s.q("SELECT CAST(@@DBTS AS bigint)")[0][0] > dbts_backup
        s.c.close()
        _restore_from_backup()
        s = Src(RDB)
        # note: RESTORE advances @@DBTS (here by ~24000), the restored rows keep their old, lower rowversions
        r = _ld("rs", dest)
        check_state(s, dest, "dbo.rs", ["id"], label=f"round {rnd} right after restore ({r.executed_type})")
        # changes after the restore get rowversions that were already used (and loaded) before
        for k in range(5):
            s.x(
                f"UPDATE dbo.rs SET val = CONCAT(N'post{rnd}-{k}-', id) WHERE id % {rng.choice([11, 101, 1009])} = {rng.randrange(11)}",
                f"DELETE TOP (20) FROM dbo.rs WHERE id % 13 = {k}",
                f"INSERT INTO dbo.rs (id, val, n) VALUES ({900000 + rnd * 10 + k}, N'ins', 5)",
            )
            r = _ld("rs", dest)
            check_state(s, dest, "dbo.rs", ["id"], label=f"round {rnd} post-restore change {k} ({r.executed_type})")


def test_restore_with_same_rowcount_and_lower_watermark():
    """The hard case: after the restore the number of rows is unchanged but values differ, only the rowversions
    went backwards (no count mismatch to hint at it)."""
    _fresh_restore_db()
    s = Src(RDB)
    s.x(
        "CREATE TABLE dbo.rs (id int NOT NULL PRIMARY KEY, val nvarchar(50), n int, rv rowversion)",
        "INSERT INTO dbo.rs (id, val, n) SELECT TOP 5000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), N'orig', 0 FROM sys.all_objects a CROSS JOIN sys.all_objects b",
    )
    _master(f"BACKUP DATABASE {RDB} TO DISK = '{BAK}' WITH INIT, FORMAT")
    dest = fresh_dest("rs_same")
    _ld("rs_same", dest)
    s.x("UPDATE dbo.rs SET val = N'changed after backup', n = 1")
    for _ in range(3):
        s.x("UPDATE dbo.rs SET n = n + 1 WHERE id % 2 = 0")
    _ld("rs_same", dest)
    check_state(s, dest, "dbo.rs", ["id"], label="before restore")
    s.c.close()
    _restore_from_backup()
    s = Src(RDB)
    _ld("rs_same", dest)
    check_state(s, dest, "dbo.rs", ["id"], label="after restore, same row count")


# -- schema drift ----------------------------------------------------------------------------------


def _drift_setup(t: str):
    ensure_db()
    s = Src()
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, a int, b varchar(10), c decimal(10,2), rv rowversion)",
        f"INSERT INTO dbo.{t} (id, a, b, c) SELECT TOP 500 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1, 'x', 1.5 FROM sys.all_objects",
    )
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    return s, dest


def _step(s, dest, t, sql, label, **chk):
    s.x(sql)
    time.sleep(0.01)
    r = load(t, f"dbo.{t}", dest)
    s.x(f"UPDATE dbo.{t} SET a = a WHERE id = 1" if "a" in s.cols(f"dbo.{t}") else f"UPDATE dbo.{t} SET id = id WHERE id = 1")
    load(t, f"dbo.{t}", dest)
    check_state(s, dest, f"dbo.{t}", ["id"], label=f"{label} ({r.executed_type})", **chk)


def test_drift_add_column():
    t = "rt_drift_add"
    s, dest = _drift_setup(t)
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ADD d nvarchar(20) NULL", "add nullable column")
    _step(s, dest, t, f"UPDATE dbo.{t} SET d = N'new😀' WHERE id % 3 = 0", "fill new column")
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ADD e int NOT NULL DEFAULT 7", "add not null default column")
    _step(s, dest, t, f"INSERT INTO dbo.{t} (id, a, b, c, d, e) VALUES (9000, 1, 'z', 1, N'q', 9)", "insert with new columns")


@pytest.mark.xfail(strict=True, reason="B8: int -> bigint fails once a value above 2^31 arrives (the target column stays int)")
def test_drift_widen_type():
    t = "rt_drift_widen"
    s, dest = _drift_setup(t)
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ALTER COLUMN b varchar(200)", "widen varchar")
    _step(s, dest, t, f"UPDATE dbo.{t} SET b = REPLICATE('w', 150) WHERE id % 5 = 0", "use the width")
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ALTER COLUMN a bigint", "int -> bigint")
    _step(s, dest, t, f"UPDATE dbo.{t} SET a = 9000000000 WHERE id % 7 = 0", "use bigint")
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ALTER COLUMN c decimal(20,6)", "widen decimal")
    _step(s, dest, t, f"UPDATE dbo.{t} SET c = 12345678901234.123456 WHERE id % 11 = 0", "use decimal width")


@pytest.mark.xfail(strict=True, reason="B8: a dropped source column makes every later write fail (the target keeps the column)")
def test_drift_drop_column():
    t = "rt_drift_drop"
    s, dest = _drift_setup(t)
    _step(s, dest, t, f"ALTER TABLE dbo.{t} DROP COLUMN c", "drop column")
    _step(s, dest, t, f"UPDATE dbo.{t} SET a = 2 WHERE id % 2 = 0", "update after drop")
    _step(s, dest, t, f"INSERT INTO dbo.{t} (id, a, b) VALUES (9000, 1, 'z')", "insert after drop")


def test_drift_rename_column():
    t = "rt_drift_rename"
    s, dest = _drift_setup(t)
    s.x(f"EXEC sp_rename 'dbo.{t}.b', 'b_new', 'COLUMN'")
    s.x(f"UPDATE dbo.{t} SET a = 2 WHERE id % 2 = 0")
    r = load(t, f"dbo.{t}", dest)
    load(t, f"dbo.{t}", dest)
    # the rename is a drop plus an add: the old column keeps its history, the new one must carry the values
    check_state(s, dest, f"dbo.{t}", ["id"], label=f"rename ({r.executed_type})")


@pytest.mark.xfail(strict=True, reason="B8: int -> varchar fails once a non-numeric value arrives (the target column stays int)")
def test_drift_int_to_string():
    t = "rt_drift_int_str"
    s, dest = _drift_setup(t)
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ALTER COLUMN a varchar(20)", "int -> varchar")
    _step(s, dest, t, f"UPDATE dbo.{t} SET a = 'not a number' WHERE id % 3 = 0", "text values")
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ALTER COLUMN a int" if False else f"UPDATE dbo.{t} SET a = '12' WHERE id % 3 = 1", "more text")


def test_drift_string_to_int_and_pk_type():
    t = "rt_drift_str_int"
    s, dest = _drift_setup(t)
    s.x(f"UPDATE dbo.{t} SET b = '5'")
    load(t, f"dbo.{t}", dest)
    _step(s, dest, t, f"ALTER TABLE dbo.{t} ALTER COLUMN b int", "varchar -> int")
    _step(s, dest, t, f"UPDATE dbo.{t} SET b = 99 WHERE id % 2 = 0", "int values")


@pytest.mark.xfail(strict=True, reason="B8: a primary key widened int -> bigint fails once a key above 2^31 arrives")
def test_drift_pk_int_to_bigint():
    t = "rt_drift_pk"
    ensure_db()
    s = Src()
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (id int NOT NULL, a int, rv rowversion)", f"INSERT INTO dbo.{t} (id, a) SELECT TOP 300 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1 FROM sys.all_objects", f"ALTER TABLE dbo.{t} ADD CONSTRAINT pk_{t} PRIMARY KEY (id)")
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    s.x(f"ALTER TABLE dbo.{t} DROP CONSTRAINT pk_{t}", f"ALTER TABLE dbo.{t} ALTER COLUMN id bigint NOT NULL", f"ALTER TABLE dbo.{t} ADD CONSTRAINT pk_{t} PRIMARY KEY (id)")
    s.x(f"INSERT INTO dbo.{t} (id, a) VALUES (5000000000, 2)", f"UPDATE dbo.{t} SET a = 3 WHERE id < 10", f"DELETE FROM dbo.{t} WHERE id = 20")
    r = load(t, f"dbo.{t}", dest)
    s.x(f"UPDATE dbo.{t} SET a = 4 WHERE id = 5000000000")
    load(t, f"dbo.{t}", dest)
    check_state(s, dest, f"dbo.{t}", ["id"], label=f"pk int -> bigint ({r.executed_type})")
