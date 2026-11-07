"""Realistic native load tests, scenario 1: every supported SQL Server type, random cycles of
insert/update/delete, delta-column modes and key edge cases. Opt-in:

    SQL2DELTA_REALISTIC=1 scripts/run_tests_local.sh tests/test_50_realistic_native.py -x -s

`SQL2DELTA_RT_SEED` fixes the random seed (default: printed random seed), `SQL2DELTA_RT_CYCLES` the number
of cycles (default 30).
"""

import os
import time
import random

import pytest

from .rt_support import (
    current_state,
    Src,
    check_state,
    ensure_db,
    fresh_dest,
    latest_pk_keys,
    load,
    needs_realistic,
)

pytestmark = [pytest.mark.realistic, needs_realistic]
CYCLES = int(os.getenv("SQL2DELTA_RT_CYCLES", "30"))
SEED = int(os.getenv("SQL2DELTA_RT_SEED", str(random.randrange(10**6))))

# sql_variant, hierarchyid, text/ntext are in test_54 (they fail the load, one table each)
WIDE_DDL = """CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, ti tinyint, si smallint, bi bigint, b bit,
 d38 decimal(38,10), d38_0 decimal(38,0), n18 numeric(18,2), mny money, smny smallmoney, f float, r real,
 dt date, t0 time(0), t3 time(3), t7 time(7), dtm datetime, dt2_0 datetime2(0), dt2_3 datetime2(3), dt2_7 datetime2(7),
 sdt smalldatetime, dto0 datetimeoffset(0), dto7 datetimeoffset(7),
 ch char(5), vc varchar(20), vcm varchar(max), nch nchar(5), nvc nvarchar(max), vb varbinary(20), vbm varbinary(max), bn binary(4),
 uid uniqueidentifier, x xml, geo geography, rv rowversion)"""

EMOJI = "N'a😀b𝄞c'"


def _lit(rng: random.Random) -> dict[str, str]:
    def n(p, v):
        return "NULL" if rng.random() < p else v

    i = rng.randrange(-10**6, 10**6)
    return {
        "ti": n(0.2, str(rng.randrange(256))),
        "si": n(0.2, str(rng.randrange(-32768, 32768))),
        "bi": n(0.2, str(rng.choice([rng.randrange(-2**63, 2**63), 2**63 - 1, -2**63, 0]))),
        "b": n(0.2, str(rng.randrange(2))),
        "d38": n(0.2, f"{rng.randrange(-10**27, 10**27)}.{rng.randrange(10**10):010d}"),
        "d38_0": n(0.2, str(rng.choice([10**37, -10**36, 0, rng.randrange(-10**30, 10**30)]))),
        "n18": n(0.2, f"{rng.randrange(-10**15, 10**15)}.{rng.randrange(100):02d}"),
        "mny": n(0.2, f"{rng.randrange(-10**14, 10**14)}.{rng.randrange(10000):04d}"),
        "smny": n(0.2, f"{rng.randrange(-200000, 200000)}.{rng.randrange(10000):04d}"),
        "f": n(0.2, rng.choice(["0", "1.5e300", "-2.2250738585072014E-308", repr(rng.random() * 1e10)])),
        "r": n(0.2, rng.choice(["0", "3.4e38", "-1.17549435E-38", repr(round(rng.random() * 1e5, 2))])),
        "dt": n(0.2, f"'{rng.choice(['0001-01-01', '9999-12-31', f'{rng.randrange(1900, 2100)}-0{rng.randrange(1, 10)}-1{rng.randrange(10)}'])}'"),
        "t0": n(0.2, f"'{rng.randrange(24):02d}:{rng.randrange(60):02d}:{rng.randrange(60):02d}'"),
        "t3": n(0.2, f"'{rng.randrange(24):02d}:{rng.randrange(60):02d}:{rng.randrange(60):02d}.{rng.randrange(1000):03d}'"),
        "t7": n(0.2, f"'{rng.randrange(24):02d}:{rng.randrange(60):02d}:{rng.randrange(60):02d}.{rng.randrange(10**7):07d}'"),
        "dtm": n(0.2, f"'{rng.randrange(1753, 9999)}-0{rng.randrange(1, 10)}-1{rng.randrange(10)} 0{rng.randrange(10)}:00:00.{rng.choice(['000', '003', '007'])}'"),
        "dt2_0": n(0.2, f"'{rng.randrange(1, 9999):04d}-06-15 10:11:12'"),
        "dt2_3": n(0.2, f"'{rng.randrange(1, 9999):04d}-06-15 10:11:12.{rng.randrange(1000):03d}'"),
        "dt2_7": n(0.2, f"'{rng.randrange(1, 9999):04d}-06-15 10:11:12.{rng.randrange(10**7):07d}'"),
        "sdt": n(0.2, f"'{rng.randrange(1900, 2078)}-06-15 10:{rng.randrange(60):02d}'"),
        "dto0": n(0.2, f"'2020-0{rng.randrange(1, 10)}-05 10:00:00 {rng.choice(['+', '-'])}0{rng.randrange(9)}:30'"),
        "dto7": n(0.2, f"'2020-0{rng.randrange(1, 10)}-05 10:00:00.{rng.randrange(10**7):07d} {rng.choice(['+', '-'])}0{rng.randrange(9)}:00'"),
        "ch": n(0.2, f"'{rng.choice(['', 'a', 'ab', 'abcde'])}'"),
        "vc": n(0.2, "'" + rng.choice(["", "x", "it''s", " lead", "trail ", "tab\t"]) + "'"),
        "vcm": n(0.2, f"REPLICATE('v', {rng.choice([0, 1, 9000, 20000])})"),
        "nch": n(0.2, f"N'{rng.choice(['', 'é', 'äöü', 'ab'])}'"),
        "nvc": n(0.2, rng.choice([EMOJI, "N''", "N' '", "N'日本語'", f"REPLICATE(N'😀', {rng.choice([1, 5000])})", "NULL"])),
        "vb": n(0.2, rng.choice(["0x", "0x00", "0xFF00FF", f"0x{rng.randrange(16**8):08X}"])),
        "vbm": n(0.2, f"CAST(REPLICATE(CAST('b' AS varchar(max)), {rng.choice([0, 1, 9000])}) AS varbinary(max))"),
        "bn": n(0.2, f"0x{rng.randrange(16**8):08X}"),
        "uid": n(0.2, "NEWID()"),
        "x": n(0.2, rng.choice(["'<a>1</a>'", "'<a b=\"1\"><c/></a>'", f"'<r>{i}</r>'"])),
        "geo": n(0.2, f"geography::Point({rng.randrange(-90, 90)}, {rng.randrange(-180, 180)}, 4326)"),
    }


def _insert_one(s: Src, rng: random.Random, t: str, id_: int):
    lit = _lit(rng)
    cols = ["id"] + list(lit)
    s.x(f"INSERT INTO dbo.{t} ({', '.join(cols)}) VALUES ({id_}, {', '.join(lit.values())})")


def _insert_batch(s: Src, t: str, start: int, n: int):
    s.x(
        f"""WITH nn AS (SELECT TOP ({n}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
              FROM sys.all_objects a CROSS JOIN sys.all_objects b)
            INSERT INTO dbo.{t} (id, ti, si, bi, b, d38, n18, mny, f, dt, dt2_7, vc, nvc, vb, uid, x)
            SELECT {start} + i, i % 256, i % 30000 - 15000, i * 1000003, i % 2, i * 1.0000000001, i / 7.0, i * 1.5, i / 3.0,
              DATEADD(day, i % 5000, '2000-01-01'), DATEADD(microsecond, i, CAST('2020-01-01' AS datetime2(7))), CONCAT('v', i),
              CONCAT(N'😀', i), CAST(i AS varbinary(8)), NEWID(), CONCAT('<r>', i, '</r>') FROM nn"""
    )


def _cycle(s: Src, rng: random.Random, t: str, state: dict, cyc: int) -> str:
    ids: list[int] = [r[0] for r in s.q(f"SELECT TOP 2000 id FROM dbo.{t} ORDER BY NEWID()")]
    ops = rng.sample(
        ["single_ins", "batch_ins", "update_few", "update_many", "noop_update", "pk_update",
         "delete_few", "delete_many", "reinsert", "null_flip", "big_str"],
        rng.randrange(1, 5),
    )
    for op in ops:
        if op == "single_ins":
            for _ in range(rng.randrange(1, 5)):
                state["next"] += 1
                _insert_one(s, rng, t, state["next"])
        elif op == "batch_ins":
            n = rng.choice([10000, 2500])
            _insert_batch(s, t, state["next"], n)
            state["next"] += n
        elif op == "update_few" and ids:
            lit = _lit(rng)
            col = rng.choice(list(lit))
            s.x(f"UPDATE dbo.{t} SET {col} = {lit[col]} WHERE id IN ({', '.join(map(str, ids[:5]))})")
        elif op == "update_many" and ids:
            s.x(f"UPDATE dbo.{t} SET si = si + 1, vc = CONCAT('u{cyc}', id) WHERE id % {rng.choice([3, 50, 1000])} = {rng.randrange(3)}")
        elif op == "noop_update" and ids:
            s.x(f"UPDATE dbo.{t} SET si = si WHERE id IN ({', '.join(map(str, ids[:50]))})")
        elif op == "pk_update" and ids:
            state["next"] += 1
            s.x(f"UPDATE dbo.{t} SET id = {state['next']} WHERE id = {ids[0]}")
        elif op == "delete_few" and ids:
            s.x(f"DELETE FROM dbo.{t} WHERE id IN ({', '.join(map(str, ids[:3]))})")
            state["deleted"] = ids[:3]
        elif op == "delete_many":
            s.x(f"DELETE TOP (3000) FROM dbo.{t} WHERE id % 7 = {rng.randrange(7)}")
        elif op == "reinsert" and state.get("deleted"):
            for i in state.pop("deleted"):
                try:
                    _insert_one(s, rng, t, i)
                except Exception:  # key still there
                    pass
        elif op == "null_flip" and ids:
            s.x(
                f"UPDATE dbo.{t} SET vc = CASE WHEN vc IS NULL THEN 'was null' ELSE NULL END, "
                f"nvc = CASE WHEN nvc IS NULL THEN {EMOJI} ELSE NULL END, d38 = CASE WHEN d38 IS NULL THEN 1 ELSE NULL END "
                f"WHERE id IN ({', '.join(map(str, ids[:200]))})"
            )
        elif op == "big_str" and ids:
            s.x(f"UPDATE dbo.{t} SET nvc = REPLICATE(N'𝄞x', {rng.choice([10, 100000])}) WHERE id = {ids[0]}")
    return "+".join(ops)


def test_wide_table_random_cycles():
    ensure_db()
    s = Src()
    t = "rt_wide"
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", WIDE_DDL.format(t=t))
    rng = random.Random(SEED)
    print(f"seed {SEED}")
    state = {"next": 1000}
    for i in range(1, 30):
        _insert_one(s, rng, t, i)
    s.x(f"INSERT INTO dbo.{t} (id) VALUES (0)")  # an all-NULL row
    s.x(f"INSERT INTO dbo.{t} (id, nvc, vc, vcm, nch) VALUES (-1, N'', '', '', N'')")
    dest = fresh_dest("wide")
    r = load("wide", f"dbo.{t}", dest)
    assert r.executed_type == "full"
    check_state(s, dest, f"dbo.{t}", ["id"], label="full")
    for cyc in range(CYCLES):
        ops = _cycle(s, rng, t, state, cyc)
        r = load("wide", f"dbo.{t}", dest)
        check_state(s, dest, f"dbo.{t}", ["id"], label=f"cycle {cyc} ({ops}) -> {r.executed_type}")
        if cyc % 5 == 4:  # nothing changed: nothing to load, and still equal
            r2 = load("wide", f"dbo.{t}", dest)
            assert r2.executed_type == "none", f"cycle {cyc}: unchanged source loaded as {r2.executed_type}"
    n = s.q(f"SELECT COUNT(*) FROM dbo.{t}")[0][0]
    assert latest_pk_keys(dest) == n, "latest_pk_version row count differs from the source"


# -- datetime delta column modes -------------------------------------------------------------------


def _dt_table(s: Src, t: str):
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, val nvarchar(50), amount decimal(18,4), modified datetime2(3) NOT NULL)",
    )


@pytest.mark.parametrize("mode", ["simple_delta", "simple_delta_check", "append_inserts"])
def test_datetime_delta_modes(mode: str):
    ensure_db()
    s = Src()
    t = f"rt_dtd_{mode}"
    _dt_table(s, t)
    rng = random.Random(SEED)
    nxt = 1
    dest = fresh_dest(t)
    s.x(f"INSERT INTO dbo.{t} SELECT TOP 500 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'v0', 1.5, SYSUTCDATETIME() FROM sys.all_objects")
    nxt = 501
    load(t, f"dbo.{t}", dest, delta_col="modified", primary_keys=["id"], load_mode=mode)
    check_state(s, dest, f"dbo.{t}", ["id"], label=f"{mode} full", deletes=mode != "simple_delta")
    for cyc in range(CYCLES):
        time.sleep(0.05)
        k = rng.choice(["ins", "ins_many", "upd", "del"] if mode == "simple_delta_check" else (["ins", "ins_many"] if mode == "append_inserts" else ["ins", "ins_many", "upd"]))
        if k == "ins":
            for _ in range(rng.randrange(1, 4)):
                s.x(f"INSERT INTO dbo.{t} VALUES ({nxt}, N'c{cyc}😀', {rng.random():.4f}, SYSUTCDATETIME())")
                nxt += 1
        elif k == "ins_many":
            s.x(f"INSERT INTO dbo.{t} SELECT TOP 3000 {nxt} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'bulk', 2.5, SYSUTCDATETIME() FROM sys.all_objects")
            nxt += 3000
        elif k == "upd":
            s.x(f"UPDATE TOP (20) dbo.{t} SET val = N'u{cyc}', modified = SYSUTCDATETIME() WHERE id % 13 = {cyc % 13}")
        elif k == "del":
            s.x(f"DELETE TOP (30) FROM dbo.{t} WHERE id % 11 = {cyc % 11}")
        load(t, f"dbo.{t}", dest, delta_col="modified", primary_keys=["id"], load_mode=mode)
        check_state(s, dest, f"dbo.{t}", ["id"], label=f"{mode} cycle {cyc} ({k})", deletes=mode != "simple_delta")


# -- key edge cases --------------------------------------------------------------------------------


def test_composite_pk():
    ensure_db()
    s = Src()
    t = "rt_cpk"
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (a int NOT NULL, b nvarchar(20) NOT NULL, c date NOT NULL, v int, rv rowversion, PRIMARY KEY (a, b, c))",
        f"INSERT INTO dbo.{t} (a,b,c,v) VALUES (1,N'x','2020-01-01',1),(1,N'x','2020-01-02',2),(1,N'y','2020-01-01',3),(2,N'x','2020-01-01',4),(-5,N'😀','2020-01-01',5)",
    )
    dest = fresh_dest(t)
    rng = random.Random(SEED)
    load(t, f"dbo.{t}", dest)
    check_state(s, dest, f"dbo.{t}", ["a", "b", "c"], label="full")
    for cyc in range(CYCLES):
        k = rng.choice(["upd", "del", "ins", "pk", "batch"])
        a, b = rng.randrange(1, 4), rng.choice(["x", "y", "z", "😀"])
        if k == "upd":
            s.x(f"UPDATE dbo.{t} SET v = v + 1 WHERE a = {a}")
        elif k == "del":
            s.x(f"DELETE FROM dbo.{t} WHERE a = {a} AND b = N'{b}'")
        elif k == "ins":
            s.x(f"IF NOT EXISTS (SELECT 1 FROM dbo.{t} WHERE a={a} AND b=N'{b}' AND c='2021-01-01') INSERT INTO dbo.{t}(a,b,c,v) VALUES ({a},N'{b}','2021-01-01',{cyc})")
        elif k == "pk":
            s.x(f"UPDATE TOP (1) dbo.{t} SET b = CONCAT(b, N'p') WHERE a = {a}")
        else:
            s.x(f"INSERT INTO dbo.{t}(a,b,c,v) SELECT TOP 2000 100+{cyc}, CONCAT(N'b', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))), '2022-02-02', 1 FROM sys.all_objects")
        load(t, f"dbo.{t}", dest)
        check_state(s, dest, f"dbo.{t}", ["a", "b", "c"], label=f"cycle {cyc} ({k})")


def test_string_pk_spaces_and_case():
    """Case-sensitive collation keeps 'a'/'A' apart; leading spaces ' a' vs 'a' collapse to one key after TRIM."""
    ensure_db()
    s = Src()
    t = "rt_spk"
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (k nvarchar(20) COLLATE Latin1_General_CS_AS NOT NULL PRIMARY KEY, v int, rv rowversion)",
        f"INSERT INTO dbo.{t} (k,v) VALUES (N'a',1),(N'A',2),(N'b',3),(N'B ',4),(N'ä',5),(N'a  ',6)" if False else
        f"INSERT INTO dbo.{t} (k,v) VALUES (N'a',1),(N'A',2),(N'b',3),(N'B',4),(N'ä',5),(N'Ä',6),(N'é',7)",
    )
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    check_state(s, dest, f"dbo.{t}", ["k"], label="full")
    for step, sql in enumerate(
        [
            f"UPDATE dbo.{t} SET v = 20 WHERE k = N'A'",
            f"DELETE FROM dbo.{t} WHERE k = N'a'",
            f"UPDATE dbo.{t} SET v = 30 WHERE k = N'b'",
            f"INSERT INTO dbo.{t} (k,v) VALUES (N'a',100)",
            f"UPDATE dbo.{t} SET k = N'a ' WHERE k = N'a'",  # trailing space: same key after padding compare
            f"UPDATE dbo.{t} SET v = 31 WHERE k = N'B'",
            f"DELETE FROM dbo.{t} WHERE k = N'Ä'",
        ]
    ):
        time.sleep(0.01)
        s.x(sql)
        load(t, f"dbo.{t}", dest)
        check_state(s, dest, f"dbo.{t}", ["k"], label=f"step {step}: {sql}")


def test_string_pk_leading_space_collision():
    ensure_db()
    s = Src()
    t = "rt_spk2"
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (k varchar(20) NOT NULL PRIMARY KEY, v int, rv rowversion)",
        f"INSERT INTO dbo.{t} (k,v) VALUES ('x',1),(' x',2),('  x',3),('y',4)",
    )
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    rows = [r for r in delta_rows_(dest)]
    assert len(rows) == 4, rows
    s.x(f"UPDATE dbo.{t} SET v = 22 WHERE k = ' x'")
    load(t, f"dbo.{t}", dest)
    s.x(f"DELETE FROM dbo.{t} WHERE k = 'y'")
    load(t, f"dbo.{t}", dest)
    cur = {}
    for r in sorted(delta_rows_(dest), key=lambda r: r["__timestamp"]):
        cur.setdefault(r["k"], []).append(r)
    # three source rows share the key 'x' after trim; the target cannot tell them apart, but the deleted 'y' must be marked
    assert cur["y"][-1]["__is_deleted"] is True
    assert not any(r["__is_deleted"] for r in cur["x"]), "a live trimmed-equal key was marked deleted"


def delta_rows_(dest):
    from .rt_support import delta_rows

    return delta_rows(dest)


@pytest.mark.parametrize(
    "pk_type",
    [
        "int",
        "bigint",
        pytest.param(
            "decimal(38,0)",
            marks=pytest.mark.xfail(strict=True, reason="B6: delta-rs 0.18.1 statistics of decimal(38,0) near 1e38"),
        ),
        "uniqueidentifier",
        "datetime2(7)",
        "binary(8)",
    ],
)
def test_extreme_pk_values(pk_type: str):
    ensure_db()
    s = Src()
    t = "rt_xpk_" + pk_type.replace("(", "_").replace(")", "").replace(",", "_")
    vals = {
        "int": ["-2147483648", "-1", "0", "1", "2147483647"],
        "bigint": ["-9223372036854775808", "-9007199254740993", "-1", "0", "2147483648", "4294967296", "9007199254740992", "9007199254740993", "9007199254740995", "9223372036854775807"],
        "decimal(38,0)": ["-99999999999999999999999999999", "0", "9007199254740993", "9007199254740995", "99999999999999999999999999999999999999"],
        "uniqueidentifier": ["'00000000-0000-0000-0000-000000000000'", "'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'", "NEWID()"],
        "datetime2(7)": ["'0001-01-01'", "'9999-12-31 23:59:59.9999999'", "'2020-01-01 00:00:00.0010001'", "'2020-01-01 00:00:00.0020002'"],
        "binary(8)": ["0x0000000000000000", "0xFFFFFFFFFFFFFFFF", "0x0000000000000001", "0x0000000000000100"],
    }[pk_type]
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (k {pk_type} NOT NULL PRIMARY KEY, v int, rv rowversion)")
    for i, v in enumerate(vals):
        s.x(f"INSERT INTO dbo.{t} (k, v) VALUES ({v}, {i})")
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    check_state(s, dest, f"dbo.{t}", ["k"], label="full")
    for step in range(6):
        k = s.q(f"SELECT TOP 1 CAST(k AS varchar(50)) FROM dbo.{t} ORDER BY NEWID()")
        if step % 3 == 0:
            s.x(f"UPDATE dbo.{t} SET v = v + 100 WHERE k = (SELECT TOP 1 k FROM dbo.{t} ORDER BY k)")
            s.x(f"UPDATE dbo.{t} SET v = v + 100 WHERE k = (SELECT TOP 1 k FROM dbo.{t} ORDER BY k DESC)")
        elif step % 3 == 1:
            s.x(f"DELETE FROM dbo.{t} WHERE k = (SELECT TOP 1 k FROM dbo.{t} ORDER BY k DESC)")
        else:
            s.x(f"UPDATE dbo.{t} SET v = -v WHERE k = (SELECT TOP 1 k FROM dbo.{t} ORDER BY k)")
        load(t, f"dbo.{t}", dest)
        check_state(s, dest, f"dbo.{t}", ["k"], label=f"step {step}")


@pytest.mark.xfail(
    strict=True,
    reason="B4: Delta timestamps keep microseconds, so datetime2(7) keys 100 ns apart become one key",
)
def test_datetime2_7_pk_differing_in_100ns():
    """B4 (open): Delta stores microseconds, so two datetime2(7) keys that differ only in the 7th digit become one
    key."""
    ensure_db()
    s = Src()
    t = "rt_dt27_pk"
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (k datetime2(7) NOT NULL PRIMARY KEY, v int, rv rowversion)",
        f"INSERT INTO dbo.{t} (k, v) VALUES ('2020-01-01 00:00:00.0000001', 1), ('2020-01-01 00:00:00.0000002', 2)",
    )
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    s.x(f"UPDATE dbo.{t} SET v = 22 WHERE v = 2")
    load(t, f"dbo.{t}", dest)
    current, _ = current_state(dest, ["k"])
    assert len(current) == 2, f"two source keys, {len(current)} target key(s)"
    check_state(s, dest, f"dbo.{t}", ["k"], label="100ns keys")


def test_no_pk_is_a_full_load_each_time():
    ensure_db()
    s = Src()
    t = "rt_nopk"
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (a int, b int, rv rowversion)", f"INSERT INTO dbo.{t} (a,b) VALUES (1,1),(1,1),(2,2)")
    dest = fresh_dest(t)
    for sql in [None, f"UPDATE dbo.{t} SET b = 9 WHERE a = 2", f"DELETE FROM dbo.{t} WHERE a = 1"]:
        if sql:
            s.x(sql)
        r = load(t, f"dbo.{t}", dest)
        assert r.executed_type == "full"
        rows = delta_rows_(dest)
        last = max(r["__timestamp"] for r in rows)
        got = sorted((r["a"], r["b"]) for r in rows if r["__timestamp"] == last and not r["__is_deleted"])
        assert got == sorted(s.q(f"SELECT a, b FROM dbo.{t}")), (sql, got)


@pytest.mark.xfail(
    strict=True,
    reason="B6: delta-rs 0.18.1 fails on the statistics of decimal(38,0) values of about +-1e37 and more",
)
@pytest.mark.parametrize("value", [-(10**37), -(10**38 - 1)])
def test_decimal38_large_negative_value(value: int):
    """B6 (open, upstream delta-rs 0.18.1, stats parsing): a decimal(38,0) of about -1e37 fails the write."""
    ensure_db()
    s = Src()
    t = "rt_dec38_neg"
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, d decimal(38,0), rv rowversion)", f"INSERT INTO dbo.{t} (id, d) VALUES (1, {value})")
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    check_state(s, dest, f"dbo.{t}", ["id"], label="full")
