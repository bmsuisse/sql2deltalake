"""Support for the opt-in realistic tests against SQL Server (tests/test_5*_realistic_*.py).

Environment: `DB_CONNECTION_STRING` or `ODBC_MASTER_CONN` (a master connection, never printed), the
database is `<ODBCLAKE_TEST_DB_PREFIX or s2d_rt_>native` and only this database is ever touched.
Source of truth check: `check_state` materialises the current state from the append-only Delta
history (latest row per key, delete markers dropped) and diffs it against `SELECT *` of the source.
"""

import datetime as dt
import decimal
import os
import shutil
import struct
import time
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import pytest

REALISTIC = os.getenv("SQL2DELTA_REALISTIC") == "1"
DB_PREFIX = os.getenv("ODBCLAKE_TEST_DB_PREFIX", "s2d_rt_")
DB_NAME = DB_PREFIX + "native"
DATA = Path(os.getenv("SQL2DELTA_RT_DATA", "tests/_data/rt"))

needs_realistic = pytest.mark.skipif(
    not REALISTIC, reason="realistic native tests, set SQL2DELTA_REALISTIC=1"
)


def _master() -> str:
    return os.environ.get("DB_CONNECTION_STRING") or os.environ["ODBC_MASTER_CONN"]


def conn_str(db: str = DB_NAME) -> str:
    cs = _master()
    return cs.replace("Database=master", "Database=" + db).replace(
        "database=master", "database=" + db
    )


def connect(db: str = DB_NAME, autocommit: bool = True):
    import arrowsql
    from sql2deltalake.reader.arrowsql_reader import strip_odbc_driver

    return arrowsql.connect(strip_odbc_driver(conn_str(db)), autocommit=autocommit)


def ensure_db():
    assert DB_NAME.startswith("s2d_"), "the realistic tests only touch databases named s2d_*"
    with connect("master") as c:
        cur = c.cursor()
        cur.execute(f"IF DB_ID('{DB_NAME}') IS NULL CREATE DATABASE {DB_NAME}")
        cur.execute(f"ALTER DATABASE {DB_NAME} SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE")


class Src:
    """A long-lived autocommit connection with a few helpers."""

    def __init__(self, db: str = DB_NAME):
        self.db = db
        self.c = connect(db)

    def x(self, *stmts: str):
        cur = self.c.cursor()
        for s in stmts:
            cur.execute(s)
        return cur

    def q(self, sql: str) -> list[tuple]:
        cur = self.c.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]

    def cols(self, table: str) -> list[str]:
        cur = self.c.cursor()
        cur.execute(f"SELECT TOP 0 * FROM {table}")
        return [d[0] for d in cur.description]

    def spid(self) -> int:
        return self.q("SELECT @@SPID")[0][0]


def fresh_dest(name: str) -> Any:
    from sql2deltalake.destination.file_system import FileSystemDestination

    p = DATA / name
    shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True)
    return FileSystemDestination(p)


def reader(name: str, db: str = DB_NAME, **kw):
    from sql2deltalake.reader.arrowsql_reader import ArrowSQLReader

    DATA.mkdir(parents=True, exist_ok=True)
    return ArrowSQLReader(conn_str(db), str(DATA / f"{name}.duckdb"), source_dialect="tsql", **kw)


def load(name: str, table: str, dest, db: str = DB_NAME, **cfg):
    from sql2deltalake import WriteConfig, write_db_to_delta

    schema, tbl = table.split(".")
    return write_db_to_delta(
        reader(name, db), (schema, tbl), dest, WriteConfig(dialect="tsql", **cfg)
    )


# -- normalisation --------------------------------------------------------------------------------


def _f32(v: float) -> float:
    return struct.unpack("f", struct.pack("f", v))[0]


def norm(v: Any, *, trim: bool = True) -> Any:
    """A canonical, hashable form of a source value or a Delta value."""
    if v is None:
        return None
    if isinstance(v, bool):
        return ("b", v)
    if type(v).__name__ == "UUID":
        return ("s", str(v).upper())
    if isinstance(v, (int,)):
        return ("n", decimal.Decimal(v))
    if isinstance(v, decimal.Decimal):
        return ("n", v.normalize() if v != 0 else decimal.Decimal(0))
    if isinstance(v, float):
        return ("f", _f32(v))  # real and float meet at float32 precision, see norm_pair
    if isinstance(v, str):
        return ("s", v.strip(" ") if trim else v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return ("x", bytes(v))
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return ("dt", v.replace(microsecond=v.microsecond))
    if isinstance(v, dt.date):
        return ("d", v)
    if isinstance(v, dt.time):
        return ("t", v.isoformat())
    return ("?", repr(v))


def same(s: Any, t: Any, *, trim: bool = True, tol_us: int = 1) -> bool:
    """Source value `s` vs Delta value `t`, tolerating what the load documents to change: strings
    are trimmed, datetimeoffset goes to UTC, time becomes a string, numbers compare by value."""
    if s is None or t is None:
        return s is None and t is None
    if isinstance(s, dt.time) and isinstance(t, str):
        return t.startswith(s.isoformat()) or t.rstrip("0").startswith(s.isoformat().rstrip("0"))
    if isinstance(s, float) and isinstance(t, float):
        return s == t or _f32(s) == _f32(t) or abs(s - t) <= 1e-6 * abs(s)
    if isinstance(s, dt.datetime) and isinstance(t, dt.datetime):
        if s.tzinfo is not None:
            s = s.astimezone(dt.timezone.utc)
        if t.tzinfo is not None:
            t = t.astimezone(dt.timezone.utc)
        return abs(s.replace(tzinfo=None) - t.replace(tzinfo=None)) <= dt.timedelta(microseconds=tol_us)
    if isinstance(s, str) and isinstance(t, str):
        return (s.strip(" ") == t.strip(" ")) if trim else s == t
    if isinstance(s, (bytes, bytearray)) and isinstance(t, int):  # rowversion lands as bigint
        return int.from_bytes(bytes(s), "big") == t
    if isinstance(s, (bytes, bytearray, memoryview)) and isinstance(t, (bytes, bytearray)):
        return bytes(s) == bytes(t)
    if isinstance(s, (int, decimal.Decimal)) and isinstance(t, (int, decimal.Decimal)):
        return decimal.Decimal(s) == decimal.Decimal(t)
    if isinstance(s, str) and not isinstance(t, str):  # uniqueidentifier as UUID
        return str(t).upper() == s.upper()
    return s == t or str(s).upper() == str(t).upper()


# -- state of the target ---------------------------------------------------------------------------

META = {"__timestamp", "__is_deleted", "__is_full_load"}


def delta_rows(dest, rel: str = "delta") -> list[dict]:
    from deltalake import DeltaTable

    return DeltaTable(str(dest.path / rel)).to_pyarrow_table().to_pylist()


def current_state(dest, pk: list[str]) -> tuple[dict[tuple, dict], list[str]]:
    """latest row per key by `__timestamp` (a tie of two different rows is reported)."""
    problems: list[str] = []
    best: dict[tuple, dict] = {}
    for r in delta_rows(dest):
        k = tuple(norm(r[c]) for c in pk)
        cur = best.get(k)
        if cur is None or r["__timestamp"] > cur["__timestamp"]:
            best[k] = r
        elif r["__timestamp"] == cur["__timestamp"]:
            if {a: b for a, b in r.items()} != {a: b for a, b in cur.items()}:
                problems.append(f"tie on __timestamp for key {k}")
            if r["__is_deleted"] != cur["__is_deleted"] and not r["__is_deleted"]:
                best[k] = r
    return best, problems


def check_state(
    src: Src,
    dest,
    table: str,
    pk: list[str],
    *,
    ignore: tuple[str, ...] = (),
    trim: bool = True,
    deletes: bool = True,
    label: str = "",
    where: str = "",
    allow_dup_keys: bool = False,
):
    """Target (latest per key, no delete markers) must equal `SELECT *` of the source."""
    cols = [c for c in src.cols(table) if c not in ignore]
    src_rows = src.q(f"SELECT {', '.join(f'[{c}]' for c in cols)} FROM {table} {where}")
    cur, problems = current_state(dest, pk)
    tgt_rows = [r for r in cur.values() if not r["__is_deleted"]]
    if not deletes:  # modes without delete detection: keep everything that was ever seen
        tgt_rows = list(cur.values())
    # duplicate current rows per key
    kcount = Counter(tuple(norm(r[c]) for c in pk) for r in tgt_rows)
    dups = [k for k, n in kcount.items() if n > 1]
    if dups and not allow_dup_keys:
        problems.append(f"{len(dups)} duplicate current keys, e.g. {dups[:3]}")

    def _sig(row: dict[str, Any]) -> tuple:
        return tuple(norm(row[c], trim=trim) for c in cols)

    src_dicts = [dict(zip(cols, r)) for r in src_rows]
    # exact multiset comparison first, field-wise fallback to explain differences
    tgt_by_key: dict[tuple, list[dict]] = {}
    for r in tgt_rows:
        tgt_by_key.setdefault(tuple(norm(r[c]) for c in pk), []).append(r)
    src_by_key: dict[tuple, list[dict]] = {}
    for r in src_dicts:
        src_by_key.setdefault(tuple(norm(r[c]) for c in pk), []).append(r)
    missing = [k for k in src_by_key if k not in tgt_by_key]
    extra = [k for k in tgt_by_key if k not in src_by_key]
    if missing:
        problems.append(f"{len(missing)} source keys missing in target, e.g. {missing[:3]}")
    if extra:
        problems.append(f"{len(extra)} target keys not in source (undeleted?), e.g. {extra[:3]}")
    diffs = 0
    examples: list[str] = []
    for k, srows in src_by_key.items():
        trows = tgt_by_key.get(k)
        if not trows or len(srows) != len(trows):
            continue
        if len(srows) > 1:
            continue
        s, t = srows[0], trows[0]
        for c in cols:
            if not same(s[c], t.get(c), trim=trim, tol_us=1000 if c == 'dtm' else 1):
                diffs += 1
                if len(examples) < 4:
                    examples.append(f"key {k} col {c}: source {s[c]!r:.60} target {t.get(c)!r:.60}")
    if diffs:
        problems.append(f"{diffs} value differences: " + "; ".join(examples))
    assert not problems, f"[{label}] {table}: " + " | ".join(problems)
    return len(src_rows)


def latest_pk_keys(dest) -> Optional[int]:
    from deltalake import DeltaTable

    p = dest.path / "delta_load" / "latest_pk_version"
    if not p.exists():
        return None
    return DeltaTable(str(p)).to_pyarrow_table().num_rows


def wait_tick():
    time.sleep(0.02)


def check_aggregate(src: Src, dest, table: str, label: str = ""):
    """The same check for tables too big for Python dicts: count and column sums of the current state, in DuckDB."""
    import duckdb
    from deltalake import DeltaTable

    tgt = DeltaTable(str(dest.path / "delta")).to_pyarrow_dataset()  # noqa: F841
    con = duckdb.connect()
    con.register("tgt", tgt)
    got = con.execute(
        """SELECT count(*), sum(id::bigint), sum(amount::double), sum(length(val)::bigint), sum(rv::bigint)
           FROM (SELECT * FROM tgt QUALIFY row_number() OVER (PARTITION BY id ORDER BY __timestamp DESC, __is_deleted ASC) = 1)
           WHERE NOT __is_deleted"""
    ).fetchone()
    exp = src.q(f"SELECT COUNT(*), SUM(CAST(id AS bigint)), SUM(CAST(amount AS float)), SUM(CAST(LEN(val) AS bigint)), SUM(CAST(CAST(rv AS bigint) AS decimal(38,0))) FROM {table}")[0]
    ok = got[0] == exp[0] and got[1] == exp[1] and abs(float(got[2]) - float(exp[2])) <= 1e-6 * abs(float(exp[2])) and got[3] == exp[3] and int(got[4]) == int(exp[4])
    assert ok, f"[{label}] {table}: target {got} != source {exp}"
