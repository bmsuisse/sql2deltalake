"""Realistic native load tests, scenario 4: loads of different tables in parallel threads and processes, a
5M-row table with its peak memory, and the GIL held by a native load.

    SQL2DELTA_REALISTIC=1 scripts/run_tests_local.sh tests/test_53_realistic_parallel.py -x -s
`SQL2DELTA_RT_BIG_ROWS` sets the big table's row count and enables that test (e.g. 5000000).
"""

import os
import random
import subprocess
import sys
import threading
import time

import pytest

from .rt_support import Src, check_aggregate, check_state, ensure_db, fresh_dest, latest_pk_keys, load, needs_realistic

pytestmark = [pytest.mark.realistic, needs_realistic]
SEED = int(os.getenv("SQL2DELTA_RT_SEED", str(random.randrange(10**6))))
BIG = int(os.getenv("SQL2DELTA_RT_BIG_ROWS", "5000000"))


def _tbl(s: Src, t: str, rows: int):
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, grp int, val nvarchar(60), amount decimal(18,4), ts datetime2(3), flag bit, rv rowversion)",
        f"""WITH n AS (SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
              FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
            INSERT INTO dbo.{t} (id, grp, val, amount, ts, flag) SELECT i, i % 100, CONCAT(N'row 😀 ', i), i * 1.25, DATEADD(second, i, '2020-01-01'), i % 2 FROM n""",
    )


def _mutate(s: Src, t: str, cyc: int, rng: random.Random):
    top = s.q(f"SELECT MAX(id) FROM dbo.{t}")[0][0]
    s.x(
        f"UPDATE dbo.{t} SET val = CONCAT(N'c{cyc} ', id), amount = amount + 1 WHERE id % {rng.choice([50, 500])} = {rng.randrange(50)}",
        f"DELETE TOP (200) FROM dbo.{t} WHERE id % 31 = {rng.randrange(31)}",
        f"INSERT INTO dbo.{t} (id, grp, val, amount, ts, flag) SELECT TOP 300 {top} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1, N'n', 1, SYSUTCDATETIME(), 1 FROM sys.all_objects",
    )


def test_two_tables_two_threads():
    ensure_db()
    s = Src()
    names = ["rt_par_a", "rt_par_b"]
    for n in names:
        _tbl(s, n, 100000)
    dests = {n: fresh_dest(n) for n in names}
    errors: list[str] = []
    barrier = threading.Barrier(2)
    srcs = {n: Src() for n in names}

    def work(n: str):
        rng = random.Random(SEED + hash(n) % 1000)
        try:
            load(n, f"dbo.{n}", dests[n])
            for cyc in range(15):
                _mutate(srcs[n], n, cyc, rng)
                barrier.wait(timeout=300)  # both load at the same moment
                load(n, f"dbo.{n}", dests[n])
                check_state(srcs[n], dests[n], f"dbo.{n}", ["id"], label=f"{n} cycle {cyc}")
        except BaseException as e:  # noqa: BLE001
            errors.append(f"{n}: {type(e).__name__}: {str(e)[:300]}")
            barrier.abort()

    ths = [threading.Thread(target=work, args=(n,)) for n in names]
    [t.start() for t in ths]
    [t.join() for t in ths]
    assert not errors, errors


def test_two_tables_two_processes():
    ensure_db()
    s = Src()
    names = ["rt_proc_a", "rt_proc_b"]
    rng = random.Random(SEED)
    for n in names:
        _tbl(s, n, 100000)
        fresh_dest(n)
    for cyc in range(8):
        if cyc:
            for n in names:
                _mutate(s, n, cyc, rng)
        procs = [subprocess.Popen([sys.executable, "-m", "tests.rt_worker", n, f"dbo.{n}"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for n in names]
        outs = [p.communicate(timeout=600)[0] for p in procs]
        for n, p, o in zip(names, procs, outs):
            assert p.returncode == 0, f"{n} cycle {cyc}: rc {p.returncode}\n{o[-800:]}"
    from sql2deltalake.destination.file_system import FileSystemDestination
    from .rt_support import DATA

    for n in names:
        check_state(s, FileSystemDestination(DATA / n), f"dbo.{n}", ["id"], label=n)


def test_native_load_does_not_hold_the_gil():
    """A heartbeat thread must keep ticking while a load of 1M rows runs (reads and writes release the GIL)."""
    ensure_db()
    s = Src()
    t = "rt_gil"
    _tbl(s, t, 1000000)
    dest = fresh_dest(t)
    gaps: list[float] = []
    stop = threading.Event()

    def beat():
        last = time.perf_counter()
        while not stop.is_set():
            time.sleep(0.01)
            now = time.perf_counter()
            gaps.append(now - last)
            last = now

    th = threading.Thread(target=beat)
    th.start()
    t0 = time.perf_counter()
    load(t, f"dbo.{t}", dest)
    s.x(f"UPDATE dbo.{t} SET val = N'x' WHERE id % 100 = 0")
    load(t, f"dbo.{t}", dest)
    total = time.perf_counter() - t0
    stop.set()
    th.join()
    worst = max(gaps)
    print(f"load wall {total:.1f}s, heartbeat worst gap {worst:.2f}s over {len(gaps)} ticks")
    assert worst < 2.0, f"another Python thread was starved for {worst:.1f}s while a native load ran"


def _rss(name: str, table: str) -> tuple[str, int, float]:
    r = subprocess.run([sys.executable, "-m", "tests.rt_worker", name, table], capture_output=True, text=True, timeout=3000)
    assert r.returncode == 0, r.stdout[-500:] + r.stderr[-800:]
    kind = [ln for ln in r.stdout.splitlines() if ln.startswith("LOADED")][0].split()
    rss = int([ln for ln in r.stdout.splitlines() if ln.startswith("MAXRSS_MB")][0].split()[1])
    return kind[1], rss, float(kind[2].rstrip("s"))


@pytest.mark.skipif(
    not os.getenv("SQL2DELTA_RT_BIG_ROWS"),
    reason="several GB of RAM and minutes per phase: set SQL2DELTA_RT_BIG_ROWS (e.g. 5000000)",
)
def test_5m_rows_full_unchanged_small_change_and_peak_rss():
    ensure_db()
    s = Src()
    t = "rt_big"
    _tbl(s, t, BIG)
    assert s.q(f"SELECT COUNT(*) FROM dbo.{t}")[0][0] == BIG
    dest = fresh_dest(t)
    res = {}
    res["full"] = _rss(t, f"dbo.{t}")
    res["unchanged"] = _rss(t, f"dbo.{t}")
    k = BIG // 1000
    s.x(f"UPDATE dbo.{t} SET val = N'changed', amount = amount + 1 WHERE id % 1000 = 7")
    res["0.1% changed"] = _rss(t, f"dbo.{t}")
    s.x(f"DELETE FROM dbo.{t} WHERE id % 1000 = 9", f"INSERT INTO dbo.{t} (id, grp, val, amount, ts, flag) SELECT TOP {k} {BIG} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1, N'n', 1, SYSUTCDATETIME(), 0 FROM sys.all_objects a CROSS JOIN sys.all_objects b")
    res["0.1% del+ins"] = _rss(t, f"dbo.{t}")
    for phase, (kind, rss, secs) in res.items():
        print(f"{phase:<14} {kind:<6} {secs:7.1f}s  peak RSS {rss} MB", flush=True)
    assert res["full"][0] == "full" and res["unchanged"][0] == "none"
    assert res["0.1% changed"][0] == "delta"
    assert res["unchanged"][1] < 3000 and res["0.1% changed"][1] < 6000, f"memory: {res}"
    from sql2deltalake.destination.file_system import FileSystemDestination
    from .rt_support import DATA

    n = s.q(f"SELECT COUNT(*) FROM dbo.{t}")[0][0]
    assert latest_pk_keys(FileSystemDestination(DATA / t)) == n
    check_aggregate(s, FileSystemDestination(DATA / t), f"dbo.{t}", label="5M")
