"""Realistic native load tests, scenario 2: faults in the middle of a load. After every fault the next
load must converge the target to the source without a lost update or a duplicate current row.

    SQL2DELTA_REALISTIC=1 scripts/run_tests_local.sh tests/test_51_realistic_faults.py -x -s
"""

import os
import random
import signal
import subprocess
import sys
import threading
import time

import pytest

from .rt_support import (
    DATA,
    DB_NAME,
    Src,
    check_state,
    connect,
    ensure_db,
    fresh_dest,
    latest_pk_keys,
    load,
    needs_realistic,
)

pytestmark = [pytest.mark.realistic, needs_realistic]
SEED = int(os.getenv("SQL2DELTA_RT_SEED", str(random.randrange(10**6))))
ROWS = int(os.getenv("SQL2DELTA_RT_FAULT_ROWS", "300000"))


def _big_table(s: Src, t: str, rows: int = ROWS):
    s.x(
        f"DROP TABLE IF EXISTS dbo.{t}",
        f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, grp int, val nvarchar(100), amount decimal(18,4), ts datetime2(3), rv rowversion)",
        f"""WITH n AS (SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
              FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
            INSERT INTO dbo.{t} (id, grp, val, amount, ts) SELECT i, i % 100, CONCAT(N'row 😀 ', i), i * 1.25, DATEADD(second, i, '2020-01-01') FROM n""",
    )


def _mutate(s: Src, rng: random.Random, t: str, cyc: int):
    k = rng.choice(["upd_pct", "ins", "del", "mixed", "pk"])
    top = s.q(f"SELECT MAX(id) FROM dbo.{t}")[0][0] or 0
    if k in ("upd_pct", "mixed"):
        s.x(f"UPDATE dbo.{t} SET val = CONCAT(N'u{cyc}-', id), amount = amount + 1 WHERE id % {rng.choice([100, 1000, 20])} = {rng.randrange(20)}")
    if k in ("ins", "mixed"):
        s.x(f"INSERT INTO dbo.{t} (id, grp, val, amount, ts) SELECT TOP 5000 {top} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1, N'new', 1, SYSUTCDATETIME() FROM sys.all_objects a CROSS JOIN sys.all_objects b")
    if k in ("del", "mixed"):
        s.x(f"DELETE TOP (3000) FROM dbo.{t} WHERE id % 17 = {rng.randrange(17)}")
    if k == "pk":
        # negated ids never collide with the inserts above MAX(id) nor with each other
        s.x(f"UPDATE TOP (100) dbo.{t} SET id = -id WHERE id > 0 AND id % 29 = 1")


def _converge(s: Src, t: str, dest, label: str, tries: int = 3):
    for i in range(tries):
        load(t, f"dbo.{t}", dest)
    check_state(s, dest, f"dbo.{t}", ["id"], label=label)
    assert latest_pk_keys(dest) == s.q(f"SELECT COUNT(*) FROM dbo.{t}")[0][0], f"[{label}] latest_pk_version differs"


class _Killer(threading.Thread):
    """Kills, after `delay` seconds, every other session that runs against the database."""

    def __init__(self, delay: float, table: str):
        super().__init__(daemon=True)
        self.delay, self.table, self.killed = delay, table, 0

    def run(self):
        time.sleep(self.delay)
        c = connect("master")
        cur = c.cursor()
        end = time.time() + 4
        while time.time() < end and not self.killed:
            cur.execute(
                f"""SELECT r.session_id FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions se ON se.session_id = r.session_id
                    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) tx
                    WHERE se.is_user_process = 1 AND r.database_id = DB_ID('{DB_NAME}') AND r.session_id <> @@SPID AND tx.text LIKE '%{self.table}%'
                    AND tx.text NOT LIKE '%dm_exec_requests%'"""
            )
            for (spid,) in cur.fetchall():
                try:
                    cur.execute(f"KILL {int(spid)}")
                    self.killed += 1
                except Exception:
                    pass
            time.sleep(0.01)


def test_kill_connection_mid_load():
    ensure_db()
    s = Src()
    t = "rt_kill"
    _big_table(s, t)
    dest = fresh_dest(t)
    rng = random.Random(SEED)
    killed_total = 0
    panics: list[str] = []
    for cyc in range(12):
        if cyc:
            _mutate(s, rng, t, cyc)
        delay = rng.choice([0.0, 0.05, 0.2, 0.5, 1.0, 2.0])
        killer = _Killer(delay, t)
        killer.start()
        try:
            load(t, f"dbo.{t}", dest)
            outcome = "survived"
        except Exception as e:
            outcome = f"failed ({type(e).__name__})"
        except BaseException as e:  # pyo3 PanicException is a BaseException: `except Exception` in user code misses it
            outcome = f"PANIC ({type(e).__name__})"
            panics.append(str(e)[:150])
        killer.join()
        killed_total += killer.killed
        print(f"cycle {cyc}: delay {delay} killed {killer.killed} -> {outcome}")
        _converge(s, t, dest, f"cycle {cyc} after kill at {delay}s ({outcome})")
    assert killed_total > 0, "the killer never hit a session, the test proves nothing"
    assert not panics, f"a killed connection panicked the process instead of raising an Exception: {panics[0]}"


@pytest.mark.parametrize("point", ["load_updates", "additional_updates", "restore_last_pk", "write_delta2", "local_pk_count"])
def test_exception_at_every_fail_point(point: str, monkeypatch: pytest.MonkeyPatch):
    from sql2deltalake import _native

    ensure_db()
    s = Src()
    t = f"rt_fp_{point}"
    _big_table(s, t, 50000)
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    rng = random.Random(SEED)
    hits = 0
    for cyc in range(10):
        _mutate(s, rng, t, cyc)
        s.x(f"UPDATE TOP (50) dbo.{t} SET val = N'x' WHERE id % 2 = 0")  # make sure there is something to load
        armed = {"on": True}
        orig = _native.fail_point

        def boom(name: str):
            if name == point and armed["on"]:
                armed["on"] = False
                raise RuntimeError(f"injected failure at {name}")
            return orig(name)

        monkeypatch.setattr(_native, "fail_point", boom)
        try:
            load(t, f"dbo.{t}", dest)
        except Exception as e:
            hits += "injected" in str(e)
        monkeypatch.setattr(_native, "fail_point", orig)
        _converge(s, t, dest, f"{point} cycle {cyc}")
    print(f"{point}: injected {hits}x")


def test_sigint_mid_load():
    ensure_db()
    s = Src()
    t = "rt_sigint"
    _big_table(s, t)
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    rng = random.Random(SEED)
    env = dict(os.environ)
    interrupted = 0
    for cyc in range(10):
        _mutate(s, rng, t, cyc)
        s.x(f"UPDATE dbo.{t} SET amount = amount + 1 WHERE id % 3 = 0")
        p = subprocess.Popen([sys.executable, "-m", "tests.rt_worker", t, f"dbo.{t}"], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        time.sleep(rng.choice([0.3, 0.8, 1.5, 2.5, 4.0]))
        if p.poll() is None:
            p.send_signal(signal.SIGINT)
            interrupted += 1
        try:
            out, _ = p.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            p.kill()
            out, _ = p.communicate()
            pytest.fail(f"cycle {cyc}: the process did not stop after SIGINT within 60s (a native call that ignores signals?)")
        _converge(s, t, dest, f"cycle {cyc} after SIGINT (rc {p.returncode})")
    print("interrupted", interrupted)
    assert interrupted > 0


def test_late_commit_under_rcsi():
    """A transaction holds a lower rowversion and commits after the next load: its row must show up in the
    load after it (MIN_ACTIVE_ROWVERSION watermark), also across several loads while it stays open."""
    ensure_db()
    s = Src()
    t = "rt_late"
    s.x(f"DROP TABLE IF EXISTS dbo.{t}", f"CREATE TABLE dbo.{t} (id int NOT NULL PRIMARY KEY, val nvarchar(20), rv rowversion)", f"INSERT INTO dbo.{t} (id, val) VALUES (1, N'a'), (2, N'b')")
    assert s.q(f"SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = '{DB_NAME}'")[0][0] == 1
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    for cyc in range(8):
        a = connect(autocommit=False)
        ca = a.cursor()
        ca.execute(f"INSERT INTO dbo.{t} (id, val) VALUES ({100 + cyc}, N'late{cyc}')")  # lower rowversion, uncommitted
        if cyc % 2:
            ca.execute(f"UPDATE dbo.{t} SET val = N'late-upd{cyc}' WHERE id = 1")
        s.x(f"INSERT INTO dbo.{t} (id, val) VALUES ({1000 + cyc}, N'after{cyc}')", f"UPDATE dbo.{t} SET val = N'b{cyc}' WHERE id = 2")
        load(t, f"dbo.{t}", dest)  # the uncommitted row is invisible
        s.x(f"INSERT INTO dbo.{t} (id, val) VALUES ({2000 + cyc}, N'after2-{cyc}')")
        load(t, f"dbo.{t}", dest)  # still open
        a.commit()
        a.close()
        load(t, f"dbo.{t}", dest)
        check_state(s, dest, f"dbo.{t}", ["id"], label=f"late commit cycle {cyc}")


def test_failed_load_leaves_readable_state():
    """B1 (fixed): after a load failed in the middle (any exception, SIGINT, killed connection) the load restored
    delta_load/latest_pk_version with delta-rs RESTORE, whose log (protocol with empty feature lists) the native
    read (delta-kernel) rejected: every following load failed until the table was deleted by hand."""
    ensure_db()
    from sql2deltalake import _native

    s = Src()
    t = "rt_restore_bug"
    _big_table(s, t, 2000)
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    orig = _native.fail_point
    for i in range(3):
        s.x(f"UPDATE dbo.{t} SET val = N'v{i}' WHERE id % 2 = 0")

        def boom(name: str):
            if name == "load_updates":
                raise RuntimeError("injected")
            return orig(name)

        _native.fail_point = boom
        try:
            load(t, f"dbo.{t}", dest)
        except RuntimeError:
            pass
        finally:
            _native.fail_point = orig
        load(t, f"dbo.{t}", dest)  # must succeed
        load(t, f"dbo.{t}", dest)
        check_state(s, dest, f"dbo.{t}", ["id"], label=f"round {i}")


def test_deletes_survive_rebuild_of_latest_pk_after_failed_load():
    """B2 (fixed): after a failed load, delta_load/latest_pk_version deleted by hand (as a user would) and rebuilt,
    keys that left the source must get their delete markers (the stale last_load_state must not be trusted)."""
    import shutil

    from sql2deltalake import _native

    ensure_db()
    s = Src()
    t = "rt_lostdel"
    _big_table(s, t, 1000)
    dest = fresh_dest(t)
    load(t, f"dbo.{t}", dest)
    orig = _native.fail_point
    for rnd in range(3):
        s.x(f"DELETE TOP (10) FROM dbo.{t}")

        def boom(name: str):
            if name == "load_updates":
                raise RuntimeError("injected")
            return orig(name)

        _native.fail_point = boom
        try:
            load(t, f"dbo.{t}", dest)
        except RuntimeError:
            pass
        finally:
            _native.fail_point = orig
        shutil.rmtree(dest.path / "delta_load" / "latest_pk_version", ignore_errors=True)
        for _ in range(3):
            load(t, f"dbo.{t}", dest)
        check_state(s, dest, f"dbo.{t}", ["id"], label=f"round {rnd}")
