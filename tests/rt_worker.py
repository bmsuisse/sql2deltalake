"""Subprocess entry for the realistic tests: `python -m tests.rt_worker <name> <table> [loops]` loads the
table (default config) `loops` times, printing the executed type of each load; stays quiet otherwise."""

import resource
import sys
import time

from .rt_support import DATA, load
from sql2deltalake.destination.file_system import FileSystemDestination

if __name__ == "__main__":
    name, table = sys.argv[1], sys.argv[2]
    loops = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    dest = FileSystemDestination(DATA / name)
    for _ in range(loops):
        t0 = time.perf_counter()
        r = load(name, table, dest)
        print("LOADED", r.executed_type, f"{time.perf_counter() - t0:.2f}s", flush=True)
    print("MAXRSS_MB", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024, flush=True)
