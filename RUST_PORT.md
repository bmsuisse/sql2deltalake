# Rust port: progress

Tracks the move of the load logic into Rust (`crates/`, PyO3 module `sql2deltalake._core`).
The plan and the inventory are in [RUST_PLAN.md](RUST_PLAN.md); this file records what is done.

Line counts are non-blank lines of `sql2deltalake/**/*.py` and `crates/**/*.rs` (tests excluded).

| Step | State | Python lines | Rust lines | Rust share | Suite (`scripts/run_tests_local.sh`) |
|---|---|---|---|---|---|
| 0. Baseline | done | 4465 | 0 | 0 % | 40 passed, 1 skipped |
| 1. Remove ODBC | done | 4041 | 0 | 0 % | 40 passed, 1 skipped |
| 2. Maturin workspace | done | 4041 | 15 | 0 % | 40 passed, 1 skipped |
| 3. Pure logic | done | 4042 | 916 | 18 % | 72 passed, 1 skipped (+32 parity tests) |
| 4+5. Load in Rust, native by default | done | 4233 | 2477 | 36 % | native: 72 passed, 1 skipped; `SQL2DELTA_NATIVE=0`: 72 passed, 1 skipped |
| + kernel reads, state shortcut, one-pass diff, slices | done | 4289 | 2950 | 41 % | native: 73 passed, 2 skipped; `SQL2DELTA_NATIVE=0`: 73 passed, 2 skipped |

The Python lines include the Python implementation of the load (`db_to_delta.py`, `write_utils/restore_pk.py`,
about 1700 lines), which only the Spark and ADBC/Postgres readers still run. On the arrowsql path everything from
the first source query to the last state write runs in Rust; Python does the metadata discovery (`make_writer`:
columns, primary keys, sqlglot rendering), `meta/schema.json`, the Delta writes (deltalake) and the log.

## Step 1: ODBC removed

- `ODBCReader`, `odbc_utils.build_connection_string`, the `arrow-odbc`/`pyodbc` extras and the CI ODBC driver install are gone.
  SQL Server is read through arrowsql only.
- The local half (delta-rs writes, DuckDB views) that `ODBCReader` and `ADBCReader` duplicated is now one base class,
  `reader/local_reader.py` `LocalDeltaReader`; `ArrowSQLReader` and `ADBCReader` (Postgres) only implement the source reads.
- A connection string passed to `make_writer`/`write_db_to_delta` becomes an `ArrowSQLReader` (an ADBC Postgres reader for `dialect="postgres"`).
- Test fixtures connect with arrowsql's DB-API (`arrowsql.connect`, the mssql_python interface). `ODBCLAKE_TEST_DB_PREFIX`
  sets the prefix of the test databases, so several checkouts can share one server.
- Kept: `strip_odbc_driver` (drops a `Driver=` keyword from connection strings written for ODBC) and and the `ODBCLAKE_*` test
  variable names.
- Kept for now: the Spark JDBC `SparkReader` (the Spark data source work builds on the same package).

## Step 2: maturin workspace

- Cargo workspace at the repo root: `crates/sql2deltalake-core` (pure Rust, no PyO3) and `crates/sql2deltalake-py`
  (PyO3 0.29, abi3-py310, module `sql2deltalake._core`). arrow-rs 60, the version arrowsql exports.
- `pyproject.toml` builds with maturin; `requires-python` is now `>=3.10` (arrowsql and the abi3 wheel need 3.10).
- CI: a `test_rust` job (fmt, clippy, `cargo test`); the Python jobs build the extension through `uv sync`.
- Dev loop: `maturin develop --uv`, then `scripts/run_tests_local.sh`.

## Step 3: pure logic in Rust

- `sqlgen.rs`: every SQL statement the load sends to the source (full load, PK/delta snapshot, delta criteria and
  their literals, `MAX`/`COUNT`, `OPENJSON`/`JSON_TABLE` row reads, load timestamp), T-SQL and Postgres, plus the
  default type map and `compat_name`. sqlglot stays at the Python boundary only: `_native.py` renders each column's
  types and the type map once (`column_spec`, `type_map_spec`); a query source is passed as rendered SQL.
- Golden tests: `tests/sql_golden.py` builds the cases with the Python/sqlglot implementation; `cargo test` checks the
  snapshot `crates/sql2deltalake-core/tests/golden/sqlgen.json` (byte-identical SQL), `tests/test_30_native_sqlgen.py`
  compares live and checks that the snapshot is current.
- `normalize.rs`: the Arrow cast stage between arrowsql and the writer (large types, `time` to text with the column
  scale, UTC time zones, uint8 to int16) runs in Rust; `ArrowSQLReader` streams through `_core.normalize` (zero-copy
  C stream in and out) instead of the per-batch pyarrow code.
- `scalar.rs`: values read from Arrow (delta values, counts, clock) rendered like the Python objects were.

## Steps 4 and 5: the load runs in Rust

`exec_write_db_to_delta` hands the load to `_core.run_load` when the reader is an `ArrowSQLReader` (T-SQL) and
`SQL2DELTA_NATIVE` is not `0` (the default). The GIL is released for the whole load; Python is only entered for
the host callbacks in `_native.Host` (Delta writes with deltalake, Delta reads as pyarrow dataset streams, the
destination's file operations, the logger).

In Rust (`crates/sql2deltalake-core/src/load.rs`, `keys.rs`, `table.rs`):

- the state machine: overwrite, full load, `force_full`, `append_inserts`, delta, `simple_delta(_check)` with its
  second pass, new-column and PK-mismatch fallbacks, the lock file, vacuum of the state tables;
- the PK snapshot diff (`primary_keys_ts` against `latest_pk_version`), strange updates (> 1000 or
  `no_complex_entries_load`: re-read by `MIN`, else `OPENJSON` batches below 7000 characters), delete markers,
  the new `latest_pk_version`, the `simple_delta` upsert: hash lookups on Arrow row-format keys instead of DuckDB
  views (`EXCEPT` is distinct and NULL-safe, anti joins cast key types to the snapshot's);
- `last_load_state` and the no-updates shortcut (`MIN_ACTIVE_ROWVERSION` watermark, schema fingerprint). The native
  load also stores the delta-value maximum and row count of `latest_pk_version`, the `primary_keys_ts` version and
  whether the next load has a delta value, so an unchanged table is recognized from the one-row state table and
  the source's `MAX`/`COUNT` without reading the snapshots (a state written by the Python implementation lacks
  these columns and takes the reading path);
- the per-pass `__timestamp` (source clock read once per pass, strictly later than the previous pass);
- restore on failure (`latest_pk_version` and `primary_keys_ts` back to their versions before the load);
- `restore_last_pk` (last full load plus newest row per key after it, without deleted keys);
- the Arrow normalization of everything read from the source, and a tee: rows streamed to a Delta table are
  kept (all columns of staging tables, only PK and delta columns of a full load), so nothing is read back.

Readers that override `source_write_sql_to_delta` (for example a reader that splits queries into slices) still
run natively: the host copies through the reader and the load reads the staging table back.

Tests: the fault injection of tests 17 and 18 used to monkeypatch private Python functions; it now patches
`sql2deltalake._native.fail_point`, which both implementations call at the same steps.

Not native yet: the Spark reader (Spark writes Delta, JDBC reads), Postgres through ADBC, and custom readers that
are not `ArrowSQLReader` subclasses: they run the Python implementation in `db_to_delta.py`.

## Delta reads with delta-kernel-rs

For a local target folder (`FileSystemDestination`) the load reads its Delta tables (versions, schemas, the PK
snapshots, the state) with delta-kernel-rs 0.30.1 in Rust, without Python (cargo feature `kernel`, on by default;
`SQL2DELTA_KERNEL=0` falls back to the Python callback, which reads through pyarrow datasets). Other destinations
(Azure, Databricks) use the callback. Writes stay with Python deltalake.

## Source reads through arrowsql-core

The native load reads SQL Server through the `arrowsql-core` crate (cargo feature `arrowsql-core`, on by default,
pinned to arrowsql main `cb18dae`, which has the core and the DNS cache): TDS to Arrow in Rust, no Python for the
source. `SQL2DELTA_ARROWSQL_CORE=0` reads through arrowsql's Python package instead; CI runs the arrowsql suite both
ways. Readers that override the source methods (`source_sql_to_py`, `_source_arrow`, `source_write_sql_to_delta`)
keep going through Python.

Before a delta load on SQL Server, one statement reads the source clock, `MIN_ACTIVE_ROWVERSION()` (in the table's
database) and `MAX`/`COUNT` of the delta column; if it fails, the load reads them one by one. Table existence,
versions and columns of a local target are checked in Rust and cached for the load (dropped for a table when the
load writes, restores, removes or vacuums it).

## Parallel source reads

`ArrowSQLReader(..., partitions=N)` splits the data reads of a native load (full load, PK snapshot, changed rows)
into N range queries on the first primary key if it is an integer, read on parallel connections by arrowsql.
Off by default (`partitions=1`).

## Performance

`tests/test_40_perf_native.py` (opt-in: `SQL2DELTA_PERF=1 scripts/run_tests_local.sh tests/test_40_perf_native.py -s`)
loads a 1M-row SQL Server table (8 columns, rowversion) with the Python implementation (`SQL2DELTA_NATIVE=0`),
natively, and natively with `partitions=4`, each into its own local target, and prints the median wall time of
`write_db_to_delta` per phase: full load, unchanged reload, reload after 1% of the rows changed.

The table below is from the same scenario run as separate processes, 5 rounds per variant, against the code before
the port ("before": commit 7c76338, pure Python with the pyarrow normalization), the Python implementation of this
branch (`SQL2DELTA_NATIVE=0`, it already uses the Rust normalization) and the native load. Release build of `_core`,
local SQL Server, on a shared machine (load average about 8 on 24 cores), so the best of 5 is shown:

| phase | before s | Python path s | native s | vs before | native, 4 partitions s | vs before |
|---|---|---|---|---|---|---|
| full load | 1.04 | 0.95 | 0.98 | 1.1x | 0.76 | 1.4x |
| unchanged (no-updates shortcut) | 0.080 | 0.077 | 0.049 | 1.6x | 0.043 | 1.9x |
| 1% of rows changed (delta load) | 0.78 | 0.73 | 0.57 | 1.4x | 0.53 | 1.5x |

Where the time goes (`SQL2DELTA_TRACE=1` prints each step of a native load):

- full load: the source read plus the deltalake write. deltalake pulls a batch, then writes it, so the two add
  up (1M rows: reading 0.39 s, 0.12 s with 4 partitions; writing 0.33 s). The Rust side (cast stage, tee of the
  key columns, `latest_pk_version`) is small next to it; the remaining lever is the writer.
- unchanged: the source `MAX`/`COUNT` query and fixed costs (metadata queries and sqlglot in `make_writer`, the
  log flush, opening the Delta logs); no table data is read.
- 1% changed: reading the 1M-row PK snapshot from the source and writing `primary_keys_ts` (about 0.3 s), writing
  `latest_pk_version` (0.1 s); the snapshot comparison is one parallel hash pass (about 0.05 s).

## Spark data source: jobs and timings

Spark jobs per phase of a `run` (`tests_spark/test_23_spark_jobs.py` asserts them, local[4]):

| phase | prepare | read + append | commit |
|---|---|---|---|
| full load (no target) | 0 | 2 | 7 |
| unchanged | 2 (history of target and state) | 0 | 0 |
| changed | 2 | 1 | 6 (was 10) |

A changed load no longer reads `latest_pk_version` back in `prepare()`: `commit()` keeps the table it wrote for
the next `prepare()` of the process (was prepare 7, commit 7).

`latest_pk_version` is append-only (state format 3): rows with `__state_seq` 0 are the keys as of the last
rewrite, a delta commit appends only the keys its load changed or deleted (`__state_seq` = the Delta version it
creates, `__is_deleted` for a deleted key). Reading it (a new driver process, or a time-travel read) replays the
appended rows in Rust (`_core.replay_state`: per key the highest `__state_seq` wins). A commit rewrites the table
(compaction) when the rows appended since the last rewrite would exceed a fifth of the keys (at least 10 000), and
always after a full load or a rebuild. It stays a cache: a state of format 2 is not recognized and is rebuilt
from the target; staleness checks (`StalePlanError`) and the rebuild rules are unchanged, and an append that
does not fit the table (other key types after a rebuild) falls back to a rewrite.

`tests_spark/test_40_perf_spark.py` (opt-in: `SQL2DELTA_PERF=1 scripts/run_spark_tests_local.sh
tests_spark/test_40_perf_spark.py -s`), 1M rows, `numPartitions=4`, median of 3, local SQL Server, shared machine
(load average about 10 on 24 cores), before and after the append-only state, same session:

| phase | before s | jobs | after s | jobs |
|---|---|---|---|---|
| full load | 3.33 | 9 | 3.57 | 9 |
| unchanged | 0.27 | 2 | 0.32 | 2 |
| 1% changed | 2.93 | 13 | 1.71 | 9 |

Commit of the changed load: 2.25-2.72 s before (of which the 1M-row state overwrite 1.39-1.67 s), 0.92-1.20 s
after (the append of the 10 000 changed keys 0.17-0.22 s). What is left in it is reading this load's rows back
from the target and the two history reads. The full load still writes the whole state (1M keys, about 1 s).

The native (non-Spark) 1M-row test on the same machine at load average 18: full 1.50 s (4 partitions 1.15 s),
unchanged 0.11 s, 1% changed 1.10 s (4 partitions 0.82 s); the Python path 1.44 / 0.16 / 1.42 s. These are
slower than the table above (measured at load average 8), so compare within a row only.

## Review fixes (final review)

- PK snapshot diff: when keys repeat in `latest_pk_version` (keys equal after `TRIM`, user-set keys of a view) the
  diff is set-based in both directions, as the Python `EXCEPT` was; no false delete markers.
- The `OPENJSON` join quotes identifiers with `"` doubled (a column name could inject SQL before).
- Readers with their own `source_write_sql_to_delta`: strange updates read in chunks land once each.
- `KeyboardInterrupt`/`SystemExit` in a callback end the load (`Error::Interrupted`), no fallback takes them.
- A failed restore of the load state after a failed load is logged and attached to the raised exception.
- Spark: the watermark of a rebuilt state is MAX over the rebuilt keys, not over the whole append-only target;
  `commit()` of a full plan without its rows in the target writes no state; `commit()` raises `StalePlanError`
  when another load appended or committed since `prepare()`; target paths with a backtick are quoted.
- Nits: UInt64 delta values above the BIGINT range are an error, `normalize` of a non-struct array raises
  `TypeError`, delete markers carry the pass's source timestamp.
- Release: abi3 wheels (CPython 3.10+) for manylinux x86_64/aarch64, macOS arm64/x86_64 and Windows x64, plus the
  sdist, published through the existing trusted publisher.

## CI

- The legacy `spark` jobs (SparkReader over JDBC, pyspark 3.5) did not hang: they were slow. With Spark's defaults
  every small Delta read and write of the Python load (dozens per load, plus a `VACUUM` of five state tables)
  ran 200 shuffle tasks, 50 Delta log snapshot partitions and a parallel file listing; on a GitHub runner the
  suite needed well over an hour (8 of 19 files in 21 minutes). The test session now uses 4 shuffle partitions,
  2 snapshot partitions and a parallel listing of 4: locally the whole legacy Spark suite takes 8 minutes
  (`test_03` 124 s before, 34 s after).
- Two Python-load bugs that only Spark showed: `VACUUM` through Delta Lake adds commits to `latest_pk_version`
  after `last_load_state` recorded its version, so the no-updates shortcut never matched (now the vacuum runs
  before the state is written, and not at all for an unchanged load); `test_32`'s restore test is native-only.
- `spark x postgres` is dropped (the Python load on Postgres runs in `local`/`azure x postgres`; SparkReader is
  superseded by the data source), and `local x mssql` too (the same suite as `arrowsql x mssql`).
- New job `test_spark_source`: `tests_spark/` with pyspark 4.1 and delta-spark 4.1 (installed over the locked
  test group), JDK 21, a SQL Server service container. `tests_spark` fails instead of skipping without
  `ODBC_MASTER_CONN` when `CI` is set.

## What is left

- Spark: `SparkReader` (JDBC, Spark writes Delta) still runs the Python implementation. The Rust load could drive
  it through the same host interface (Spark DataFrames as Arrow for the PK snapshots, Spark for the data writes).
  The Spark data source (`sql2deltalake.spark`) is the replacement.
- Postgres: `ADBCReader` still runs the Python implementation (the SQL generation for Postgres is ported and
  golden-tested; the native path needs an ADBC source stream and a local Postgres to test against).
- When Spark and Postgres are native, `db_to_delta.py`, `write_utils/restore_pk.py` and the DuckDB half of
  `LocalDeltaReader` can be deleted (the consistency check in `consistency.py` and the test helpers still use
  DuckDB as an independent check).
- Writes: Delta writes stay with Python deltalake (per the decision not to use delta-rs from Rust); with Spark as the
  writer for Spark targets.
