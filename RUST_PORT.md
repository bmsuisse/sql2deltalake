# Rust port: progress

Tracks the move of the load logic into Rust (`crates/`, PyO3 module `odbc2deltalake._core`).
The plan and the inventory are in [RUST_PLAN.md](RUST_PLAN.md); this file records what is done.

Line counts are non-blank lines of `odbc2deltalake/**/*.py` and `crates/**/*.rs` (tests excluded).

| Step | State | Python lines | Rust lines | Rust share | Suite (`scripts/run_tests_local.sh`) |
|---|---|---|---|---|---|
| 0. Baseline | done | 4465 | 0 | 0 % | 40 passed, 1 skipped |
| 1. Remove ODBC | done | 4041 | 0 | 0 % | 40 passed, 1 skipped |
| 2. Maturin workspace | done | 4041 | 15 | 0 % | 40 passed, 1 skipped |
| 3. Pure logic | done | 4042 | 916 | 18 % | 72 passed, 1 skipped (+32 parity tests) |
| 4+5. Load in Rust, native by default | done | 4233 | 2477 | 36 % | native: 72 passed, 1 skipped; `SQL2DELTA_NATIVE=0`: 72 passed, 1 skipped |

## Step 1: ODBC removed

- `ODBCReader`, `odbc_utils.build_connection_string`, the `arrow-odbc`/`pyodbc` extras and the CI ODBC driver install are gone.
  SQL Server is read through arrowsql only.
- The local half (delta-rs writes, DuckDB views) that `ODBCReader` and `ADBCReader` duplicated is now one base class,
  `reader/local_reader.py` `LocalDeltaReader`; `ArrowSQLReader` and `ADBCReader` (Postgres) only implement the source reads.
- A connection string passed to `make_writer`/`write_db_to_delta` becomes an `ArrowSQLReader` (an ADBC Postgres reader for `dialect="postgres"`).
- Test fixtures connect with arrowsql's DB-API (`arrowsql.connect`, the mssql_python interface). `ODBCLAKE_TEST_DB_PREFIX`
  sets the prefix of the test databases, so several checkouts can share one server.
- Kept: `strip_odbc_driver` (drops a `Driver=` keyword from connection strings written for ODBC), the `ODBCLAKE_*` test
  variable names and the `odbc2deltalake` import name.
- Kept for now: the Spark JDBC `SparkReader` (the Spark data source work builds on the same package).

## Step 2: maturin workspace

- Cargo workspace at the repo root: `crates/sql2deltalake-core` (pure Rust, no PyO3) and `crates/sql2deltalake-py`
  (PyO3 0.29, abi3-py310, module `odbc2deltalake._core`). arrow-rs 60, the version arrowsql exports.
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
`odbc2deltalake._native.fail_point`, which both implementations call at the same steps.

Not native yet: the Spark reader (Spark writes Delta, JDBC reads), Postgres through ADBC, and custom readers that
are not `ArrowSQLReader` subclasses: they run the Python implementation in `db_to_delta.py`.

## Delta reads with delta-kernel-rs

For a local target folder (`FileSystemDestination`) the load reads its Delta tables (versions, schemas, the PK
snapshots, the state) with delta-kernel-rs 0.30.1 in Rust, without Python (cargo feature `kernel`, on by default;
`SQL2DELTA_KERNEL=0` falls back to the Python callback, which reads through pyarrow datasets). Other destinations
(Azure, Databricks) use the callback. Writes stay with Python deltalake.

## Parallel source reads

`ArrowSQLReader(..., partitions=N)` splits the data reads of a native load (full load, PK snapshot, changed rows)
into N range queries on the first primary key if it is an integer, read on parallel connections by arrowsql.
Off by default (`partitions=1`).

## Performance

`tests/test_40_perf_native.py` (opt-in: `SQL2DELTA_PERF=1 scripts/run_tests_local.sh tests/test_40_perf_native.py -s`)
loads a 1M-row SQL Server table (8 columns, rowversion) with the Python implementation (`SQL2DELTA_NATIVE=0`),
natively, and natively with `partitions=4`, each into its own local target, and prints the median wall time of
`write_db_to_delta` over 3 rounds (release build of `_core`, local SQL Server, a shared machine, so expect noise):

| phase | Python s | native s | speedup | native, 4 partitions s | speedup |
|---|---|---|---|---|---|
| full load | 0.94 | 0.89 | 1.1x | 0.65 | 1.4x |
| unchanged (no-updates shortcut) | 0.09 | 0.13 | 0.7x | 0.10 | 0.9x |
| 1% of rows changed (delta load) | 0.80 | 0.69 | 1.2x | 0.62 | 1.3x |

The Python column already uses the Rust Arrow normalization (step 3). Where the time goes in a native load
(`SQL2DELTA_TRACE=1` prints each step): a full load is the source read plus the deltalake write, which deltalake
does after pulling each batch, so they add up (reading 1M rows: 0.39 s, 0.12 s with 4 partitions; writing them:
0.33 s); a delta load reads and writes the 1M-row PK snapshot and `latest_pk_version` (about 0.4 s together), the
key diffs take about 0.1 s each on 8 threads.
