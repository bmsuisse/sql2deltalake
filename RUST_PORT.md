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
- `last_load_state` and the no-updates shortcut (`MIN_ACTIVE_ROWVERSION` watermark, schema fingerprint);
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

## Performance

`tests/test_40_perf_native.py` (opt-in: `SQL2DELTA_PERF=1 scripts/run_tests_local.sh tests/test_40_perf_native.py -s`)
loads a 1M-row SQL Server table (8 columns, rowversion) into two local targets, once with the Python
implementation (`SQL2DELTA_NATIVE=0`) and once natively, and prints the wall time of `write_db_to_delta`
(release build of `_core`, local SQL Server, same machine):

| phase | Python s | native s | speedup |
|---|---|---|---|
| full load | 2.25 | 1.11 | 2.0x |
| unchanged (no-updates shortcut) | 0.10 | 0.13 | 0.7x |
| 1% of rows changed (delta load) | 1.12 | 0.74 | 1.5x |

`SQL2DELTA_TRACE=1` prints the time of each step of a native delta load. Of the 0.74 s, reading the 1M-row
PK snapshot from the source and writing `primary_keys_ts` is about 0.3 s and writing `latest_pk_version` 0.1 s
(both through deltalake); the key diffs take about 0.1 s each on 8 threads (hash-partitioned key sets).
The unchanged case is dominated by fixed costs (metadata queries in Python, opening Delta tables, the log flush).
