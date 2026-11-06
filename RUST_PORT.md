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
