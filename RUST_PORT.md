# Rust port: progress

Tracks the move of the load logic into Rust (`crates/`, PyO3 module `odbc2deltalake._core`).
The plan and the inventory are in [RUST_PLAN.md](RUST_PLAN.md); this file records what is done.

Line counts are non-blank lines of `odbc2deltalake/**/*.py` and `crates/**/*.rs` (tests excluded).

| Step | State | Python lines | Rust lines | Rust share | Suite (`scripts/run_tests_local.sh`) |
|---|---|---|---|---|---|
| 0. Baseline | done | 4465 | 0 | 0 % | 40 passed, 1 skipped |
| 1. Remove ODBC | done | 4041 | 0 | 0 % | 40 passed, 1 skipped |

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
