# Baseline test run

Source tree: c348764 (sql2deltalake 0.14.3 plus the arrowsql reader), run on 2026-11-06 before any library fix.

Environment:
- `.venv` on Python 3.13, synced from `uv.lock` (deltalake 0.18.1, deltalake2db 0.10.2, duckdb 1.3.2,
  pyarrow 20.0.0, polars 1.31.0, sqlglot 27.0.0, pytest 8.4.1). The command is in `scripts/run_tests_local.sh`.
- arrowsql from the local mssql-python-rs checkout through a `.pth` (not the PyPI 0.2.0 wheel from the lock).
- SQL Server 2022 on localhost, no Spark, no Azurite, no ODBC driver.
- `ODBCLAKE_TEST_CONFIGURATION=arrowsql`, so every load reads the source through `ArrowSQLReader`.

Test-environment fixes made to get here:
- The previous `.venv` had deltalake 1.6.6 and newer pyarrow/polars/duckdb, which do not match the lock
  (deltalake 1.x fails with a TypeError). It was re-synced to the lock.
- `scripts/run_tests_local.sh` defaulted to the `local` configuration, which reads with `ODBCReader`
  (arrow-odbc plus an ODBC driver, neither installed), so every load test failed with ModuleNotFoundError.
  The default is now `arrowsql`. The script also keeps pytest's exit code through the password filter.

Command: `scripts/run_tests_local.sh -rA`

Result: 27 passed, 0 failed, 1 skipped (28 collected, 77 s).

| Test | Result |
|---|---|
| test_01_first_full.py::test_first_load_timestamp[arrowsql] | pass |
| test_01_first_full.py::test_first_load_sys_start[arrowsql] | pass |
| test_01_first_full.py::test_first_load_always_full[arrowsql] | pass |
| test_02_full_load.py::test_first_load_always_full[arrowsql] | pass |
| test_03_delta.py::test_delta[arrowsql] | pass |
| test_03_delta.py::test_delta_sys[arrowsql] | pass |
| test_04_strange_delta.py::test_strange_delta[arrowsql] | pass |
| test_04_strange_delta.py::test_strange_delta_sys[arrowsql] | pass |
| test_05_conversion.py::test_first_load_timestamp[arrowsql] | pass |
| test_06_append_inserts.py::test_append_inserts[arrowsql] | pass |
| test_07_simple_delta.py::test_delta_sys[arrowsql] | pass |
| test_08_simple_delta_check.py::test_delta_sys[arrowsql] | pass |
| test_09_query.py::test_delta_query[arrowsql] | pass |
| test_10_consistency_fix.py::test_delta_query[arrowsql] | pass |
| test_11_schema_drift.py::test_schema_drift[arrowsql] | pass |
| test_12_insert_while_load.py::test_insert_while_load[arrowsql] | pass |
| test_13_type_change.py::test_source_type_widening_does_not_fail_on_stale_staging[arrowsql] | pass |
| test_13_type_change.py::test_source_type_change_int_to_string_spark | skip (column migration is only implemented for Spark; NO_SPARK=1) |
| test_14_rowversion_change_detection.py::test_rowversion_change_detection[arrowsql] | pass |
| test_15_arrowsql_reader_types.py::test_strip_odbc_driver (3 cases) | pass |
| test_15_arrowsql_reader_types.py::test_arrowsql_types_match_odbc_reader_types | pass |
| test_16_load_timestamp.py::test_sliced_source_read_has_one_load_timestamp[arrowsql] | pass |
| test_17_no_updates_shortcut.py::test_unchanged_load_takes_shortcut[arrowsql] | pass |
| test_22_spark_databricks_dialect.py::test_databricks_dialect_detected_via_env_var | pass |
| test_22_spark_databricks_dialect.py::test_plain_spark_dialect_when_not_on_databricks | pass |
| test_dbutils.py::test_dbutils | pass |

No real failures in the baseline. The review findings below are bugs the suite does not cover;
each fix adds its own regression test.

## After the review fixes

Same environment and command, with the regression tests added for the review findings
(test_17 fail-then-retry and late commit, test_18 per-pass timestamps and restore_pk, test_15 time
scale, utf8_strings and braced Driver values): 40 passed, 0 failed, 1 skipped.
