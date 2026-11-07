# Rust rewrite plan: sql2deltalake

Status: step 1 (baseline) done in environment only; see "Baseline" at the end. Source of truth for the
inventory is `odbc2deltalake/` at commit c348764 (0.14.3 + arrowsql reader).

## Target

A Rust core (crate `sql2deltalake-core`) built on `deltalake` (delta-rs), arrow-rs and arrowsql's Rust core
(`ddbc_bindings` today, to be split into a plain `arrowsql-core` rlib; it already has `crate-type = ["cdylib","rlib"]`),
exposed to Python through a PyO3 extension (`sql2deltalake._core`, maturin). The public Python API stays identical;
the package is simply `sql2deltalake`.

## Inventory

Size: ~4.7k lines Python. `db_to_delta.py` (1385) is the load algorithm, the rest is plumbing.

### Movable to Rust (pure logic or thin glue over delta-rs/arrow)

| Python | Lines | What it does | Rust home | Notes |
|---|---|---|---|---|
| `db_to_delta.py` `exec_write_db_to_delta`, `do_full_load`, `do_delta_load`, `do_append_inserts_load`, `do_deletes`, `_handle_additional_updates`, `_write_delta2`, `_load_updates_to_delta`, `_retrieve_primary_key_data`, `_list_to_chunks` | ~1100 | Load-mode state machine (overwrite/append/force_full/append_inserts/simple_delta/simple_delta_check), delta_1/delta_2/PK-ts files, delete detection, "strange update" handling via OPENJSON | `load::{full,delta,append_inserts,simple_delta,deletes,updates}` | The core of the port. Orchestration is IO bound; keep it async (tokio) and sequential per table. |
| `db_to_delta.py` `_get_cols_select`, `_source_convert`, `_get_update_sql`, `_get_latest_pk_query`, `_fetch_load_timestamp`, `_transform_dt`, `_get_type_map`, `_temp_table` | ~300 | Builds the source SELECT (type conversion, trimming, `__timestamp`, rowversion to bigint) and the PK/timestamp queries, via sqlglot | `sqlgen` | See "sqlglot problem" below. This is the riskiest part. |
| `write_init.py` `WriteConfig`, `DBDeltaPathConfigs`, `compat_name`, `get_delta_col`, `make_writer`, `DEFAULT_DATA_TYPE_MAP`, `WriteConfigAndInfos` | 306 | Config, delta-col/PK selection, column-name sanitising | `config`, `plan` | `WriteConfig` stays a Python frozen dataclass (user facing, holds a callable `get_target_name`); converted to a Rust struct at the boundary. `get_target_name` callable: either call back into Python per column or pre-resolve names once in Python and pass a map. Prefer the latter. |
| `metadata.py` | 205 | `get_primary_keys`, `get_columns`, `InformationSchemaColInfo`, compat level (INFORMATION_SCHEMA / sys.columns queries, first-result-set discovery) | `metadata` | Executes through the arrowsql core; no Python needed. Pydantic `FieldWithType`/`InformationSchemaColInfo` become a Rust struct, with a PyO3 mirror (or dict) for the shim. |
| `delta_logger.py` | 162 | Writes load log as a delta table (+ `DeltaStorageBackend`) | `delta_log` | Pure delta-rs append. Python `logging` bridge stays (Rust `log` crate -> pyo3-log). |
| `write_utils/restore_pk.py` | 228 | Rebuilds last PK version view after a table restore / consistency fix | `restore_pk` | Needs a local SQL engine, see below. |
| `consistency.py`, `load_infos.py`, `load_result.py` | ~165 | PK consistency check, local/source ts+count, result dataclasses | `consistency`, `load::infos`, `result` | Result types exposed as PyO3 classes with the same attribute names. |
| `read_utils/delta_rs.py` | 167 | arrow <-> sqlglot type mapping, `DeltaRSDeltaOps` (version, vacuum, restore, properties, set_nullable, column infos) | `delta_ops` | Direct delta-rs calls. `set_nullable` needs a metadata-only schema commit (check delta-rs support; else commit via `CommitBuilder`). |
| `reader/arrowsql_reader.py` | 160 | Normalises arrowsql schema/batches to arrow-odbc shape (large_string->string, time->string, tz->UTC, uint8->int16), writes the stream to delta | `reader::arrowsql` | Becomes a cast stage on the arrow-rs stream between arrowsql and the delta writer; no pyarrow round trip, no GIL. This is where the largest speed win is. |
| `query.py`, `sql_glot_utils.py`, `sql_schema.py`, `odbc_utils.py` | ~175 | Identifier/value quoting, union, count-limit-one, connection string building | `sqlgen::quote`, `connstr` | Trivial. arrowsql core already has `connstr.rs`. |

### The local SQL engine problem (decide before phase 3)

The Python reader runs *local* SQL (DuckDB via `deltalake2db`) against delta tables: `local_execute_sql_to_delta`,
`local_register_view`, `local_register_update_view`, `local_upsert_into`, `local_pylist_to_delta`, `local_execute_sql_to_py`.
The load algorithm uses it for: anti-joins of source PKs against local PKs (deletes), joins of delta_1/delta_2 into the
target (merge), and the PK/timestamp comparison. Options:

1. DataFusion (+ `deltalake` `datafusion` feature): native, same process, SQL dialect differs from DuckDB.
   Recommended. Most queries are joins/anti-joins/`EXCEPT`, which DataFusion handles. Write them as DataFrame API
   calls instead of SQL strings where possible, so no dialect issue at all.
2. Embed DuckDB (`duckdb` crate, bundled): keeps the SQL unchanged, +~30 MB, and delta access needs the
   delta extension or arrow scan. Fallback if DataFusion cannot do a query.
3. Keep calling DuckDB through Python: defeats the purpose.

Merge/upsert: delta-rs `MergeBuilder` (`local_upsert_into`) covers it natively.

### The sqlglot problem

`sqlglot` generates T-SQL / Postgres / Spark SQL / DuckDB from one AST and transpiles between them. There is no Rust
equivalent with the same coverage (`sqlparser-rs` parses and prints but does not transpile). The generated queries are
a small closed set (column select with casts, PK+rowversion select, `WHERE ts > x`, `OPENJSON` join, union, count limit 1).
Plan: a small hand-written query builder with one `Dialect` enum (`Tsql`, `Postgres`, `Spark`, `Databricks`) emitting
text directly, covered by golden tests that compare against sqlglot output for every query shape (generate the
fixtures once from the Python implementation). Do NOT port sqlglot. Source-type -> Delta-type mapping
(`DEFAULT_DATA_TYPE_MAP`, `_build_type`) becomes a plain enum match.

### Must stay in Python

| Python | Why |
|---|---|
| `reader/spark_reader.py` (447) | Needs a SparkSession / JDBC / Spark Connect; no Rust path. Stays as a `DataSourceReader` implementation. Also stays the home for the Databricks dialect detection. |
| `reader/odbc_reader.py`, `reader/adbc_reader.py` | pyodbc / arrow-odbc / ADBC are Python drivers. Keep as legacy readers; once the Rust arrowsql reader is default they are optional extras. Postgres: either keep ADBC or later add a tokio-postgres reader in Rust (phase 6, optional). |
| `destination/databricks.py` (`dbutils`), `destination/azure.py`, `destination/azure_utils.py` | `dbutils`/`azure-identity` credential flows are Python objects. They only need to expose `as_path_options("object_store")` (URL + storage options dict, or a token provider callback) so the Rust core can open the table. Rust-native Azure (object_store with `azure` feature) can take a static key/SAS/token from the options; refreshing tokens needs a Python callback. |
| `destination/file_system.py`, `destination/destination.py` (ABC) | Python ABC is public API (users subclass `Destination`). Keep; Rust receives (url, options) after `str(dest)` / `as_path_options`. Pure local paths and `abfss://` URLs are passed straight through. Operations the load uses on `Destination` (`mkdir`, `exists`, `upload_str`, `modified_time`, `remove`, `/`) get a Rust trait `Store` backed by `object_store`; custom Python `Destination` subclasses are bridged via a PyO3 adapter (slow path, rarely used). |
| `DataSourceReader` ABC (`reader/reader.py`) | Public extension point. Keep the ABC; the Rust engine is selected when the reader is the arrowsql/Rust reader, otherwise the existing Python algorithm runs. This allows the Python implementation to remain as reference and fallback during the migration. |
| `__init__.py`, `write_db_to_delta`, `make_writer`, `WriteConfig`, `WriteConfigAndInfos`, `DBDeltaPathConfigs`, `DEFAULT_DATA_TYPE_MAP`, `logging.py`, `utils.py` | Public API shims (see below). |

## Proposed layout

```
sql2deltalake/
  Cargo.toml                 # workspace
  pyproject.toml             # maturin, python-source = "python"
  crates/
    sql2deltalake-core/      # pure Rust, no PyO3; unit + integration tests with cargo test
      src/
        config.rs            # WriteConfig, LoadMode, path constants
        metadata.rs          # ColInfo, PK discovery, compat level
        sqlgen/{mod,tsql,postgres,spark,quote}.rs
        types.rs             # source type -> arrow/delta type map
        reader/{mod,arrowsql}.rs   # trait SourceReader (+ ArrowsqlReader), cast stage
        store.rs             # trait Store over object_store, local + azure
        delta_ops.rs         # vacuum/restore/properties/set_nullable
        engine.rs            # DataFusion context, delta table registration
        load/{mod,full,delta,append_inserts,simple_delta,deletes,updates}.rs
        restore_pk.rs, consistency.rs, delta_log.rs, result.rs
    sql2deltalake-py/        # PyO3 cdylib (_core): thin conversion layer, GIL released during loads
  python/
    sql2deltalake/           # real package: __init__.py (API), write_init.py (WriteConfig...), destination/, reader/ (spark, adbc, arrowsql)
  tests/                     # the existing pytest suite, unchanged, runs against both engines
```

arrowsql dependency: add a git/path dependency on an `arrowsql-core` rlib. Needs a split in the arrowsql repo (today the
crate is `ddbc_bindings`, mixing PyO3 glue with the TDS/Arrow code). Until then, consume `ddbc_bindings` as an rlib
path dependency and ignore its `pyo3` symbols (works, but pulls pyo3 into the build). Version pins: arrow-rs must match
(arrowsql uses arrow 60); `deltalake` must be on the same arrow major, otherwise bridge through the Arrow C stream
interface (which arrowsql already exports).

## Public API compatibility

- Keep: `write_db_to_delta`, `make_writer`, `WriteConfig` (frozen dataclass, same fields/defaults), `WriteConfigAndInfos`
  (`.execute()`, `.check_delta_consistency()`, `.exec_source_query()`, `.from_()`), `DBDeltaPathConfigs`,
  `DEFAULT_DATA_TYPE_MAP`, `DataSourceReader`, `Destination`, destinations, `LoadResult`/`AppendOnlyLoadResult`.
- The package and the PyPI distribution are `sql2deltalake`; no alias package for the old import name is kept.
- Python 3.9 floor in the current pyproject; arrowsql needs 3.10+. Rust wheels are abi3 (`abi3-py310`), so decide: drop 3.9, or
  keep the pure-Python path for 3.9 (the shim already allows this).
- Behavioural contract = the existing tests (`tests/test_01 ... test_17`). They are the spec; run them on both engines
  via an env switch (`SQL2DELTALAKE_ENGINE=python|rust`) until parity.

## Phases

0. Baseline (this step): existing suite green with arrowsql as reader. Record the numbers.
1. Skeleton: maturin workspace, PyO3 module, CI building wheels, `SQL2DELTALAKE_ENGINE` switch. No logic moved.
2. Leaf logic with golden tests: `types`, `sqlgen` (quote, select builder, 4 dialects), `config`, `metadata` over arrowsql core,
   `delta_ops` (version/vacuum/restore/properties/set_nullable). Golden fixtures generated from the Python implementation.
3. Full load path end to end: arrowsql stream -> cast stage -> delta-rs write (overwrite, append) incl. schema drift (`allow_schema_drift`),
   `__timestamp`, `delta_log`. Makes `test_01, 02, 05, 09, 15, 16` pass on the Rust engine.
4. Incremental loads: PK+ts files, DataFusion engine, `delta`, `simple_delta(_check)`, `append_inserts`, deletes, additional-updates (OPENJSON),
   merge via delta-rs. Tests 03, 04, 06, 07, 08, 11, 12, 13, 14, 17.
5. Consistency/restore: `restore_pk`, `consistency`, `test_10`. Destination bridge (object_store Azure + Python callback for tokens / dbutils).
6. Cut over: Rust default for the arrowsql reader, Python algorithm stays for Spark/ODBC/ADBC readers. Benchmarks vs 0.14.3 (NYC taxi on the local
   SQL Server, 100k and 3M rows). Optional: Postgres reader in Rust.

Risks, in order: (1) sqlglot replacement fidelity, (2) DataFusion vs DuckDB semantics for the PK/ts anti-joins and type edge cases
(decimals, datetimeoffset, rowversion/binary compare), (3) arrow/deltalake version coupling with arrowsql, (4) Azure token refresh through the PyO3 boundary,
(5) delta-rs feature gaps (set_nullable metadata commit, restore, vacuum parity with deltalake-python).

## Baseline (step 1)

Environment: `.venv` (Python 3.13, uv) with `pip install -e .` and test deps; arrowsql via a `.pth` pointing at
`../mssql-python-rs/python` (the prebuilt `ddbc_bindings` .so of the local checkout, same link its own editable install uses).
Run with `scripts/run_tests_local.sh` (config `local`, no Spark, no Azurite, credentials sourced from the arrowsql `.env.local`).
Collection works: 28 tests collected. Execution result: see commit message / hand-off report.
