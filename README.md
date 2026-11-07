<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/bmsuisse/sql2deltalake/main/docs/assets/logo-dark.svg">
    <img src="https://raw.githubusercontent.com/bmsuisse/sql2deltalake/main/docs/assets/logo.svg" alt="sql2deltalake" width="460">
  </picture>
</p>

<p align="center">
  Incremental loads from SQL Server and Postgres into an append-only SCD2 Delta table.
</p>

sql2deltalake reads a source table (or query) and keeps a Delta table in sync with it, without a full reload every time. The Delta table is append-only and always has these system columns:

- `__timestamp`: the date of the load
- `__is_deleted`: true for the deletion of a record
- `__is_full_load`: true if the row came from a full load, which means an implicit deletion of records that were not delivered

SQL Server is read through [arrowsql](https://github.com/bmsuisse/arrowsql), a Rust TDS driver with Arrow output. The load itself runs in a Rust core (`crates/sql2deltalake-core`, exposed to Python as `sql2deltalake._core` by `crates/sql2deltalake-py`; see [RUST_PORT.md](RUST_PORT.md)). Delta tables are written locally with the Python `deltalake` package (DuckDB for local queries), or with Spark on Databricks. Postgres is read through ADBC and still uses the Python implementation of the load, like the Spark JDBC reader. The focus is Microsoft SQL Server and Postgres. Other databases should not be hard to add.

## Quick start

### SQL Server (arrowsql)

`ArrowSQLReader` reads SQL Server through arrowsql and writes the Delta table locally with delta-rs (DuckDB for local queries). Requires Python 3.10+.

```bash
pip install "sql2deltalake[arrowsql]"
```

```python
from pathlib import Path
from sql2deltalake import write_db_to_delta
from sql2deltalake.reader.arrowsql_reader import ArrowSQLReader

reader = ArrowSQLReader("Server=myhost,1433;Database=mydb;Uid=me;Pwd=secret;Encrypt=yes")
write_db_to_delta(reader, ("dbo", "user"), Path("YOUR_TARGET_PATH"))
```

A connection string passed directly to `write_db_to_delta` or `make_writer` is turned into an `ArrowSQLReader` (an `ADBCReader` if `WriteConfig.dialect` is `"postgres"`). arrowsql speaks TDS itself, so a `Driver=` keyword in the string is ignored.

With an `ArrowSQLReader` the load runs in the Rust core; set `SQL2DELTA_NATIVE=0` to run the Python implementation instead. `ArrowSQLReader(..., partitions=4)` reads large tables
on 4 parallel connections, split by ranges of an integer primary key.

`ArrowSQLReader(..., utf8_strings=True)` transfers nvarchar/nchar/ntext columns as UTF-8, which is about half the bytes for mostly-ASCII text. To decide, arrowsql runs a describe and a sample of the first 256 rows of each data query, so the server runs part of every data query twice. It is off by default.

### Postgres (ADBC)

Use `ADBCReader` (`pip install "sql2deltalake[postgres,local]"`) with an ADBC connection, or pass a Postgres connection string with `WriteConfig(dialect="postgres")`. The load runs in Python.

```python
from pathlib import Path
import adbc_driver_postgresql.dbapi
from sql2deltalake import write_db_to_delta, WriteConfig
from sql2deltalake.reader.adbc_reader import ADBCReader

reader = ADBCReader(adbc_driver_postgresql.dbapi.connect("postgresql://me:secret@myhost/mydb", autocommit=True))
write_db_to_delta(reader, ("public", "user"), Path("YOUR_TARGET_PATH"), WriteConfig(dialect="postgres"))
```

### On Databricks (Spark)

Instead of delta-rs/DuckDB you can use Spark for reading and writing. `pip install sql2deltalake` is enough there, with no dependencies beyond Databricks Spark and dbutils.

```python
from sql2deltalake import write_db_to_delta
from sql2deltalake.destination.databricks import DatabricksDestination
from sql2deltalake.reader.spark_reader import SparkReader

dest = DatabricksDestination(
    dbutils=dbutils, container="raw", account="some_account", path="raw/super_path", scheme="abfss"
)
reader = SparkReader(
    spark=spark,
    sql_config={  # see https://docs.databricks.com/en/connect/external-systems/sql-server.html
        "host": "some_host",
        "port": "1433",
        "user": "some_user",
        "encrypt": "true",
        "password": "what_ever",
        "database": "mydb",
    },
    spark_format="sqlserver",
)

write_db_to_delta(reader, ("dbo", "user"), dest)
```

### Spark data source

With Spark (pyspark >= 4.0, e.g. on Databricks), Spark can do all the Delta writing: the `sql2deltalake` data source returns the rows a load appends, and the load is a plain append.

`pip install "sql2deltalake[spark-source]"` (on the driver and the executors; SQL Server only, read through arrowsql)

```python
from sql2deltalake.spark import FORMAT, commit, prepare  # FORMAT == "sql2deltalake"

options = prepare(spark, url=conn_str, table="dbo.orders", target="/lake/orders", numPartitions=8)
df = spark.read.format(FORMAT).options(**options).load()
df.write.format("delta").mode("append").option("mergeSchema", "true").save("/lake/orders")
commit(spark, options)
```

`target` is a Delta path (contains `/` or `:`) or a table name (then `saveAsTable(target)`).

The load logic is the one of `write_db_to_delta`, with the same SQL and the same result (the test suite in `tests_spark/` compares both value for value):

* No target, `loadMode="force_full"`, no primary key or no delta column, or new source columns (hence `mergeSchema`): a full load, `__is_full_load` true.
* Otherwise a delta load: the source's MAX/COUNT of the delta column, a snapshot of primary keys and delta column (stored as `primary_keys_ts`), compared with the keys of the last load (`latest_pk_version`). It returns the rows with a delta value above the watermark, the rows whose delta value changed in other ways ("strange updates", e.g. after a restore; up to 1000 by key, more by re-reading from their lowest delta value) and a delete marker (`__is_deleted` true, NULL in all other columns) per key that is gone.
* A source column whose type changed (checked when the source schema changed since the last load), with the rule of `SparkReader`: if the target column holds every value of the new type (bigint in the target, int in the source; a string column), the rows are cast to the target's type; if the new type holds every value of the target column (int to bigint, decimal(18,2) to decimal(20,4)), `prepare` rewrites the target with the column converted (same rows; the state stays valid, a key or delta column means a full load); otherwise `prepare` fails with an error naming the column and both types, before anything is written.
* Nothing changed (MAX, COUNT and schema as committed by the last load, and for rowversion every rowversion up to MAX committed then): an empty DataFrame.
* `loadMode`: `append` (default), `force_full`, `append_inserts`, `simple_delta`, `simple_delta_check`, as in `WriteConfig.load_mode`.

`__timestamp` is the source server's UTC clock, read once per pass and the same in every slice. Delete markers get the first one, the rows the second, re-read strange updates a later one. Every load's timestamps are later than the target's latest.

Options (case-insensitive, camelCase or snake_case): `url` (the connection string; a `Driver=` keyword is ignored), `table` or `query`, `target`, `keys` (comma separated; default: the table's primary key), `deltaColumn` (default: rowversion, `__timestamp` or the temporal period start column), `loadMode`, `numPartitions` (slices; hashed with `CHECKSUM` on the key columns, so every slice query selects the same rows), `partitionColumn` (range slices on one integer, decimal, float, date or datetime column instead, bounds from its MIN/MAX), `batchSize`, `utf8Strings`, `noTrim`, `noComplexEntriesLoad`, `maxInlineDeletes` (default 100000; above it a full load is done instead, since the delete markers travel inside the plan), `state` (state location, default `<target>_state/` for a path, `<target>_state_*` tables for a name).

Why `prepare` and `commit`: Spark runs a Python data source (`schema()`, `reader()`, `partitions()`, `read()`, and a writer's `commit()`) in Python worker processes without a SparkSession, so the data source cannot read or write the target and the state tables. `prepare` reads them on the driver and passes the decisions to the data source as the `plan` option; `commit` writes the new state with Spark after the append. The state is a cache: `last_load_state` records the target's MAX(`__timestamp`) and COUNT(*) it belongs to, and if they do not match (no `commit`, a failed append) `prepare` rebuilds `latest_pk_version` from the target itself. `commit` takes the rows of this load from the target, so a `commit` without the append keeps the old state for those keys and the next load reads them again. Without `target`, `spark.read.format("sql2deltalake").option("url", ...).option("table", ...).load()` is a full load of the source. `commit` of a plan that is committed already does nothing, so a retry of `commit` is safe; a plan another load overtook raises `StalePlanError`. Run one load per target at a time. The driver keeps the key state `prepare` read for `commit` and the next `prepare`, for the 4 targets it used last (environment variable `SQL2DELTA_SPARK_STATE_CACHE`, 0 keeps none).

Differences to `write_db_to_delta`: `simple_delta_check` decides before reading (it counts the keys of the changed rows) and then does either a simple or a full delta load, not both; delete markers carry a timestamp of the source server read before the rows, not the local clock after them; a row that is in the snapshot with a new delta value but deleted before its rows are read gets its delete marker in the next load, not in this one.

## Query as source

Instead of a table or view you can pass a sqlglot query, for example to sync only a subset. Set the primary keys, because they cannot be determined from a query.

```python
import sqlglot as sg
from sql2deltalake import write_db_to_delta, WriteConfig

query = sg.parse_one("select * from dbo.[user] where age > 50", dialect="tsql")
write_db_to_delta(reader, query, dest, WriteConfig(primary_keys=["user_id"]))
```

## Advanced

### Get the writer

`make_writer` builds the writer without running it, so you can inspect the primary keys and columns that will be used. Note the argument order: source, table or query, destination.

```python
from sql2deltalake import make_writer, WriteConfig

writer = make_writer(reader, query, dest, WriteConfig(primary_keys=["user_id"]))
# inspect the primary keys and other infos here
writer.execute()  # really do it
```

### Configuration

Pass a `WriteConfig` to `write_db_to_delta` or `make_writer`. All fields are optional. The class lives in `sql2deltalake/write_init.py`.

| Field | Default | Meaning |
|---|---|---|
| `dialect` | `"tsql"` | sqlglot dialect used to generate SQL against the source. |
| `primary_keys` | `None` | Primary keys for the delta load. If `None`, they are determined from the source. |
| `delta_col` | `None` | Column used for the delta load. If `None`, it is determined from the source. Should be mostly increasing to keep loads efficient. |
| `load_mode` | `"append"` | See the table below. |
| `data_type_map` | `"default"` | Map source types before writing, for example decimal to double. The default maps `datetime`/`datetime2` to `datetime2(6)`, `rowversion`/`timestamp` to bigint and `tinyint` to smallint for T-SQL, and nothing for Postgres. Doing other conversions later in the ETL is usually better. |
| `no_complex_entries_load` | `False` | If true, "strange updates" are not loaded via `OPENJSON`. Use it if the source has no `OPENJSON`, or if you accept some extra updates to reduce complexity. |
| `get_target_name` | removes spaces and other characters Spark dislikes | Callable returning the target column name for a source column, to apply a naming convention. |
| `allow_schema_drift` | `"new_only"` | Controls schema drift on write. New source columns trigger a full load. |
| `no_trim` | `False` | If true, strings are not trimmed in the source. |

### Load modes

| `load_mode` | Behavior |
|---|---|
| `append` | Default. Delta load against the existing Delta table, falling back to a full load when needed (first load, no delta column, new columns, ...). Deletes are detected via primary keys. |
| `overwrite` | Overwrites the table. Does not build SCD2: the history exists in the Delta table only. |
| `force_full` | Always does a full load. |
| `append_inserts` | For a strictly increasing delta column, when only new rows are wanted. Rows are never deleted. Suited for logs. |
| `simple_delta` | For sources where the delta column is a datetime and there are certainly no deletes or additional updates. |
| `simple_delta_check` | Like `simple_delta`, but checks for deletes if the row count does not match. It then runs `simple_delta` and a normal delta load, so use it only if deletes are rare. |

## How it works

### Timestamp tracking for MS-SQL

The SQL Server type `rowversion` (aka `timestamp`) is a bigint that increases on every update. The delta column is picked in this order: a `rowversion`/`timestamp` column, a temporal table row-start column, a column named `__timestamp`. For Postgres tables the `xmin` column is used. Set `WriteConfig.delta_col` to use another column. A delta load then does this:

1. Read the latest delta value and the row count from the Delta table.
2. Read the maximum delta value and the row count from the source.
3. Fetch the primary keys and delta values of the source, and load all rows with a newer delta value.
4. Detect deletes: primary keys missing in the source are written with `__is_deleted = true`.

No-updates shortcut: before it reads any data, a load on a rowversion column reads `MIN_ACTIVE_ROWVERSION()` from the source. A load state is stored next to the data after each successful load (the primary-key snapshot it wrote, a fingerprint of the source schema and that watermark). On the next run the load is skipped ("No updates") only if the state still matches, the maximum delta value and row count equal the source, and the maximum is below the stored watermark. This guards against a transaction that was still open and commits a lower rowversion later, which `MAX(rowversion)` alone cannot detect.

Failed loads restore state: if a load raises, the primary-key snapshots in the `delta_load` folder are restored to the version before the load (or removed), and the lock file is cleaned up, so the next run starts from the last consistent state.

One load per target at a time: a load creates `meta/lock.txt` (only if it does not exist) and a second load of the same target raises `sql2deltalake.LoadAlreadyRunning` while that lock is less than an hour old; an older lock counts as left behind by a dead load and is taken over.

### Strange updates

In practice the delta value does not always move forward. After a restore of the source database you can see older rowversions again. The load detects such rows by comparing delta values per primary key against the last known state, and loads these "strange updates" in an extra step (via `OPENJSON` unless `no_complex_entries_load` is set).

## Known limitations

* **Sub-microsecond time values:** Delta timestamps keep microseconds. `datetime2(7)` and `datetimeoffset(7)` are rounded to the microsecond (by SQL Server, half up), so two `datetime2(7)` primary keys that differ only in the 7th digit become one key in the target. `time(n)` is stored as text and keeps all digits.
* **`decimal(38,0)` values of about ±10^37 and more** cannot be written: delta-rs 0.18 fails on their statistics (`parse decimal overflow`). Smaller precisions and values are fine.
* **`sql_variant` and `hierarchyid` columns** cannot be loaded from a table: the load stops before reading anything and names the column. Load a query that converts them instead (`CAST(col AS nvarchar(4000))`, `col.ToString()`). `text` and `ntext` are read as `nvarchar(max)`; `geography` and `geometry` as binary.
* **Schema drift other than new columns** (`write_db_to_delta`): a new source column is added to the target. Dropping a source column, or widening a type once values arrive that the target column cannot hold (`int` to `bigint` above 2^31, `int` to `varchar` with text, the same for a primary key), makes every later load fail with a cast or schema error. Nothing is lost silently; a `force_full` load into a new target (or a migration of the target table) recovers.
* **No primary key or no delta column:** every load is a full load (`__is_full_load` true); rows of earlier loads get no delete markers.

## Development

The load logic lives in a Rust core (`crates/sql2deltalake-core`), exposed to Python as the extension module
`sql2deltalake._core` (`crates/sql2deltalake-py`, PyO3, built with maturin). `uv sync` builds it; after a change to
the Rust code, rebuild it into the virtual environment with

```bash
maturin develop --uv           # debug build, add --release for benchmarks
cargo test -p sql2deltalake-core
```

Tests need a SQL Server (or Postgres) to read from. The script reads credentials from an env file and runs pytest through arrowsql, without Spark or Azurite. The test variables keep their historical `ODBCLAKE_` prefix:

```bash
scripts/run_tests_local.sh          # ODBCLAKE_TEST_CONFIGURATION=arrowsql is the default
scripts/run_tests_local.sh -k test_delta
```

Longer scenarios against SQL Server (wide tables over many random cycles, a killed connection, an exception at every step of a load, SIGINT, a source restored from a backup, schema drift, parallel loads) are opt-in:

```bash
SQL2DELTA_REALISTIC=1 scripts/run_tests_local.sh tests/test_5*_realistic_*.py   # SQL2DELTA_RT_BIG_ROWS=5000000 adds the 5M-row test
```

See the header of [scripts/run_tests_local.sh](scripts/run_tests_local.sh) for the environment setup. The plan for the Rust core is in [RUST_PLAN.md](RUST_PLAN.md), its progress in [RUST_PORT.md](RUST_PORT.md), and an early test baseline in [BASELINE.md](BASELINE.md).

## Related project

We recommend using our [Fabricks Framework](https://github.com/fabricks-framework/fabricks) to do the ETL, but of course you can use it without it as well.

Formerly published as odbc2deltalake; the old package remains on PyPI at its last release.
