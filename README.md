<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img src="docs/assets/logo.svg" alt="sql2deltalake" width="460">
  </picture>
</p>

<p align="center">
  Incremental loads from SQL Server and Postgres into an append-only SCD2 Delta table.
</p>

sql2deltalake reads a source table (or query) and keeps a Delta table in sync with it, without a full reload every time. The Delta table is append-only and always has these system columns:

- `__timestamp`: the date of the load
- `__is_deleted`: true for the deletion of a record
- `__is_full_load`: true if the row came from a full load, which means an implicit deletion of records that were not delivered

It is the successor to `odbc2deltalake` and is designed to run together with [arrowsql](https://github.com/bmsuisse/arrowsql), a Rust TDS driver for SQL Server with Arrow output that needs no ODBC driver. The focus is Microsoft SQL Server and Postgres. Other databases should not be hard to add.

## Status

- The PyPI distribution and the import name are being renamed from `odbc2deltalake` to `sql2deltalake`. Until the first `sql2deltalake` release the code is imported as `odbc2deltalake` (a `sql2deltalake` import alias will be added). The `odbc2deltalake` package on PyPI is the previous release line.
- Planned, not implemented yet: a Spark data source (`spark.read.format(...)`) and a Rust core (see [RUST_PLAN.md](RUST_PLAN.md)). The public Python API is meant to stay the same.

## Quick start

Examples use the current import name `odbc2deltalake`.

### With arrowsql (recommended, no ODBC driver)

`ArrowSQLReader` uses the same local engine as the ODBC reader (DuckDB and delta-rs), but every read from SQL Server goes through arrowsql. Requires Python 3.10+.

```bash
pip install "odbc2deltalake[arrowsql]"
```

```python
from pathlib import Path
from odbc2deltalake import write_db_to_delta
from odbc2deltalake.reader.arrowsql_reader import ArrowSQLReader

reader = ArrowSQLReader("Server=myhost,1433;Database=mydb;Uid=me;Pwd=secret;Encrypt=yes")
write_db_to_delta(reader, ("dbo", "user"), Path("YOUR_TARGET_PATH"))
```

An ODBC `Driver=` keyword in the connection string is ignored.

`ArrowSQLReader(..., utf8_strings=True)` transfers nvarchar/nchar/ntext columns as UTF-8, which is about half the bytes for mostly-ASCII text. To decide, arrowsql runs a describe and a sample of the first 256 rows of each data query, so the server runs part of every data query twice. It is off by default.

### With ODBC

Needs an ODBC driver for SQL Server.

```bash
pip install "odbc2deltalake[local]"          # local files
pip install "odbc2deltalake[local_azure]"    # also Azure storage (adlfs)
```

```python
from pathlib import Path
from odbc2deltalake import write_db_to_delta

write_db_to_delta(
    your_odbc_connection_string,
    ("dbo", "user"),
    Path("YOUR_TARGET_PATH"),
)
```

A connection string is turned into an `ODBCReader`. For Postgres use `ADBCReader` (`pip install "odbc2deltalake[postgres,local]"`) with an ADBC connection.

### On Databricks (Spark)

Instead of delta-rs/DuckDB you can use Spark for reading and writing. `pip install odbc2deltalake` is enough there, with no dependencies beyond Databricks Spark and dbutils.

```python
from odbc2deltalake import write_db_to_delta
from odbc2deltalake.destination.databricks import DatabricksDestination
from odbc2deltalake.reader.spark_reader import SparkReader

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

## Query as source

Instead of a table or view you can pass a sqlglot query, for example to sync only a subset. Set the primary keys, because they cannot be determined from a query.

```python
import sqlglot as sg
from odbc2deltalake import write_db_to_delta, WriteConfig

query = sg.parse_one("select * from dbo.[user] where age > 50", dialect="tsql")
write_db_to_delta(reader, query, dest, WriteConfig(primary_keys=["user_id"]))
```

## Advanced

### Get the writer

`make_writer` builds the writer without running it, so you can inspect the primary keys and columns that will be used. Note the argument order: source, table or query, destination.

```python
from odbc2deltalake import make_writer, WriteConfig

writer = make_writer(reader, query, dest, WriteConfig(primary_keys=["user_id"]))
# inspect the primary keys and other infos here
writer.execute()  # really do it
```

### Configuration

Pass a `WriteConfig` to `write_db_to_delta` or `make_writer`. All fields are optional. The class lives in `odbc2deltalake/write_init.py`.

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

### Strange updates

In practice the delta value does not always move forward. After a restore of the source database you can see older rowversions again. The load detects such rows by comparing delta values per primary key against the last known state, and loads these "strange updates" in an extra step (via `OPENJSON` unless `no_complex_entries_load` is set).

## Development

Tests need a local SQL Server. The script reads credentials from an env file and runs pytest through arrowsql, without ODBC driver, Spark or Azurite:

```bash
scripts/run_tests_local.sh          # ODBCLAKE_TEST_CONFIGURATION=arrowsql is the default
scripts/run_tests_local.sh -k test_delta
```

See the header of [scripts/run_tests_local.sh](scripts/run_tests_local.sh) for the environment setup. Test results of the current source tree are in [BASELINE.md](BASELINE.md), the plan for the Rust core is in [RUST_PLAN.md](RUST_PLAN.md).

## Related project

We recommend using our [Fabricks Framework](https://github.com/fabricks-framework/fabricks) to do the ETL, but of course you can use it without it as well.
