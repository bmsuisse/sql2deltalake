#!/bin/bash
# Runs tests/ against a local SQL Server through arrowsql (no Spark, no Azurite).
# Credentials come from the arrowsql checkout's .env.local, sourced into the environment only.
# Setup (versions from uv.lock; arrowsql comes from the local checkout, Spark is not needed):
#   uv sync --python 3.13 --frozen --inexact --no-dev --extra arrowsql --group test \
#     --no-install-package pyspark --no-install-package delta-spark --no-install-package arrowsql
#   echo "$PWD/../mssql-python-rs/python" > .venv/lib/python3.13/site-packages/arrowsql_local.pth
# The .pth points at the prebuilt ddbc_bindings .so, same as arrowsql's own editable install;
# uv sync recreates the venv when the interpreter changes, so re-add the .pth after that.
# The Rust extension (sql2deltalake._core) is built by uv sync; rebuild it with `maturin develop --uv`.
set -o pipefail
cd "$(dirname "$0")/.."
set -a; source "${ARROWSQL_ENV:-../mssql-python-rs/.env.local}"; set +a
export ODBC_MASTER_CONN="$DB_CONNECTION_STRING"
# "arrowsql" and "local" both read SQL Server through ArrowSQLReader into local files.
export ODBCLAKE_TEST_CONFIGURATION="${ODBCLAKE_TEST_CONFIGURATION:-arrowsql}" NO_SQL_SERVER=1 NO_SPARK=1 ODBCLAKE_TEST_SOURCE_SERVER=mssql
.venv/bin/python -m pytest tests "$@" 2>&1 | sed "s/$MSSQL_SA_PASSWORD/***/g"
