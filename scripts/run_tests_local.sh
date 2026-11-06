#!/bin/bash
# Runs tests/ against a local SQL Server through arrowsql (no ODBC driver, no Spark, no Azurite).
# Credentials come from the arrowsql checkout's .env.local, sourced into the environment only.
# Setup: uv venv --python 3.13 .venv; uv pip install -e . pytest ... ; and a .pth in .venv
# pointing at ../mssql-python-rs/python (prebuilt ddbc_bindings .so, same as arrowsql's own editable install).
cd "$(dirname "$0")/.."
set -a; source "${ARROWSQL_ENV:-../mssql-python-rs/.env.local}"; set +a
export ODBC_MASTER_CONN="$DB_CONNECTION_STRING"
export ODBCLAKE_TEST_CONFIGURATION="${ODBCLAKE_TEST_CONFIGURATION:-local}" NO_SQL_SERVER=1 NO_SPARK=1 ODBCLAKE_TEST_SOURCE_SERVER=mssql
.venv/bin/python -m pytest tests "$@" 2>&1 | sed "s/$MSSQL_SA_PASSWORD/***/g"
