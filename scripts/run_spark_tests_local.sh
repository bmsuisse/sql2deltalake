#!/bin/bash
# Runs tests_spark/ (the Spark data source, local[4]) against a local SQL Server through arrowsql.
# Credentials come from the arrowsql checkout's .env.local, sourced into the environment only.
# The tests create and drop their own databases (prefix s2d_spark_).
# Setup, a venv next to .venv because pyspark 4 / delta-spark 4 conflict with the locked test group:
#   uv venv .venv-spark --python 3.13
#   uv export --frozen --extra arrowsql --group test --no-hashes --no-emit-project \
#     | grep -v -E '^(arrowsql|delta-spark|pyspark|py4j)==' > /tmp/reqs.txt
#   VIRTUAL_ENV=$PWD/.venv-spark uv pip install -r /tmp/reqs.txt "pyspark==4.1.0" "delta-spark==4.1.0"
#   VIRTUAL_ENV=$PWD/.venv-spark uv pip install --no-deps -e .
#   echo "$PWD/../mssql-python-rs/python" > .venv-spark/lib/python3.13/site-packages/arrowsql_local.pth
# delta-spark 4.1.0 needs pyspark 4.1.0 exactly (with 4.1.4 it cannot read Delta checkpoints).
# Spark needs Java 17 or 21: JAVA_HOME defaults to ~/.local/jdk/jdk-21 if that exists.
set -o pipefail
cd "$(dirname "$0")/.."
set -a; source "${ARROWSQL_ENV:-../mssql-python-rs/.env.local}"; set +a
export ODBC_MASTER_CONN="$DB_CONNECTION_STRING"
if [ -z "$JAVA_HOME" ] && [ -d "$HOME/.local/jdk/jdk-21" ]; then export JAVA_HOME="$HOME/.local/jdk/jdk-21"; fi
export PYSPARK_PYTHON="$PWD/.venv-spark/bin/python" PYSPARK_DRIVER_PYTHON="$PWD/.venv-spark/bin/python"
# own basetemp: other pytest runs of the same user rotate (delete) /tmp/pytest-of-<user>/pytest-N
.venv-spark/bin/python -m pytest tests_spark --basetemp="$PWD/.pytest-spark-tmp" "$@" 2>&1 | sed "s/$MSSQL_SA_PASSWORD/***/g"
