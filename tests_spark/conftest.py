"""Fixtures for the Spark data source tests: a local[4] SparkSession with Delta, and one fresh
SQL Server database per test (prefix ``s2d_spark_``, or ``S2D_SPARK_DB_PREFIX``), set up with tests/sqls/init_mssql.sql.

The master connection string comes from ODBC_MASTER_CONN (see scripts/run_spark_tests_local.sh).
"""

import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent


def with_database(conn_str: str, database: str) -> str:
    pattern = re.compile(r"(?i)(^|;)\s*(database|initial catalog)\s*=[^;]*")
    if pattern.search(conn_str):
        return pattern.sub(
            lambda m: f"{m.group(1)}Database={database}", conn_str, count=1
        )
    return conn_str.rstrip(";") + f";Database={database}"


def connect(conn_str: str):
    import arrowsql

    from odbc2deltalake.reader.arrowsql_reader import strip_odbc_driver

    return arrowsql.connect(strip_odbc_driver(conn_str), autocommit=True)


def execute(conn_str: str, *statements: str) -> None:
    with connect(conn_str) as conn:
        for sql in statements:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()


def query(conn_str: str, sql: str) -> list:
    with connect(conn_str) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return [tuple(r) for r in cursor.fetchall()]
        finally:
            cursor.close()


@pytest.fixture(scope="session")
def master_conn() -> str:
    conn = os.getenv("ODBC_MASTER_CONN")
    if not conn:
        if os.getenv("CI"):  # in CI a skip would pass without running anything
            pytest.fail("ODBC_MASTER_CONN is not set")
        pytest.skip(
            "ODBC_MASTER_CONN is not set (run scripts/run_spark_tests_local.sh)"
        )
    return conn


@pytest.fixture
def source(request, master_conn: str):
    """A fresh database for the test, with the tables of tests/sqls/init_mssql.sql."""
    test = re.sub(r"\W+", "_", request.node.name.removeprefix("test_")).strip("_")
    name = f"{os.getenv('S2D_SPARK_DB_PREFIX', 's2d_spark_')}{test}"[:100]
    drop = (
        f"IF DB_ID('{name}') IS NOT NULL BEGIN "
        f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{name}]; END"
    )
    execute(master_conn, drop, f"CREATE DATABASE [{name}]")
    conn_str = with_database(master_conn, name)
    init = (ROOT / "tests/sqls/init_mssql.sql").read_text(encoding="utf-8-sig")
    batches = [b for b in init.replace("\r\n", "\n").split("\nGO\n") if b.strip()]
    execute(conn_str, *batches)
    yield conn_str
    if os.getenv("KEEP_S2D_DATABASES", "0") != "1":
        execute(master_conn, drop)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[4]")
        .appName("sql2deltalake_spark_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
