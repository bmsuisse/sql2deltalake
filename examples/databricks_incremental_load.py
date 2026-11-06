# ruff: noqa: F821  (spark and dbutils are notebook globals)
# Databricks notebook: incremental load of SQL Server tables into Unity Catalog tables.
#
# Cluster: a Databricks Runtime that supports Python data sources (the pyspark 4 API), and in the
# notebook:  %pip install "odbc2deltalake[spark-source]"
# The connection string comes from a secret scope; replace the placeholders.

from sql2deltalake.spark import FORMAT, commit, prepare

connection = dbutils.secrets.get(
    scope="<secret-scope>", key="<sqlserver-connection-string>"
)

tables = {
    # source table: (target table, options)
    "dbo.orders": ("main.raw.orders", {"numPartitions": 8}),
    "dbo.order_lines": (
        "main.raw.order_lines",
        {"numPartitions": 16, "partitionColumn": "line_id"},
    ),
    "dbo.audit_log": ("main.raw.audit_log", {"loadMode": "append_inserts"}),
}

for source_table, (target, extra) in tables.items():
    options = prepare(spark, url=connection, table=source_table, target=target, **extra)
    df = spark.read.format(FORMAT).options(**options).load()
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        target
    )
    commit(spark, options)
    # state tables: <target>_state_latest_pk_version, _primary_keys_ts, _last_load_state
