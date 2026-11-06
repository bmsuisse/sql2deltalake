"""Incremental load of a SQL Server table into a Delta table with Spark (local mode).

    pip install "odbc2deltalake[spark-source]" "pyspark>=4.0" "delta-spark>=4.0"
    SQLSERVER_CONNECTION="Server=localhost,1433;Database=shop;Uid=loader;Pwd=...;Encrypt=yes" \
        python examples/spark_incremental_load.py dbo.orders /tmp/lake/orders

Run it again after changing rows in the source: the first run is a full load, later runs append
only the changed rows, delete markers and rows changed in unusual ways (e.g. a restore).
"""

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from sql2deltalake.spark import FORMAT, commit, prepare


def main(table: str, target: str) -> None:
    builder = (
        SparkSession.builder.master("local[4]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # 1. decide what this load reads (reads the target and its state through Spark)
    options = prepare(
        spark,
        url=os.environ["SQLSERVER_CONNECTION"],
        table=table,
        target=target,
        numPartitions=4,  # four slices, hashed on the primary key
    )
    # 2. the rows to append; the data source reads SQL Server through arrowsql
    df = spark.read.format(FORMAT).options(**options).load()
    # 3. Spark writes; append only, never merge
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(target)
    # 4. advance the state (primary keys and delta column as of this load)
    commit(spark, options)

    spark.read.format("delta").load(target).groupBy(
        "__timestamp", "__is_deleted"
    ).count().orderBy("__timestamp").show(truncate=False)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
