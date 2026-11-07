"""Makes ``spark.read.format("sql2deltalake")`` work in every SparkSession without
``sql2deltalake.spark.register(spark)``: PySpark >= 4.0 imports the installed top-level modules
named ``pyspark_*`` when a session first uses a Python data source, and registers their
``DefaultSource``."""

from sql2deltalake.spark._source import SqlToDeltaDataSource as DefaultSource

__all__ = ["DefaultSource"]
