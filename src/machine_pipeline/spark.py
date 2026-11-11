"""SparkSession factory shared by the pipeline and the tests."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "machine-pipeline") -> SparkSession:
    """Return the active SparkSession, or a small local one.

    On Databricks ``getOrCreate`` returns the cluster session and the local
    settings below are ignored.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")  # small data: avoid 200 tiny tasks
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
