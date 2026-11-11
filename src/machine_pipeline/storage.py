"""Table read/write in one place, so storage can differ per environment.

Locally the layers are Parquet folders. On Databricks the same DataFrames are
written as Unity Catalog Delta tables. Transformations never touch storage.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def write_table(df: DataFrame, path: str | Path) -> None:
    """Parquet full refresh: overwrite keeps reruns idempotent."""
    df.write.mode("overwrite").parquet(str(path))


def read_table(spark: SparkSession, path: str | Path) -> DataFrame:
    return spark.read.parquet(str(path))


def write_delta_table(df: DataFrame, table: str) -> None:
    """Delta full refresh of a catalog table such as ``workspace.machine_pipeline.silver``.

    Overwrite keeps reruns idempotent; Delta keeps the old versions in its
    history (time travel), so a bad run can be inspected or rolled back.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def read_delta_table(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.table(table)
