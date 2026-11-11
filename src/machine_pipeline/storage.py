"""Table read/write in one place, so the storage format can be swapped (Parquet locally)."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def write_table(df: DataFrame, path: str | Path) -> None:
    """Full refresh: overwrite keeps reruns idempotent."""
    df.write.mode("overwrite").parquet(str(path))


def read_table(spark: SparkSession, path: str | Path) -> DataFrame:
    return spark.read.parquet(str(path))
