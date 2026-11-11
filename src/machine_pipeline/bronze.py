"""Bronze layer: land raw CSV as-is in Parquet, plus ingestion metadata.

All columns are read as strings on purpose. Casting happens in silver, so a
malformed value (e.g. ``cycle_time_s = "n/a"``) is kept and later quarantined
with a reason instead of silently becoming null here.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from machine_pipeline.config import RAW_COLUMNS

RAW_SCHEMA = StructType([StructField(name, StringType(), nullable=True) for name in RAW_COLUMNS])


def read_raw(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read raw CSV with the explicit schema; fail if the header does not match."""
    return (
        spark.read.option("header", True)
        .option("enforceSchema", False)  # validate header names against RAW_SCHEMA
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(str(path))
    )


def with_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add ``source_file`` and ``ingested_at`` to a file-based DataFrame.

    Uses the ``_metadata`` column instead of ``input_file_name()``, because it
    also works on Databricks Unity Catalog.
    """
    return df.select(
        *RAW_COLUMNS,
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


def transform_bronze(spark: SparkSession, raw_path: str | Path) -> DataFrame:
    return with_ingestion_metadata(read_raw(spark, raw_path))
