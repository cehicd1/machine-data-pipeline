"""Silver layer: typed, validated, deduplicated parts with gap to previous part.

Flow: cast -> validate -> split (valid / quarantine) -> deduplicate -> add gap.
Each step is a small DataFrame -> DataFrame function so it can be tested alone.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from machine_pipeline.config import RAW_TIMESTAMP_FORMAT

REASON_COL = "quarantine_reason"

SILVER_COLUMNS: tuple[str, ...] = (
    "part_id",
    "machine_id",
    "product_code",
    "produced_at",
    "cycle_time_s",
    "is_scrap",
    "gap_s",
    "source_file",
    "ingested_at",
)


def _blank(col: str) -> Column:
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


def cast_types(df: DataFrame) -> DataFrame:
    """Add typed ``*_typed`` columns. Invalid values become null, never errors.

    ``try_*`` functions are needed because Spark 4 runs in ANSI mode, where a
    plain ``cast("n/a" as double)`` raises instead of returning null.
    """
    scrap = F.lower(F.trim("is_scrap"))
    return df.withColumns(
        {
            "produced_at_typed": F.try_to_timestamp("produced_at", F.lit(RAW_TIMESTAMP_FORMAT)),
            # SQL try_cast also works on Spark Connect clients older than 4.0.
            "cycle_time_s_typed": F.expr("try_cast(cycle_time_s AS DOUBLE)"),
            "is_scrap_typed": F.when(scrap == "true", True).when(scrap == "false", False),
        }
    )


def add_quarantine_reason(df: DataFrame) -> DataFrame:
    """Add ``quarantine_reason``: all failed rules joined by '; ', or null if valid."""
    rules: list[tuple[Column, str]] = [
        (_blank("part_id"), "missing part_id"),
        (_blank("machine_id"), "missing machine_id"),
        (_blank("product_code"), "missing product_code"),
        (F.col("produced_at_typed").isNull(), "invalid produced_at"),
        (F.col("cycle_time_s_typed").isNull(), "invalid cycle_time_s"),
        (F.col("cycle_time_s_typed") <= 0, "non-positive cycle_time_s"),
        (F.col("is_scrap_typed").isNull(), "invalid is_scrap"),
    ]
    reasons = F.concat_ws("; ", *[F.when(cond, F.lit(msg)) for cond, msg in rules])
    return df.withColumn(REASON_COL, F.when(reasons != "", reasons))


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``.

    Valid rows get the typed columns under their final names. Quarantined rows
    keep the raw string values, so the original input stays visible.
    """
    invalid = F.col(REASON_COL).isNotNull()
    valid = df.filter(~invalid).select(
        "part_id",
        "machine_id",
        "product_code",
        F.col("produced_at_typed").alias("produced_at"),
        F.col("cycle_time_s_typed").alias("cycle_time_s"),
        F.col("is_scrap_typed").alias("is_scrap"),
        "source_file",
        "ingested_at",
    )
    quarantine = df.filter(invalid).drop(
        "produced_at_typed", "cycle_time_s_typed", "is_scrap_typed"
    )
    return valid, quarantine


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per ``part_id``; the most recently ingested one wins."""
    w = Window.partitionBy("part_id").orderBy(F.col("ingested_at").desc(), "source_file")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_gap(df: DataFrame) -> DataFrame:
    """Add ``gap_s``: seconds since the previous part on the same machine.

    Null for the first part of each machine. ``part_id`` breaks timestamp ties
    so the result is deterministic.
    """
    w = Window.partitionBy("machine_id").orderBy("produced_at", "part_id")
    prev = F.lag("produced_at").over(w)
    return df.withColumn("gap_s", F.unix_seconds("produced_at") - F.unix_seconds(prev))


def transform_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Full silver transform. Returns ``(silver, quarantine)``."""
    valid, quarantine = split_valid(add_quarantine_reason(cast_types(bronze)))
    silver = add_gap(deduplicate(valid)).select(*SILVER_COLUMNS)
    return silver, quarantine
