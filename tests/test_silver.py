from __future__ import annotations

from collections.abc import Callable

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from machine_pipeline.silver import transform_silver

BRONZE_SCHEMA = (
    "part_id string, machine_id string, product_code string, produced_at string, "
    "cycle_time_s string, is_scrap string, source_file string, ingested_at string"
)


def bronze_row(**overrides: str | None) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        "part_id": "M01-1",
        "machine_id": "M01",
        "product_code": "P-100",
        "produced_at": "2026-01-05 08:00:00",
        "cycle_time_s": "45.0",
        "is_scrap": "false",
        "source_file": "production.csv",
        "ingested_at": "2026-01-06 00:00:00",
    }
    row.update(overrides)
    return row


def bronze_df(spark: SparkSession, rows: list[dict[str, str | None]]) -> DataFrame:
    df = spark.createDataFrame([tuple(r.values()) for r in rows], BRONZE_SCHEMA)
    return df.withColumn("ingested_at", F.to_timestamp("ingested_at"))


def test_valid_row_is_typed(spark: SparkSession) -> None:
    silver, quarantine = transform_silver(bronze_df(spark, [bronze_row(is_scrap="TRUE")]))

    # Format in Spark: collect() would convert timestamps to the local time zone.
    [row] = silver.withColumn("ts", F.date_format("produced_at", "yyyy-MM-dd HH:mm:ss")).collect()
    assert quarantine.count() == 0
    assert dict(silver.dtypes)["produced_at"] == "timestamp"
    assert row.ts == "2026-01-05 08:00:00"
    assert row.cycle_time_s == 45.0
    assert row.is_scrap is True


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("machine_id", "", "missing machine_id"),
        ("machine_id", None, "missing machine_id"),
        ("product_code", " ", "missing product_code"),
        ("produced_at", "2026-02-30 25:61:00", "invalid produced_at"),
        ("cycle_time_s", "n/a", "invalid cycle_time_s"),
        ("cycle_time_s", "-12.5", "non-positive cycle_time_s"),
        ("cycle_time_s", "0", "non-positive cycle_time_s"),
        ("is_scrap", "maybe", "invalid is_scrap"),
    ],
)
def test_malformed_rows_are_quarantined_with_reason(
    spark: SparkSession, field: str, value: str | None, reason: str
) -> None:
    silver, quarantine = transform_silver(bronze_df(spark, [bronze_row(**{field: value})]))

    [row] = quarantine.collect()
    assert silver.count() == 0
    assert row.quarantine_reason == reason
    assert row[field] == value  # raw input is kept for inspection


def test_all_failed_rules_are_reported(spark: SparkSession) -> None:
    row = bronze_row(machine_id="", cycle_time_s="n/a", is_scrap="maybe")
    _, quarantine = transform_silver(bronze_df(spark, [row]))

    reason = quarantine.first().quarantine_reason
    assert reason == "missing machine_id; invalid cycle_time_s; invalid is_scrap"


def test_exact_duplicates_are_removed(spark: SparkSession) -> None:
    rows = [bronze_row(), bronze_row(), bronze_row(part_id="M01-2")]
    silver, _ = transform_silver(bronze_df(spark, rows))

    assert sorted(r.part_id for r in silver.collect()) == ["M01-1", "M01-2"]


def test_latest_ingested_row_wins(spark: SparkSession) -> None:
    rows = [
        bronze_row(cycle_time_s="45.0", ingested_at="2026-01-06 00:00:00"),
        bronze_row(cycle_time_s="47.0", ingested_at="2026-01-07 00:00:00"),
    ]
    silver, _ = transform_silver(bronze_df(spark, rows))

    [row] = silver.collect()
    assert row.cycle_time_s == 47.0


def test_invalid_copy_does_not_replace_valid_row(spark: SparkSession) -> None:
    rows = [
        bronze_row(ingested_at="2026-01-06 00:00:00"),
        bronze_row(cycle_time_s="n/a", ingested_at="2026-01-07 00:00:00"),  # newer but broken
    ]
    silver, quarantine = transform_silver(bronze_df(spark, rows))

    assert [r.cycle_time_s for r in silver.collect()] == [45.0]
    assert quarantine.count() == 1


def test_gap_is_computed_per_machine(make_silver: Callable[..., DataFrame]) -> None:
    # Rows are interleaved and out of order on purpose.
    silver = make_silver(
        [
            ("M01", "2026-01-05 08:02:00", 60),
            ("M02", "2026-01-05 08:01:00", 60),
            ("M01", "2026-01-05 08:00:00", 60),
            ("M02", "2026-01-05 08:01:30", 30),
        ]
    )

    gaps = {r.part_id: r.gap_s for r in silver.collect()}
    assert gaps == {
        "M01-00002": None,  # first part of M01
        "M01-00000": 120,
        "M02-00001": None,  # first part of M02: no gap to M01's parts
        "M02-00003": 30,
    }
