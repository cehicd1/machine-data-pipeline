from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from machine_pipeline.bronze import RAW_SCHEMA, transform_bronze

HEADER = "part_id,machine_id,product_code,produced_at,cycle_time_s,is_scrap\n"


def test_bronze_keeps_raw_strings_and_adds_metadata(spark: SparkSession, tmp_path: Path) -> None:
    csv = tmp_path / "production.csv"
    csv.write_text(
        HEADER
        + "M01-1,M01,P-100,2026-01-05 06:01:00,45.0,false\n"
        + "M01-2,M01,P-100,2026-01-05 06:02:00,n/a,maybe\n"
    )

    rows = {r.part_id: r for r in transform_bronze(spark, tmp_path).collect()}

    assert len(rows) == 2
    assert rows["M01-2"].cycle_time_s == "n/a"  # malformed value survives for silver
    assert rows["M01-2"].is_scrap == "maybe"
    assert rows["M01-1"].source_file.endswith("production.csv")
    assert rows["M01-1"].ingested_at is not None
    assert all(isinstance(f.dataType, StringType) for f in RAW_SCHEMA.fields)


def test_bronze_rejects_unexpected_header(spark: SparkSession, tmp_path: Path) -> None:
    (tmp_path / "bad.csv").write_text(
        "part_id,machine,product_code,produced_at,cycle_time_s,is_scrap\n"
        "M01-1,M01,P-100,2026-01-05 06:01:00,45.0,false\n"
    )

    with pytest.raises(Exception, match="(?i)header"):
        transform_bronze(spark, tmp_path).collect()
