"""Shared fixtures: one local SparkSession and small DataFrame builders."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from machine_pipeline.silver import add_gap
from machine_pipeline.spark import get_spark

PartRow = tuple  # (machine_id, produced_at, cycle_time_s[, is_scrap[, product_code]])


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = get_spark("machine-pipeline-tests")
    yield session
    session.stop()


@pytest.fixture
def make_silver(spark: SparkSession) -> Callable[[Sequence[PartRow]], DataFrame]:
    """Build silver-shaped parts (with ``gap_s``) from compact tuples.

    Timestamps are given as strings and parsed by Spark, so tests do not
    depend on the time zone of the machine running them.
    """

    def _make(rows: Sequence[PartRow]) -> DataFrame:
        data = []
        for i, row in enumerate(rows):
            machine_id, produced_at, cycle_time_s = row[:3]
            is_scrap = row[3] if len(row) > 3 else False
            product_code = row[4] if len(row) > 4 else "P-100"
            data.append(
                (
                    f"{machine_id}-{i:05d}",
                    machine_id,
                    product_code,
                    produced_at,
                    float(cycle_time_s),
                    is_scrap,
                )
            )
        df = spark.createDataFrame(
            data,
            "part_id string, machine_id string, product_code string, produced_at string, "
            "cycle_time_s double, is_scrap boolean",
        )
        return add_gap(df.withColumn("produced_at", F.to_timestamp("produced_at")))

    return _make
