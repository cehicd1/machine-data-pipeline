"""Entry point: run the medallion pipeline locally (``make run``)."""

from __future__ import annotations

from machine_pipeline.bronze import run_bronze
from machine_pipeline.config import PATHS
from machine_pipeline.spark import get_spark


def main() -> None:
    spark = get_spark()
    bronze = run_bronze(spark, PATHS.raw, PATHS.bronze)
    print(f"bronze: {bronze.count():,} rows -> {PATHS.bronze}")


if __name__ == "__main__":
    main()
