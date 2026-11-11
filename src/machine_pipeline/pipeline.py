"""Entry point: run the medallion pipeline locally (``make run``)."""

from __future__ import annotations

from machine_pipeline.bronze import transform_bronze
from machine_pipeline.config import PATHS
from machine_pipeline.silver import transform_silver
from machine_pipeline.spark import get_spark
from machine_pipeline.storage import read_table, write_table


def main() -> None:
    spark = get_spark()

    write_table(transform_bronze(spark, PATHS.raw), PATHS.bronze)
    bronze = read_table(spark, PATHS.bronze)
    print(f"bronze:     {bronze.count():>9,} rows -> {PATHS.bronze}")

    silver, quarantine = transform_silver(bronze)
    write_table(silver, PATHS.silver)
    write_table(quarantine, PATHS.quarantine)
    print(f"silver:     {read_table(spark, PATHS.silver).count():>9,} rows -> {PATHS.silver}")
    print(
        f"quarantine: {read_table(spark, PATHS.quarantine).count():>9,} rows -> {PATHS.quarantine}"
    )


if __name__ == "__main__":
    main()
