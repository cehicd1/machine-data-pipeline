# Databricks notebook source
# MAGIC %md
# MAGIC # machine-data-pipeline on Databricks
# MAGIC
# MAGIC Runs the same bronze -> silver -> gold transformations as `make run`, but stores
# MAGIC each layer as a Unity Catalog **Delta table** instead of a Parquet folder.
# MAGIC Only the storage calls differ; all transformation code comes from `src/machine_pipeline`.
# MAGIC
# MAGIC All data is synthetic and generated in this notebook. See `databricks/README.md` for setup.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "machine_pipeline")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PREFIX = f"{CATALOG}.{SCHEMA}"
RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# COMMAND ----------

import os
import sys

# In a Git folder the notebook's working directory is databricks/; the package is in ../src.
sys.path.insert(0, os.path.abspath("../src"))

from machine_pipeline import generate
from machine_pipeline.bronze import transform_bronze
from machine_pipeline.gold import transform_gold
from machine_pipeline.silver import transform_silver
from machine_pipeline.storage import read_delta_table, write_delta_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema, volume and synthetic raw data

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PREFIX}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {PREFIX}.raw")

# Same seeded generator as `make data`; the CSV lands in a Unity Catalog volume.
generate.main(["--out", f"{RAW_DIR}/production.csv"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze -> Silver -> Gold

# COMMAND ----------

write_delta_table(transform_bronze(spark, RAW_DIR), f"{PREFIX}.bronze_production")
bronze = read_delta_table(spark, f"{PREFIX}.bronze_production")

silver, quarantine = transform_silver(bronze)
write_delta_table(silver, f"{PREFIX}.silver_production")
write_delta_table(quarantine, f"{PREFIX}.quarantine_production")
silver = read_delta_table(spark, f"{PREFIX}.silver_production")

by_shift, by_day = transform_gold(silver)
write_delta_table(by_shift, f"{PREFIX}.gold_machine_shift_kpis")
write_delta_table(by_day, f"{PREFIX}.gold_machine_daily_kpis")

for table in [
    "bronze_production",
    "silver_production",
    "quarantine_production",
    "gold_machine_shift_kpis",
    "gold_machine_daily_kpis",
]:
    print(f"{PREFIX}.{table}: {spark.table(f'{PREFIX}.{table}').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

display(spark.table(f"{PREFIX}.gold_machine_shift_kpis").limit(20))

# COMMAND ----------

display(
    spark.table(f"{PREFIX}.quarantine_production")
    .groupBy("quarantine_reason")
    .count()
    .orderBy("quarantine_reason")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Fleet OEE per day in SQL. It is recomputed from summed times and counts, not by
# MAGIC averaging machine OEE, so long and short shifts are weighted correctly.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            shift_date,
            SUM(parts_produced)                                      AS parts_produced,
            ROUND(SUM(run_time_min) / SUM(planned_min), 4)           AS availability,
            ROUND(SUM(ideal_time_min) / SUM(run_time_min), 4)        AS performance,
            ROUND(SUM(good_count) / SUM(parts_produced), 4)          AS quality,
            ROUND(SUM(ideal_time_min) / SUM(planned_min)
                  * SUM(good_count) / SUM(parts_produced), 4)        AS oee
        FROM {PREFIX}.gold_machine_daily_kpis
        WHERE is_scheduled
        GROUP BY shift_date
        ORDER BY shift_date
        """
    )
)
