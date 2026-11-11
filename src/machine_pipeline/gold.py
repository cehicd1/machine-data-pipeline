"""Gold layer: production KPIs and OEE per machine and shift, and per machine and day.

OEE follows the standard definition:

* planned_min   = scheduled shift time minus planned breaks. Unscheduled shifts
                  (see ``WORKING_SHIFTS``) have no planned time at all.
* downtime_min  = idle time inside planned time, from gaps longer than
                  ``DOWNTIME_THRESHOLD_S`` (stoppages, changeovers). Idle time
                  during breaks or unscheduled shifts is not downtime.
* availability  = run_time_min / planned_min, run_time = planned - downtime
* performance   = ideal production time / run time
* quality       = good parts / all parts
* oee           = availability * performance * quality

Break and shift windows are derived from the shift calendar in ``config.py``
because the raw data has no break or schedule markers.

Known simplification: the machine list comes from the data (no master data).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from importlib.resources import files

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from machine_pipeline.config import DOWNTIME_THRESHOLD_S, PRODUCTS, SHIFTS, Shift
from machine_pipeline.shifts import is_scheduled, shift_window

SHIFT_KEYS: tuple[str, ...] = ("machine_id", "shift_date", "shift_name")
DAY_KEYS: tuple[str, ...] = ("machine_id", "shift_date")
_TS = "%Y-%m-%d %H:%M:%S"


# --- shift assignment -------------------------------------------------------


def _seconds(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def assign_shift(df: DataFrame, ts_col: str = "produced_at") -> DataFrame:
    """Add ``shift_date`` and ``shift_name`` from the shift calendar.

    A shift crossing midnight belongs to the day it starts, so 00:30 on
    Tuesday is Monday's night shift.
    """
    ts = F.col(ts_col)
    tod = F.hour(ts) * 3600 + F.minute(ts) * 60 + F.second(ts)
    day = F.to_date(ts)

    cases: list[tuple[Column, str, Column]] = []
    for shift in SHIFTS:
        start, end = _seconds(shift.start), _seconds(shift.end)
        if start < end:
            cases.append(((tod >= start) & (tod < end), shift.name, day))
        else:
            cases.append((tod >= start, shift.name, day))
            cases.append((tod < end, shift.name, F.date_sub(day, 1)))

    first, *rest = cases
    name, shift_date = F.when(first[0], first[1]), F.when(first[0], first[2])
    for cond, shift_name, d in rest:
        name, shift_date = name.when(cond, shift_name), shift_date.when(cond, d)
    return df.withColumns({"shift_date": shift_date, "shift_name": name})


# --- planned production time ------------------------------------------------


def _break_window(shift_date: date, shift: Shift) -> tuple[datetime, datetime]:
    start, _ = shift_window(shift_date, shift)
    brk = datetime.combine(start.date(), shift.break_start)
    if brk < start:  # break after midnight in a night shift
        brk += timedelta(days=1)
    return brk, brk + timedelta(minutes=shift.break_minutes)


def productive_segments(start: date, end: date) -> list[tuple[date, str, str, str]]:
    """Planned production windows ``(shift_date, shift_name, seg_start, seg_end)``.

    One row per scheduled shift and side of its break, for shift dates
    ``start..end`` inclusive. Breaks and unscheduled shifts are left out.
    """
    rows: list[tuple[date, str, str, str]] = []
    d = start
    while d <= end:
        for shift in SHIFTS:
            if not is_scheduled(d, shift):
                continue
            s, e = shift_window(d, shift)
            bs, be = _break_window(d, shift)
            for seg_start, seg_end in ((s, bs), (be, e)):
                rows.append((d, shift.name, seg_start.strftime(_TS), seg_end.strftime(_TS)))
        d += timedelta(days=1)
    return rows


def segments_df(spark: SparkSession, start: date, end: date, machine_ids: list[str]) -> DataFrame:
    """Productive segments for every machine, timestamps as unix seconds.

    Timestamps are passed as strings and parsed by Spark, so they use the
    session time zone like silver (no Python local-time conversion).
    """
    segs = spark.createDataFrame(
        productive_segments(start, end),
        "shift_date date, shift_name string, seg_start string, seg_end string",
    ).select(
        "shift_date",
        "shift_name",
        F.unix_seconds(F.to_timestamp("seg_start")).alias("seg_start_s"),
        F.unix_seconds(F.to_timestamp("seg_end")).alias("seg_end_s"),
    )
    machines = spark.createDataFrame([(m,) for m in machine_ids], "machine_id string")
    return machines.crossJoin(segs)


# --- downtime ---------------------------------------------------------------


def downtime_intervals(silver: DataFrame, threshold_s: int = DOWNTIME_THRESHOLD_S) -> DataFrame:
    """Idle intervals for gaps above the threshold.

    The interval runs from the previous part to the start of the current
    part's cycle, so the current part's own cycle time is not downtime.
    """
    end_s = F.unix_seconds("produced_at")
    return silver.filter(F.col("gap_s") > threshold_s).select(
        "machine_id",
        (end_s - F.col("gap_s")).cast("double").alias("down_start_s"),
        (end_s - F.col("cycle_time_s")).alias("down_end_s"),
    )


def boundary_intervals(silver: DataFrame, segments: DataFrame) -> DataFrame:
    """Idle intervals at the edges of the reporting period, per machine.

    From period start to the first part's cycle start, and from the last part
    to period end. Without these, a machine that stops producing before period
    end would show 100% availability for its remaining scheduled shifts.
    """
    period = segments.agg(
        F.min("seg_start_s").alias("period_start_s"), F.max("seg_end_s").alias("period_end_s")
    )
    edges = silver.groupBy("machine_id").agg(
        F.min(F.unix_seconds("produced_at") - F.col("cycle_time_s")).alias("first_start_s"),
        F.max(F.unix_seconds("produced_at")).alias("last_end_s"),
    )
    edges = edges.crossJoin(period)
    head = edges.select(
        "machine_id",
        F.col("period_start_s").cast("double").alias("down_start_s"),
        F.col("first_start_s").alias("down_end_s"),
    )
    tail = edges.select(
        "machine_id",
        F.col("last_end_s").cast("double").alias("down_start_s"),
        F.col("period_end_s").cast("double").alias("down_end_s"),
    )
    return head.unionByName(tail)


def downtime_overlaps(intervals: DataFrame, segments: DataFrame) -> DataFrame:
    """Clip idle intervals to productive segments: ``downtime_s`` per segment.

    A gap across a break or shift boundary is split, and only the part inside
    planned time is kept.
    """
    overlap = F.least("down_end_s", "seg_end_s") - F.greatest("down_start_s", "seg_start_s")
    return (
        intervals.join(segments, "machine_id")
        .withColumn("downtime_s", overlap)
        .filter(F.col("downtime_s") > 0)
        .select("machine_id", "shift_date", "shift_name", "downtime_s")
    )


# --- aggregation ------------------------------------------------------------


def production_per_grain(parts: DataFrame, keys: tuple[str, ...]) -> DataFrame:
    """Parts, scrap, median cycle time and ideal production time per grain."""
    ideal_by_product = F.create_map(
        *[x for p in PRODUCTS for x in (F.lit(p.code), F.lit(p.ideal_cycle_s))]
    )
    return parts.groupBy(*keys).agg(
        F.count("*").alias("parts_produced"),
        F.sum(F.col("is_scrap").cast("int")).alias("scrap_count"),
        F.median("cycle_time_s").alias("median_cycle_time_s"),
        F.sum(ideal_by_product[F.col("product_code")]).alias("ideal_time_s"),
    )


def compute_kpis(
    parts: DataFrame, segments: DataFrame, overlaps: DataFrame, keys: tuple[str, ...]
) -> DataFrame:
    """Join planned time, production and downtime at ``keys`` grain and add OEE.

    Scheduled shifts without parts stay in the result (availability 0).
    Parts in unscheduled shifts are kept, but their time-based KPIs are null.
    The additive columns (planned, run and ideal time, good and total parts)
    are part of the output so higher levels can recompute OEE from sums.
    """
    planned = segments.groupBy(*keys).agg(
        (F.sum(F.col("seg_end_s") - F.col("seg_start_s")) / 60).alias("planned_min")
    )
    downtime = overlaps.groupBy(*keys).agg((F.sum("downtime_s") / 60).alias("downtime_min"))
    df = planned.join(production_per_grain(parts, keys), list(keys), "full").join(
        downtime, list(keys), "left"
    )

    scheduled = F.col("planned_min").isNotNull()
    parts_n = F.coalesce("parts_produced", F.lit(0))
    scrap_n = F.coalesce("scrap_count", F.lit(0))
    downtime_min = F.when(scheduled, F.coalesce("downtime_min", F.lit(0.0)))
    run_min = F.col("planned_min") - F.col("downtime_min")
    availability = F.col("run_time_min") / F.col("planned_min")
    performance = F.when(
        F.col("run_time_min") > 0,
        F.coalesce("ideal_time_s", F.lit(0.0)) / 60 / F.col("run_time_min"),
    )
    quality = F.when(parts_n > 0, (parts_n - scrap_n) / parts_n)
    oee = F.when(
        scheduled,
        F.coalesce(F.col("availability") * F.col("performance") * F.col("quality"), F.lit(0.0)),
    )

    return (
        df.withColumns(
            {
                "is_scheduled": scheduled,
                "parts_produced": parts_n,
                "scrap_count": scrap_n,
                "downtime_min": downtime_min,
                "good_count": parts_n - scrap_n,
                "ideal_time_min": F.coalesce("ideal_time_s", F.lit(0.0)) / 60,
            }
        )
        .withColumn("run_time_min", run_min)
        .withColumns({"availability": availability, "performance": performance, "quality": quality})
        .withColumn("oee", oee)
        .select(
            *keys,
            "is_scheduled",
            "planned_min",
            "parts_produced",
            "scrap_count",
            "good_count",
            F.round("median_cycle_time_s", 1).alias("median_cycle_time_s"),
            *[F.round(c, 2).alias(c) for c in ("downtime_min", "run_time_min", "ideal_time_min")],
            *[F.round(c, 4).alias(c) for c in ("availability", "performance", "quality", "oee")],
        )
        .orderBy(*keys)
    )


def shift_production_sql(parts: DataFrame) -> DataFrame:
    """Spark SQL version of ``production_per_grain(parts, SHIFT_KEYS)`` (without ideal time)."""
    # A temp view instead of DataFrame kwargs keeps this portable to Spark Connect.
    parts.createOrReplaceTempView("shift_parts")
    query = files("machine_pipeline").joinpath("sql/shift_production.sql").read_text()
    return parts.sparkSession.sql(query)


def transform_gold(
    silver: DataFrame, threshold_s: int = DOWNTIME_THRESHOLD_S
) -> tuple[DataFrame, DataFrame]:
    """Return ``(machine_shift_kpis, machine_daily_kpis)``."""
    parts = assign_shift(silver)
    bounds = parts.agg(F.min("shift_date"), F.max("shift_date")).first()
    machine_ids = sorted(r.machine_id for r in parts.select("machine_id").distinct().collect())
    segments = segments_df(silver.sparkSession, bounds[0], bounds[1], machine_ids)
    intervals = downtime_intervals(silver, threshold_s).unionByName(
        boundary_intervals(silver, segments)
    )
    overlaps = downtime_overlaps(intervals, segments)
    by_shift = compute_kpis(parts, segments, overlaps, SHIFT_KEYS)
    by_day = compute_kpis(parts, segments, overlaps, DAY_KEYS)
    return by_shift, by_day
