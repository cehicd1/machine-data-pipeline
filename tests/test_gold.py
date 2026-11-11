from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from machine_pipeline.gold import (
    DAY_KEYS,
    SHIFT_KEYS,
    assign_shift,
    compute_kpis,
    downtime_intervals,
    downtime_overlaps,
    production_per_grain,
    productive_segments,
    segments_df,
    shift_production_sql,
    transform_gold,
)
from machine_pipeline.shifts import shift_at

MON = date(2026, 1, 5)
FRI = date(2026, 1, 9)
MakeSilver = Callable[..., DataFrame]


# --- helpers ----------------------------------------------------------------


def downtime_min(
    spark: SparkSession, silver: DataFrame, start: date, end: date, threshold_s: int = 300
) -> dict[tuple[str, date, str], float]:
    """Downtime minutes per (machine, shift_date, shift) from real gold functions."""
    machines = sorted(r.machine_id for r in silver.select("machine_id").distinct().collect())
    overlaps = downtime_overlaps(
        downtime_intervals(silver, threshold_s), segments_df(spark, start, end, machines)
    )
    agg = overlaps.groupBy(*SHIFT_KEYS).agg((F.sum("downtime_s") / 60).alias("m"))
    return {(r.machine_id, r.shift_date, r.shift_name): r.m for r in agg.collect()}


def segments(spark: SparkSession, rows: list[tuple[str, date, str, int]]) -> DataFrame:
    """Planned segments from (machine, shift_date, shift, minutes)."""
    return spark.createDataFrame(
        [(m, d, s, 0, minutes * 60) for m, d, s, minutes in rows],
        "machine_id string, shift_date date, shift_name string, seg_start_s long, seg_end_s long",
    )


def overlaps(spark: SparkSession, rows: list[tuple[str, date, str, float]]) -> DataFrame:
    """Downtime per shift from (machine, shift_date, shift, downtime_s)."""
    return spark.createDataFrame(
        rows, "machine_id string, shift_date date, shift_name string, downtime_s double"
    )


def kpi_parts(spark: SparkSession, groups: list[tuple[str, date, str, str, int, int]]) -> DataFrame:
    """Parts from (machine, shift_date, shift, product, n_parts, n_scrap)."""
    data = [
        (m, d, s, product, 100.0, i < scrap)
        for m, d, s, product, n, scrap in groups
        for i in range(n)
    ]
    return spark.createDataFrame(
        data,
        "machine_id string, shift_date date, shift_name string, product_code string, "
        "cycle_time_s double, is_scrap boolean",
    )


def by_key(df: DataFrame, keys: tuple[str, ...]) -> dict[tuple, Row]:
    return {tuple(r[k] for k in keys): r for r in df.collect()}


# --- shift calendar ---------------------------------------------------------


@pytest.mark.parametrize(
    ("ts", "shift_date", "shift_name"),
    [
        ("2026-01-05 05:59:59", date(2026, 1, 4), "night"),  # Sunday's night shift
        ("2026-01-05 06:00:00", MON, "early"),
        ("2026-01-05 13:59:59", MON, "early"),
        ("2026-01-05 14:00:00", MON, "late"),
        ("2026-01-05 21:59:59", MON, "late"),
        ("2026-01-05 22:00:00", MON, "night"),
        ("2026-01-05 23:59:59", MON, "night"),
        ("2026-01-06 00:00:00", MON, "night"),  # after midnight, still Monday's night shift
        ("2026-01-06 05:59:59", MON, "night"),
        ("2026-01-06 06:00:00", date(2026, 1, 6), "early"),
    ],
)
def test_shift_assignment_around_midnight(
    spark: SparkSession, ts: str, shift_date: date, shift_name: str
) -> None:
    df = spark.createDataFrame([(ts,)], "produced_at string").select(
        F.to_timestamp("produced_at").alias("produced_at")
    )
    row = assign_shift(df).first()

    assert (row.shift_date, row.shift_name) == (shift_date, shift_name)
    # The Spark expression and the Python helper must agree.
    py_date, py_shift = shift_at(datetime.fromisoformat(ts))
    assert (py_date, py_shift.name) == (shift_date, shift_name)


def test_productive_segments_exclude_breaks_and_weekends() -> None:
    monday = productive_segments(MON, MON)
    minutes = sum(
        (datetime.fromisoformat(e) - datetime.fromisoformat(s)).total_seconds() / 60
        for _, _, s, e in monday
    )

    assert len(monday) == 6  # 3 shifts x 2 segments around the break
    assert minutes == 3 * (480 - 30)
    assert ("night", "2026-01-05 22:00:00", "2026-01-06 02:00:00") in [
        (n, s, e) for _, n, s, e in monday
    ]
    assert productive_segments(date(2026, 1, 10), date(2026, 1, 11)) == []  # Sat, Sun


# --- downtime ---------------------------------------------------------------


def test_planned_break_is_not_downtime_but_stoppage_is(
    spark: SparkSession, make_silver: MakeSilver
) -> None:
    # Early shift break is 10:00-10:30. Each machine has one 30 min idle period.
    silver = make_silver(
        [
            ("BREAK", "2026-01-05 10:00:00", 60),  # idle 10:00-10:30: exactly the break
            ("BREAK", "2026-01-05 10:31:00", 60),
            ("STOP", "2026-01-05 08:00:00", 60),  # idle 08:00-08:30: stoppage
            ("STOP", "2026-01-05 08:31:00", 60),
            ("OVERLAP", "2026-01-05 09:50:00", 60),  # idle 09:50-10:40: 20 min outside break
            ("OVERLAP", "2026-01-05 10:41:00", 60),
        ]
    )

    dt = downtime_min(spark, silver, MON, MON)

    assert dt.get(("BREAK", MON, "early"), 0.0) == 0.0
    assert dt[("STOP", MON, "early")] == pytest.approx(30.0)
    assert dt[("OVERLAP", MON, "early")] == pytest.approx(20.0)


def test_downtime_threshold(spark: SparkSession, make_silver: MakeSilver) -> None:
    silver = make_silver(
        [
            ("AT", "2026-01-05 08:00:00", 60),
            ("AT", "2026-01-05 08:05:00", 60),  # gap 300 s: not above threshold
            ("ABOVE", "2026-01-05 08:00:00", 60),
            ("ABOVE", "2026-01-05 08:05:01", 60),  # gap 301 s: downtime
        ]
    )

    dt = downtime_min(spark, silver, MON, MON, threshold_s=300)
    assert set(dt) == {("ABOVE", MON, "early")}
    # Idle runs to the start of the next cycle: 301 s gap - 60 s cycle.
    assert dt[("ABOVE", MON, "early")] == pytest.approx(241 / 60)

    assert downtime_min(spark, silver, MON, MON, threshold_s=600) == {}


def test_gap_across_shift_boundary_is_split(spark: SparkSession, make_silver: MakeSilver) -> None:
    silver = make_silver([("M01", "2026-01-05 13:50:00", 60), ("M01", "2026-01-05 14:21:00", 60)])

    dt = downtime_min(spark, silver, MON, MON)

    assert dt == {
        ("M01", MON, "early"): pytest.approx(10.0),
        ("M01", MON, "late"): pytest.approx(20.0),
    }


def test_weekend_gap_only_counts_scheduled_time(
    spark: SparkSession, make_silver: MakeSilver
) -> None:
    # Last part in Friday's night shift, next part Monday morning.
    silver = make_silver([("M01", "2026-01-10 05:50:00", 60), ("M01", "2026-01-12 06:11:00", 60)])

    dt = downtime_min(spark, silver, FRI, date(2026, 1, 12))

    assert dt == {
        ("M01", FRI, "night"): pytest.approx(10.0),
        ("M01", date(2026, 1, 12), "early"): pytest.approx(10.0),
    }


# --- OEE --------------------------------------------------------------------


def test_oee_math_on_hand_made_shift(spark: SparkSession) -> None:
    # planned 240 + 210 = 450 min, downtime 45 min -> run 405 min, availability 0.9
    # 243 parts x 80 s ideal (P-300) = 324 min -> performance 324 / 405 = 0.8
    # 9 scrap -> quality 234 / 243; OEE = 0.9 * 0.8 * 234 / 243
    segs = segments(spark, [("M01", MON, "early", 240), ("M01", MON, "early", 210)])
    down = overlaps(spark, [("M01", MON, "early", 45 * 60.0)])
    parts = kpi_parts(spark, [("M01", MON, "early", "P-300", 243, 9)])

    [row] = compute_kpis(parts, segs, down, SHIFT_KEYS).collect()

    assert row.is_scheduled is True
    assert row.planned_min == 450
    assert row.downtime_min == 45
    assert row.run_time_min == 405
    assert row.parts_produced == 243
    assert row.scrap_count == 9
    assert row.good_count == 234
    assert row.ideal_time_min == 324
    assert row.median_cycle_time_s == 100.0
    assert row.availability == pytest.approx(0.9, abs=1e-4)
    assert row.performance == pytest.approx(0.8, abs=1e-4)
    assert row.quality == pytest.approx(234 / 243, abs=1e-4)
    assert row.oee == pytest.approx(0.9 * 0.8 * 234 / 243, abs=1e-4)


def test_day_oee_is_computed_from_sums_not_averaged(spark: SparkSession) -> None:
    # early: 450 planned, no downtime, 270 good P-300 parts -> A 1.0, P 0.8, Q 1.0, OEE 0.8
    # late (short, bad): 90 planned, 45 down, 27 parts / 9 scrap -> A 0.5, P 0.8, Q 2/3
    segs = segments(spark, [("M01", MON, "early", 450), ("M01", MON, "late", 90)])
    down = overlaps(spark, [("M01", MON, "late", 45 * 60.0)])
    parts = kpi_parts(
        spark, [("M01", MON, "early", "P-300", 270, 0), ("M01", MON, "late", "P-300", 27, 9)]
    )

    shifts = by_key(compute_kpis(parts, segs, down, SHIFT_KEYS), ("shift_name",))
    [day] = compute_kpis(parts, segs, down, DAY_KEYS).collect()

    assert shifts[("early",)].oee == pytest.approx(0.8, abs=1e-4)
    assert shifts[("late",)].oee == pytest.approx(0.5 * 0.8 * 2 / 3, abs=1e-4)
    # Sums: run 495 / planned 540, ideal 396 / run 495, good 288 / parts 297.
    assert day.availability == pytest.approx(495 / 540, abs=1e-4)
    assert day.performance == pytest.approx(0.8, abs=1e-4)
    assert day.quality == pytest.approx(288 / 297, abs=1e-4)
    assert day.oee == pytest.approx(396 * 288 / (540 * 297), abs=1e-4)  # 0.7111
    mean_of_shift_oee = (shifts[("early",)].oee + shifts[("late",)].oee) / 2  # 0.5333
    assert abs(day.oee - mean_of_shift_oee) > 0.1
    # The additive shift columns let consumers roll up to the same day OEE.
    rows = list(shifts.values())
    planned, ideal = sum(r.planned_min for r in rows), sum(r.ideal_time_min for r in rows)
    good, total = sum(r.good_count for r in rows), sum(r.parts_produced for r in rows)
    assert ideal / planned * good / total == pytest.approx(day.oee, abs=1e-4)


def test_parts_in_unscheduled_shift_have_no_time_based_kpis(spark: SparkSession) -> None:
    sat = date(2026, 1, 10)
    segs = segments(spark, [("M01", MON, "early", 450)])
    down = overlaps(spark, [])
    parts = kpi_parts(
        spark, [("M01", MON, "early", "P-100", 10, 0), ("M01", sat, "early", "P-100", 5, 1)]
    )

    rows = by_key(compute_kpis(parts, segs, down, SHIFT_KEYS), ("shift_date",))

    saturday = rows[(sat,)]
    assert saturday.is_scheduled is False
    assert saturday.parts_produced == 5
    assert saturday.planned_min is None
    assert saturday.availability is None
    assert saturday.oee is None
    assert saturday.quality == pytest.approx(0.8)
    assert rows[(MON,)].is_scheduled is True


def test_scheduled_shift_without_parts_counts_as_downtime(
    spark: SparkSession, make_silver: MakeSilver
) -> None:
    # M01 runs Monday's early shift only (one part per minute, idle in the break),
    # then stops. Late and night shifts are scheduled but produce nothing.
    start = datetime(2026, 1, 5, 6, 1)
    minutes = [*range(0, 240), *range(270, 479)]  # 06:01-10:00 and 10:31-13:59
    silver = make_silver(
        [("M01", (start + timedelta(minutes=m)).isoformat(sep=" "), 60) for m in minutes]
    )

    by_shift, by_day = transform_gold(silver)
    rows = by_key(by_shift, ("shift_name",))

    assert rows[("early",)].parts_produced == 449
    assert rows[("early",)].downtime_min == pytest.approx(1.0)  # 13:59-14:00 after last part
    for idle in ("late", "night"):
        assert rows[(idle,)].is_scheduled is True
        assert rows[(idle,)].parts_produced == 0
        assert rows[(idle,)].downtime_min == pytest.approx(450.0)
        assert rows[(idle,)].availability == 0.0
        assert rows[(idle,)].oee == 0.0
        assert rows[(idle,)].quality is None
    [day] = by_day.collect()
    assert day.planned_min == 1350
    assert day.availability == pytest.approx(449 / 1350, abs=1e-4)


# --- SQL --------------------------------------------------------------------


def test_sql_aggregation_matches_dataframe(make_silver: MakeSilver) -> None:
    parts = assign_shift(
        make_silver(
            [
                ("M01", "2026-01-05 06:01:00", 40, False),
                ("M01", "2026-01-05 06:02:00", 50, True),
                ("M01", "2026-01-05 06:03:00", 70, False),
                ("M01", "2026-01-05 23:30:00", 45, False),
                ("M02", "2026-01-06 00:30:00", 60, True),
            ]
        )
    )

    expected = production_per_grain(parts, SHIFT_KEYS).drop("ideal_time_s")
    actual = shift_production_sql(parts)

    assert sorted(actual.collect()) == sorted(expected.collect())
    early = actual.filter("machine_id = 'M01' AND shift_name = 'early'").first()
    assert (early.parts_produced, early.scrap_count, early.median_cycle_time_s) == (3, 1, 50.0)
