"""Synthetic CNC production data generator.

Simulates one row per produced part for a fleet of machines running three
shifts on scheduled days (see WORKING_SHIFTS). Realistic patterns: planned
breaks, random stoppages, product changeovers, product-dependent cycle times
and ~2% scrap. Data-quality issues
(exact duplicates and malformed rows) are injected on purpose so the silver
layer has something to clean.

Pure standard library: no Spark needed to generate the input.
"""

from __future__ import annotations

import argparse
import csv
import random
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from machine_pipeline.config import PATHS, RAW_COLUMNS, SHIFTS, GeneratorConfig, Product
from machine_pipeline.shifts import is_scheduled, shift_at, shift_window

Row = dict[str, str]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def planned_break_end(ts: datetime) -> datetime | None:
    """Return the end of the planned break containing ``ts``, or None."""
    for shift in SHIFTS:
        start = datetime.combine(ts.date(), shift.break_start)
        end = start + timedelta(minutes=shift.break_minutes)
        if start <= ts < end:
            return end
    return None


def simulate_machine(machine_id: str, cfg: GeneratorConfig, rng: random.Random) -> Iterator[Row]:
    """Yield clean part rows for one machine over the configured period."""
    t = datetime.combine(cfg.start_date, SHIFTS[0].start)
    end = t + timedelta(days=cfg.n_days)
    speed = rng.uniform(0.9, 1.15)  # each machine is a bit faster or slower
    stop_rate_per_s = cfg.stoppages_per_day / 86_400

    product: Product = rng.choice(cfg.products)
    run_end = t + timedelta(hours=rng.uniform(cfg.run_min_hours, cfg.run_max_hours))
    next_stop = t + timedelta(seconds=rng.expovariate(stop_rate_per_s))
    seq = 0

    while t < end:
        shift_date, shift = shift_at(t)
        if not is_scheduled(shift_date, shift):
            # Machine is idle until the next shift; restart timers from there.
            t = shift_window(shift_date, shift)[1]
            next_stop = max(next_stop, t + timedelta(seconds=rng.expovariate(stop_rate_per_s)))
            run_end = max(run_end, t)
            continue
        if (break_end := planned_break_end(t)) is not None:
            t = break_end
            continue
        if t >= next_stop:
            t += timedelta(minutes=rng.uniform(cfg.stoppage_min_minutes, cfg.stoppage_max_minutes))
            next_stop = t + timedelta(seconds=rng.expovariate(stop_rate_per_s))
            continue
        if t >= run_end:
            product = rng.choice([p for p in cfg.products if p != product])
            t += timedelta(minutes=cfg.changeover_minutes)
            run_end = t + timedelta(hours=rng.uniform(cfg.run_min_hours, cfg.run_max_hours))
            continue

        noise = max(0.7, rng.gauss(1.0, cfg.cycle_noise_sd))
        cycle_s = round(product.base_cycle_s * speed * noise, 1)
        t += timedelta(seconds=cycle_s)
        if t >= end:
            break  # cycle would finish after the simulated period
        if not is_scheduled(*shift_at(t)):
            continue  # cycle would finish in an unscheduled shift: no part
        seq += 1
        yield {
            "part_id": f"{machine_id}-{seq:07d}",
            "machine_id": machine_id,
            "product_code": product.code,
            "produced_at": t.strftime(TS_FORMAT),
            "cycle_time_s": f"{cycle_s:.1f}",
            "is_scrap": "true" if rng.random() < cfg.scrap_rate else "false",
        }


def inject_duplicates(rows: list[Row], rate: float, rng: random.Random) -> list[Row]:
    """Return rows plus exact copies of a random ``rate`` fraction of them."""
    dupes = [dict(r) for r in rng.sample(rows, k=int(len(rows) * rate))]
    return rows + dupes


def malformed_row(base: Row, kind: int) -> Row:
    """Corrupt one field of ``base`` in a way the silver layer must catch."""
    row = dict(base)
    row["part_id"] = f"{base['part_id']}-X{kind}"
    match kind % 6:
        case 0:
            row["machine_id"] = ""
        case 1:
            row["cycle_time_s"] = "n/a"
        case 2:
            row["cycle_time_s"] = "-12.5"
        case 3:
            row["produced_at"] = "2026-02-30 25:61:00"
        case 4:
            row["is_scrap"] = "maybe"
        case 5:
            row["product_code"] = ""
    return row


def inject_malformed(rows: list[Row], n: int, rng: random.Random) -> list[Row]:
    """Return rows plus ``n`` malformed rows derived from random originals."""
    return rows + [malformed_row(base, i) for i, base in enumerate(rng.sample(rows, k=n))]


def generate(cfg: GeneratorConfig) -> list[Row]:
    """Generate the full raw dataset, ordered by time like a real event log."""
    rng = random.Random(cfg.seed)
    rows: list[Row] = []
    for i in range(1, cfg.n_machines + 1):
        machine_id = f"M{i:02d}"
        # One RNG per machine: changing one machine's simulation never shifts the others.
        rows.extend(simulate_machine(machine_id, cfg, random.Random(f"{cfg.seed}-{machine_id}")))
    rows = inject_duplicates(rows, cfg.duplicate_rate, rng)
    rows = inject_malformed(rows, cfg.n_malformed, rng)
    # Sort by timestamp string; malformed timestamps land wherever they sort.
    rows.sort(key=lambda r: (r["produced_at"], r["machine_id"]))
    return rows


def write_csv(rows: list[Row], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic CNC production data.")
    parser.add_argument("--seed", type=int, default=GeneratorConfig.seed)
    parser.add_argument("--machines", type=int, default=GeneratorConfig.n_machines)
    parser.add_argument("--days", type=int, default=GeneratorConfig.n_days)
    parser.add_argument("--out", type=Path, default=PATHS.raw / "production.csv")
    args = parser.parse_args(argv)

    cfg = replace(GeneratorConfig(), seed=args.seed, n_machines=args.machines, n_days=args.days)
    rows = generate(cfg)
    path = write_csv(rows, args.out)
    print(f"Wrote {len(rows):,} rows to {path}")


if __name__ == "__main__":
    main()
