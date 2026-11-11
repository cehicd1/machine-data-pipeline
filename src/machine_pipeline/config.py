"""Central configuration: paths, shift calendar and generator settings.

Everything tunable lives here so pipeline code stays free of magic numbers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"


@dataclass(frozen=True)
class Paths:
    raw: Path = DATA_DIR / "raw"
    bronze: Path = DATA_DIR / "bronze"
    silver: Path = DATA_DIR / "silver"
    quarantine: Path = DATA_DIR / "quarantine"
    gold: Path = DATA_DIR / "gold"


@dataclass(frozen=True)
class Shift:
    name: str
    start: time
    end: time  # exclusive; end < start means the shift crosses midnight
    break_start: time
    break_minutes: int


# Three 8h shifts, each with one planned 30 min break.
SHIFTS: tuple[Shift, ...] = (
    Shift("early", time(6), time(14), break_start=time(10), break_minutes=30),
    Shift("late", time(14), time(22), break_start=time(18), break_minutes=30),
    Shift("night", time(22), time(6), break_start=time(2), break_minutes=30),
)


@dataclass(frozen=True)
class Product:
    code: str
    base_cycle_s: float


# Synthetic product catalogue; P-300 and P-400 are the slow, complex parts.
PRODUCTS: tuple[Product, ...] = (
    Product("P-100", 45.0),
    Product("P-200", 60.0),
    Product("P-300", 95.0),
    Product("P-400", 130.0),
)


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    n_machines: int = 10
    n_days: int = 30
    start_date: date = date(2026, 1, 5)  # a Monday
    scrap_rate: float = 0.02
    duplicate_rate: float = 0.005
    n_malformed: int = 25
    stoppages_per_day: float = 3.0
    stoppage_min_minutes: float = 5.0
    stoppage_max_minutes: float = 120.0
    run_min_hours: float = 4.0  # product run length before a changeover
    run_max_hours: float = 12.0
    changeover_minutes: float = 15.0
    cycle_noise_sd: float = 0.05  # relative std dev of cycle time
    products: tuple[Product, ...] = field(default=PRODUCTS)


PATHS = Paths()
RAW_COLUMNS: tuple[str, ...] = (
    "part_id",
    "machine_id",
    "product_code",
    "produced_at",
    "cycle_time_s",
    "is_scrap",
)
