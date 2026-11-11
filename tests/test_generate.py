from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime

from machine_pipeline.config import GeneratorConfig
from machine_pipeline.generate import generate
from machine_pipeline.shifts import is_scheduled, shift_at

# Friday to Sunday: one scheduled day followed by a weekend.
SMALL = replace(GeneratorConfig(), n_machines=2, n_days=3, start_date=date(2026, 1, 9))


def clean(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [r for r in rows if "-X" not in r["part_id"]]


def test_generator_is_reproducible() -> None:
    assert generate(SMALL) == generate(SMALL)
    assert generate(SMALL) != generate(replace(SMALL, seed=7))


def test_parts_only_in_scheduled_shifts() -> None:
    for row in clean(generate(SMALL)):
        assert is_scheduled(*shift_at(datetime.fromisoformat(row["produced_at"])))


def test_injected_data_quality_issues() -> None:
    rows = generate(SMALL)
    malformed = [r for r in rows if "-X" in r["part_id"]]
    good = clean(rows)
    unique = {r["part_id"] for r in good}

    assert len(malformed) == SMALL.n_malformed
    assert len(good) - len(unique) == int(len(unique) * SMALL.duplicate_rate)
    scrap_rate = sum(r["is_scrap"] == "true" for r in good) / len(good)
    assert 0.01 < scrap_rate < 0.03
