"""Shift calendar helpers built on the config in ``config.py``."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from machine_pipeline.config import SHIFTS, WORKING_SHIFTS, Shift


def shift_at(ts: datetime) -> tuple[date, Shift]:
    """Return ``(shift_date, shift)`` for ``ts``.

    A shift that crosses midnight belongs to the date it starts on.
    """
    t = ts.time()
    for shift in SHIFTS:
        if shift.start < shift.end:
            if shift.start <= t < shift.end:
                return ts.date(), shift
        elif t >= shift.start:
            return ts.date(), shift
        elif t < shift.end:
            return ts.date() - timedelta(days=1), shift
    raise ValueError(f"No shift covers {ts}; check SHIFTS in config.py")


def shift_window(shift_date: date, shift: Shift) -> tuple[datetime, datetime]:
    """Return the ``[start, end)`` datetimes of a shift instance."""
    start = datetime.combine(shift_date, shift.start)
    end = datetime.combine(shift_date, shift.end)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def is_scheduled(shift_date: date, shift: Shift) -> bool:
    """True if production is planned for this shift instance."""
    return shift.name in WORKING_SHIFTS.get(shift_date.weekday(), frozenset())
