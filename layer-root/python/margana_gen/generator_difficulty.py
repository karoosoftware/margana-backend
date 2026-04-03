from __future__ import annotations

from typing import Any


DIFFICULTY_BANDS = {
    "easy": {"min_score": 160, "max_score": 179},
    "medium": {"min_score": 180, "max_score": 199},
    "hard": {"min_score": 200, "max_score": None},
}


def band_for_total_score(total_score: int) -> str | None:
    try:
        score = int(total_score)
    except Exception:
        return None

    for band_name, band in DIFFICULTY_BANDS.items():
        min_score = _int_or_none(band.get("min_score"))
        max_score = _int_or_none(band.get("max_score"))
        if min_score is not None and score < min_score:
            continue
        if max_score is not None and score > max_score:
            continue
        return band_name
    return None


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None
