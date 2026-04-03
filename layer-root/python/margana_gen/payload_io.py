from __future__ import annotations

import json
from pathlib import Path
from typing import Any


COMPLETED_FILENAME = "margana-completed.json"
SEMI_COMPLETED_FILENAME = "margana-semi-completed.json"


def write_payload_pair(
    directory: Path,
    *,
    completed_payload: dict[str, Any],
    semi_completed_payload: dict[str, Any],
) -> tuple[Path, Path]:
    completed_path = directory / COMPLETED_FILENAME
    semi_completed_path = directory / SEMI_COMPLETED_FILENAME

    with open(completed_path, "w", encoding="utf-8") as completed_file:
        json.dump(completed_payload, completed_file, indent=2)

    with open(semi_completed_path, "w", encoding="utf-8") as semi_completed_file:
        json.dump(semi_completed_payload, semi_completed_file, indent=2)

    return completed_path, semi_completed_path
