from __future__ import annotations

from typing import Any


def mask_grid_rows(rows: list[str], column_index: int, diag_direction: str) -> list[str]:
    masked: list[str] = []
    for r in range(5):
        row_chars = list(rows[r])
        for c in range(5):
            on_col = c == column_index
            on_diag = (diag_direction == "main" and c == r) or (diag_direction == "anti" and c == 4 - r)
            row_chars[c] = row_chars[c] if (on_col or on_diag) else "*"
        masked.append("".join(row_chars))
    return masked


def build_semi_completed_payload(
    *,
    date: str,
    puzzle_id: str,
    layout_id: str,
    vertical_target_word: str,
    column_index: int,
    diagonal_direction: str,
    diagonal_target_word: str,
    rows: list[str],
    longest_anagram_count: int,
    longest_anagram_shuffled: str | None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "date": date,
        "id": puzzle_id,
        "chain_id": layout_id,
        "word_length": 5,
        "vertical_target_word": vertical_target_word,
        "column_index": column_index,
        "diagonal_direction": diagonal_direction,
        "diagonal_target_word": diagonal_target_word,
        "grid_rows": mask_grid_rows(rows, column_index, diagonal_direction),
        "longest_anagram_count": int(longest_anagram_count),
        "longestAnagramShuffled": longest_anagram_shuffled,
    }
    if extra_fields:
        payload.update(extra_fields)
    return payload
