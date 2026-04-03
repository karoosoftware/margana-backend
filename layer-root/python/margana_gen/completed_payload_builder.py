from __future__ import annotations

from typing import Any


DEFAULT_USER = {
    "sub": "margana",
    "username": "margana",
    "email": None,
    "issuer": "generator",
    "identity_provider": None,
}


def build_completed_payload(
    *,
    saved_at: str,
    date: str,
    vertical_target_word: str,
    column_index: int,
    diagonal_direction: str,
    diagonal_target_word: str,
    rows: list[str],
    longest_anagram: str | None,
    valid_words: dict[str, Any],
    valid_words_metadata: list[dict[str, Any]],
    total_score: int,
    user: dict[str, Any] | None = None,
    meta_extra: dict[str, Any] | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "saved_at": saved_at,
        "user": dict(DEFAULT_USER if user is None else user),
        "diagonal_direction": diagonal_direction,
        "diagonal_target_word": diagonal_target_word,
        "meta": {
            "date": date,
            "rows": 5,
            "cols": 5,
            "wordLength": 5,
            "columnIndex": column_index,
            "diagonalDirection": diagonal_direction,
            "verticalTargetWord": vertical_target_word if vertical_target_word else None,
            "diagonalTargetWord": diagonal_target_word if diagonal_target_word else None,
            "longestAnagram": longest_anagram if longest_anagram else None,
            "longestAnagramCount": int(len(longest_anagram)) if longest_anagram else 0,
            "userAnagram": None,
        },
        "grid_rows": rows,
        "valid_words": valid_words,
        "valid_words_metadata": valid_words_metadata,
        "total_score": int(total_score),
    }
    if meta_extra:
        payload["meta"].update(meta_extra)
    if extra_fields:
        payload.update(extra_fields)
    return payload
