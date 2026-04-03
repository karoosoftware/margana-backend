from __future__ import annotations

from typing import Callable


def make_score_word(letter_scores: dict[str, int]) -> Callable[[str], int]:
    normalized_scores = {
        str(key).lower(): int(value)
        for key, value in letter_scores.items()
        if isinstance(value, (int, float))
    }

    def score_word(word: str) -> int:
        if not word:
            return 0
        return sum(int(normalized_scores.get(ch, 0)) for ch in str(word).lower())

    return score_word


def compute_basic_total(
    *,
    rows: list[str],
    target: str,
    diag: str,
    longest_anagram: str,
    score_word: Callable[[str], int],
) -> int:
    return (
        sum(score_word(word) for word in rows)
        + score_word(target)
        + score_word(diag)
        + score_word(longest_anagram)
    )
