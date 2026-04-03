from __future__ import annotations

from typing import Any, Callable, Iterable


def build_valid_words_metadata(
    items: list[dict[str, Any]],
    *,
    longest_anagram: str | None = None,
) -> list[dict[str, Any]]:
    metadata: list[dict[str, Any]] = []
    for item in items:
        metadata.append(
            {
                "word": item.get("word"),
                "type": item.get("type"),
                "index": int(item.get("index") or 0),
                "direction": item.get("direction"),
                "start_index": item.get("start_index"),
                "end_index": item.get("end_index"),
            }
        )

    if longest_anagram:
        metadata.append(
            {
                "word": longest_anagram,
                "type": "anagram",
                "index": 0,
                "direction": "builder",
                "start_index": None,
                "end_index": None,
            }
        )

    return metadata


def enrich_valid_words_metadata(
    items: list[dict[str, Any]],
    *,
    letter_scores: dict[str, int],
    score_word: Callable[[str], int],
    semordnilap_words: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    semordnilap_set = {str(word).lower() for word in (semordnilap_words or []) if word}

    for item in items:
        word = str(item.get("word") or "")
        item_type = item.get("type")
        base = int(score_word(word))
        is_palindrome = item_type != "anagram" and bool(word) and word == word[::-1]
        reverse = word[::-1] if isinstance(word, str) else ""
        is_semordnilap = (
            item_type != "anagram"
            and reverse != word
            and reverse.lower() in semordnilap_set
        )

        item["palindrome"] = bool(is_palindrome)
        item["semordnilap"] = bool(is_semordnilap)
        try:
            item["letter_value"] = {
                ch: int(letter_scores.get(ch, 0))
                for ch in set(word.lower())
                if ch
            }
        except Exception:
            item["letter_value"] = {ch: 0 for ch in set(word.lower()) if ch}

        letters = [ch for ch in word.lower() if ch]
        try:
            letter_scores_seq = [int(letter_scores.get(ch, 0)) for ch in letters]
        except Exception:
            letter_scores_seq = [0 for _ in letters]

        item["letters"] = letters
        item["letter_scores"] = letter_scores_seq
        item["letter_sum"] = int(base)
        base_score = base * 2 if is_palindrome else base
        item["base_score"] = int(base_score)
        item["bonus"] = 0
        item["score"] = int(base_score)

    return items
