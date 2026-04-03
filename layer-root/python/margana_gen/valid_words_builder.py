from __future__ import annotations

from typing import Any


def build_valid_words_map(items: list[dict[str, Any]], allowed_words: list[str]) -> dict[str, Any]:
    words_set_for_map = set(allowed_words)
    valid_words_map = {
        "rows": {"lr": [], "rl": []},
        "columns": {"tb": [], "bt": []},
        "diagonals": {"main": [], "main_rev": [], "anti": [], "anti_rev": []},
        "anagram": [],
    }
    seen_map = {
        "rows": {"lr": set(), "rl": set()},
        "columns": {"tb": set(), "bt": set()},
        "diagonals": {"main": set(), "main_rev": set(), "anti": set(), "anti_rev": set()},
        "anagram": set(),
    }
    seen_diagonals_all = set()

    for item in items:
        word = str(item.get("word") or "").lower()
        item_type = item.get("type")
        direction = item.get("direction")
        if not word or word not in words_set_for_map:
            continue
        if item_type == "row":
            bucket = "lr" if direction == "lr" else ("rl" if direction == "rl" else None)
            if bucket:
                valid_words_map["rows"][bucket].append(word)
        elif item_type == "column":
            bucket = "tb" if direction == "tb" else ("bt" if direction == "bt" else None)
            if bucket and word not in seen_map["columns"][bucket]:
                valid_words_map["columns"][bucket].append(word)
                seen_map["columns"][bucket].add(word)
        elif item_type == "diagonal":
            if direction in ("main", "main_rev", "anti", "anti_rev"):
                if word not in seen_diagonals_all and word not in seen_map["diagonals"][direction]:
                    valid_words_map["diagonals"][direction].append(word)
                    seen_map["diagonals"][direction].add(word)
                    seen_diagonals_all.add(word)
        elif item_type == "anagram":
            if word not in seen_map["anagram"]:
                valid_words_map["anagram"].append(word)
                seen_map["anagram"].add(word)

    return valid_words_map
