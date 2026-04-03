from __future__ import annotations

from margana_gen.valid_words_builder import build_valid_words_map


def test_build_valid_words_map_preserves_duplicate_rows_and_dedupes_other_buckets():
    items = [
        {"word": "acrid", "type": "row", "direction": "lr"},
        {"word": "acrid", "type": "row", "direction": "lr"},
        {"word": "dirca", "type": "row", "direction": "rl"},
        {"word": "tower", "type": "column", "direction": "tb"},
        {"word": "tower", "type": "column", "direction": "tb"},
        {"word": "stare", "type": "diagonal", "direction": "main"},
        {"word": "stare", "type": "diagonal", "direction": "main_rev"},
        {"word": "listen", "type": "anagram", "direction": "builder"},
        {"word": "listen", "type": "anagram", "direction": "builder"},
    ]

    result = build_valid_words_map(
        items,
        ["acrid", "dirca", "tower", "stare", "listen"],
    )

    assert result == {
        "rows": {"lr": ["acrid", "acrid"], "rl": ["dirca"]},
        "columns": {"tb": ["tower"], "bt": []},
        "diagonals": {"main": ["stare"], "main_rev": [], "anti": [], "anti_rev": []},
        "anagram": ["listen"],
    }
