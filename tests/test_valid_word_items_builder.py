from __future__ import annotations

from margana_gen.valid_word_items_builder import build_valid_word_items


def test_build_valid_word_items_with_coordinates_and_diagonal_filtering():
    rows = ["abcde", "fghij", "klmno", "pqrst", "uvwxy"]

    result = build_valid_word_items(
        grid_rows=rows,
        words5={"abcde", "edcba", "afkpu", "upkfa"},
        diagonal_words={"agmsy", "ysmga", "agm"},
        diagonal_lengths={3, 4, 5},
        include_coordinates=True,
    )

    assert result == [
        {
            "word": "abcde",
            "type": "row",
            "index": 0,
            "direction": "lr",
            "start_index": {"r": 0, "c": 0},
            "end_index": {"r": 0, "c": 4},
        },
        {
            "word": "edcba",
            "type": "row",
            "index": 0,
            "direction": "rl",
            "start_index": {"r": 0, "c": 4},
            "end_index": {"r": 0, "c": 0},
        },
        {
            "word": "afkpu",
            "type": "column",
            "index": 0,
            "direction": "tb",
            "start_index": {"r": 0, "c": 0},
            "end_index": {"r": 4, "c": 0},
        },
        {
            "word": "upkfa",
            "type": "column",
            "index": 0,
            "direction": "bt",
            "start_index": {"r": 4, "c": 0},
            "end_index": {"r": 0, "c": 0},
        },
        {
            "word": "agmsy",
            "type": "diagonal",
            "index": 0,
            "direction": "main",
            "start_index": {"r": 0, "c": 0},
            "end_index": {"r": 4, "c": 4},
        },
        {
            "word": "ysmga",
            "type": "diagonal",
            "index": 0,
            "direction": "main_rev",
            "start_index": {"r": 4, "c": 4},
            "end_index": {"r": 0, "c": 0},
        },
    ]


def test_build_valid_word_items_without_coordinates_omits_coordinate_fields():
    result = build_valid_word_items(
        grid_rows=["margz", "xxxxx", "xxxxx", "xxxxx", "xxxxx"],
        words5={"margz"},
        diagonal_words=set(),
        diagonal_lengths={2, 3, 4, 5},
        include_coordinates=False,
    )

    assert result == [
        {
            "word": "margz",
            "type": "row",
            "index": 0,
            "direction": "lr",
        }
    ]
