from __future__ import annotations

from margana_gen.semi_completed_builder import (
    build_semi_completed_payload,
    mask_grid_rows,
    shuffle_word_deterministic,
)


def test_mask_grid_rows_reveals_only_target_column_and_diagonal():
    rows = ["views", "vodka", "lager", "queue", "furze"]

    masked = mask_grid_rows(rows, 4, "main")

    assert masked == [
        "v***s",
        "*o**a",
        "**g*r",
        "***ue",
        "****e",
    ]


def test_build_semi_completed_payload_preserves_expected_shape():
    payload = build_semi_completed_payload(
        date="2026-02-20",
        puzzle_id="abc123",
        layout_id="layout456",
        vertical_target_word="aloes",
        column_index=4,
        diagonal_direction="main",
        diagonal_target_word="mocks",
        rows=["mamba", "vowel", "wacko", "quake", "draws"],
        longest_anagram_count=10,
        longest_anagram_shuffled="bwsaordkla",
        extra_fields={"madnessAvailable": False, "difficultyBandApplied": "hard"},
    )

    assert payload == {
        "date": "2026-02-20",
        "id": "abc123",
        "chain_id": "layout456",
        "word_length": 5,
        "vertical_target_word": "aloes",
        "column_index": 4,
        "diagonal_direction": "main",
        "diagonal_target_word": "mocks",
        "grid_rows": ["m***a", "*o**l", "**c*o", "***ke", "****s"],
        "longest_anagram_count": 10,
        "longestAnagramShuffled": "bwsaordkla",
        "madnessAvailable": False,
        "difficultyBandApplied": "hard",
    }


def test_shuffle_word_deterministic_is_stable_and_not_equal_when_possible():
    shuffled_once = shuffle_word_deterministic("boardwalks", "layout|2026-04-01|boardwalks")
    shuffled_twice = shuffle_word_deterministic("boardwalks", "layout|2026-04-01|boardwalks")

    assert shuffled_once == shuffled_twice
    assert sorted(shuffled_once) == sorted("boardwalks")
    assert shuffled_once != "boardwalks"
