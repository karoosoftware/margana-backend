from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from margana_gen.validation import (
    FixedTargetExclusionRule,
    PuzzleValidationContext,
    TotalScoreRule,
    default_rules,
    run_collection_validations,
    run_validations,
)


def _load_payload(rel_path: str) -> dict:
    root = Path(__file__).resolve().parents[1]
    return json.loads((root / rel_path).read_text(encoding="utf-8"))


def test_default_validations_accept_sample_completed_payload():
    payload = _load_payload("tests/fixtures/payloads/sample-completed.json")
    semi = _load_payload("tests/fixtures/payloads/sample-semi-completed.json")

    result = run_validations(
        PuzzleValidationContext(completed_payload=payload, semi_completed_payload=semi),
        default_rules(),
    )

    assert result.ok, [issue.code for issue in result.issues]


def test_fixed_target_exclusion_rejects_vertical_and_diagonal_target_words_and_reverses():
    payload = {
        "meta": {
            "rows": 5,
            "cols": 5,
            "wordLength": 5,
            "columnIndex": 0,
            "diagonalDirection": "main",
            "verticalTargetWord": "gaudy",
            "diagonalTargetWord": "tends",
        },
        "grid_rows": ["gxxxx", "axxxx", "uxxxx", "dxxxx", "yxxxx"],
        "valid_words_metadata": [
            {"word": "gaudy", "type": "column", "index": 0, "direction": "tb", "base_score": 10, "bonus": 0, "score": 10},
            {"word": "yduag", "type": "column", "index": 0, "direction": "bt", "base_score": 10, "bonus": 0, "score": 10},
            {"word": "tends", "type": "diagonal", "index": 0, "direction": "main", "base_score": 10, "bonus": 0, "score": 10},
            {"word": "sdnet", "type": "diagonal", "index": 0, "direction": "main_rev", "base_score": 10, "bonus": 0, "score": 10},
        ],
        "total_score": 40,
    }

    result = run_validations(
        PuzzleValidationContext(completed_payload=payload),
        [FixedTargetExclusionRule()],
    )

    assert not result.ok
    assert [issue.code for issue in result.issues] == [
        "fixed_target_word_scored",
        "fixed_target_word_scored",
        "fixed_target_word_scored",
        "fixed_target_word_scored",
    ]


def test_total_score_rule_flags_item_and_total_mismatches():
    payload = {
        "meta": {},
        "grid_rows": [],
        "valid_words_metadata": [
            {"word": "alpha", "base_score": 10, "bonus": 2, "score": 11},
            {"word": "bravo", "base_score": 7, "bonus": 0, "score": 7},
        ],
        "total_score": 99,
    }

    result = run_validations(
        PuzzleValidationContext(completed_payload=payload),
        [TotalScoreRule()],
    )

    assert not result.ok
    assert [issue.code for issue in result.issues] == [
        "item_score_mismatch",
        "total_score_mismatch",
    ]


def test_default_validations_reject_non_zero_bonus():
    payload = {
        "meta": {
            "rows": 5,
            "cols": 5,
            "wordLength": 5,
            "columnIndex": 0,
            "diagonalDirection": "main",
            "verticalTargetWord": "abcde",
            "diagonalTargetWord": "abcde",
            "longestAnagram": "abcdefgh",
            "longestAnagramCount": 8,
        },
        "vertical_target_word": "abcde",
        "diagonal_target_word": "abcde",
        "diagonal_direction": "main",
        "grid_rows": ["axxxx", "bxxxx", "cxxxx", "dxxxx", "exxxx"],
        "valid_words_metadata": [
            {"word": "xxxxx", "type": "row", "index": 0, "direction": "lr", "base_score": 10, "bonus": 5, "score": 15}
        ],
        "total_score": 15,
    }

    result = run_validations(PuzzleValidationContext(completed_payload=payload), default_rules())

    assert not result.ok
    assert "non_zero_bonus" in [issue.code for issue in result.issues]


def test_default_validations_reject_grid_with_wrong_row_count():
    payload = {
        "meta": {"rows": 4, "cols": 5, "wordLength": 5},
        "grid_rows": ["views", "vodka", "lager", "queue"],
        "valid_words_metadata": [],
        "total_score": 0,
    }

    result = run_validations(PuzzleValidationContext(completed_payload=payload), default_rules())

    assert not result.ok
    assert "grid_must_have_five_rows" in [issue.code for issue in result.issues]


def test_default_validations_reject_grid_row_with_wrong_length():
    payload = {
        "meta": {"rows": 5, "cols": 5, "wordLength": 5},
        "grid_rows": ["views", "vodkaa", "lager", "queue", "furze"],
        "valid_words_metadata": [],
        "total_score": 0,
    }

    result = run_validations(PuzzleValidationContext(completed_payload=payload), default_rules())

    assert not result.ok
    assert "grid_row_must_have_five_chars" in [issue.code for issue in result.issues]


def test_default_validations_reject_valid_words_rows_lr_mismatch():
    payload = {
        "meta": {"rows": 5, "cols": 5, "wordLength": 5},
        "grid_rows": ["views", "vodka", "lager", "queue", "furze"],
        "valid_words": {
            "rows": {
                "lr": ["views", "vodka", "lager", "queue", "wrong"],
            }
        },
        "valid_words_metadata": [],
        "total_score": 0,
    }

    result = run_validations(PuzzleValidationContext(completed_payload=payload), default_rules())

    assert not result.ok
    assert "valid_words_rows_lr_mismatch" in [issue.code for issue in result.issues]


def test_default_validations_accept_duplicate_rows_when_lr_matches_grid_rows():
    payload = {
        "meta": {"rows": 5, "cols": 5, "wordLength": 5},
        "grid_rows": ["aorta", "acrid", "acrid", "cuber", "brads"],
        "valid_words": {
            "rows": {
                "lr": ["aorta", "acrid", "acrid", "cuber", "brads"],
                "rl": [],
            }
        },
        "valid_words_metadata": [],
        "total_score": 0,
    }

    result = run_validations(PuzzleValidationContext(completed_payload=payload), default_rules())

    assert result.ok, [issue.code for issue in result.issues]


def test_default_validations_reject_valid_words_rows_rl_mismatch():
    payload = {
        "meta": {"rows": 5, "cols": 5, "wordLength": 5},
        "grid_rows": ["views", "vodka", "lager", "queue", "furze"],
        "valid_words": {
            "rows": {
                "lr": ["views", "vodka", "lager", "queue", "furze"],
                "rl": ["regal", "notit"],
            }
        },
        "valid_words_metadata": [],
        "total_score": 0,
    }

    result = run_validations(PuzzleValidationContext(completed_payload=payload), default_rules())

    assert not result.ok
    assert "valid_words_rows_rl_mismatch" in [issue.code for issue in result.issues]


def test_default_validations_reject_longest_anagram_count_mismatch():
    payload = _load_payload("tests/fixtures/payloads/sample-completed.json")
    broken = deepcopy(payload)
    broken["meta"] = dict(broken["meta"])
    broken["meta"]["longestAnagramCount"] = broken["meta"]["longestAnagramCount"] - 1

    result = run_validations(PuzzleValidationContext(completed_payload=broken), default_rules())

    assert not result.ok
    assert "longest_anagram_count_mismatch" in [issue.code for issue in result.issues]


def test_default_validations_reject_anagram_metadata_word_mismatch():
    payload = _load_payload("tests/fixtures/payloads/sample-completed.json")
    broken = deepcopy(payload)
    broken["valid_words_metadata"] = list(broken["valid_words_metadata"])
    for item in broken["valid_words_metadata"]:
        if item.get("type") == "anagram":
            item["word"] = "wrongword"
            break

    result = run_validations(PuzzleValidationContext(completed_payload=broken), default_rules())

    assert not result.ok
    assert "anagram_metadata_word_mismatch" in [issue.code for issue in result.issues]


def test_default_validations_reject_madness_fields_when_unavailable():
    payload = _load_payload("tests/fixtures/payloads/sample-completed.json")
    broken = deepcopy(payload)
    broken["meta"] = dict(broken["meta"])
    broken["meta"]["madnessWord"] = "margana"

    result = run_validations(PuzzleValidationContext(completed_payload=broken), default_rules())

    assert not result.ok
    assert "madness_fields_present_when_unavailable" in [issue.code for issue in result.issues]


def test_default_validations_reject_missing_madness_fields_when_available():
    payload = _load_payload("tests/fixtures/payloads/sample-madness-completed.json")
    broken = deepcopy(payload)
    broken["meta"] = dict(broken["meta"])
    broken["meta"]["madnessPath"] = None

    result = run_validations(PuzzleValidationContext(completed_payload=broken), default_rules())

    assert not result.ok
    assert "madness_fields_missing_when_available" in [issue.code for issue in result.issues]


def test_default_validations_reject_semi_completed_mismatch():
    payload = _load_payload("tests/fixtures/payloads/sample-completed.json")
    semi = _load_payload("tests/fixtures/payloads/sample-semi-completed.json")
    broken_semi = deepcopy(semi)
    broken_semi["grid_rows"] = list(broken_semi["grid_rows"])
    broken_row = list(broken_semi["grid_rows"][0])
    broken_row[0] = "z"
    broken_semi["grid_rows"][0] = "".join(broken_row)

    result = run_validations(
        PuzzleValidationContext(completed_payload=payload, semi_completed_payload=broken_semi),
        default_rules(),
    )

    assert not result.ok
    assert "semi_completed_visible_letter_mismatch" in [issue.code for issue in result.issues]


def test_default_validations_report_anagram_inventory_gap():
    payload = _load_payload("tests/fixtures/payloads/sample-completed.json")
    broken = deepcopy(payload)
    broken["meta"] = dict(broken["meta"])
    broken["meta"]["longestAnagram"] = "zzzzzzzz"
    broken["meta"]["longestAnagramCount"] = 8

    result = run_validations(PuzzleValidationContext(completed_payload=broken), default_rules())

    assert not result.ok
    assert "anagram_letters_missing" in [issue.code for issue in result.issues]


def _week_payload(day: str, *, madness: bool) -> PuzzleValidationContext:
    return PuzzleValidationContext(
        completed_payload={
            "meta": {
                "date": day,
                "madnessAvailable": madness,
            }
        }
    )


def test_collection_validations_accept_one_madness_in_complete_iso_week():
    contexts = [
        _week_payload("2026-03-30", madness=False),
        _week_payload("2026-03-31", madness=False),
        _week_payload("2026-04-01", madness=False),
        _week_payload("2026-04-02", madness=True),
        _week_payload("2026-04-03", madness=False),
        _week_payload("2026-04-04", madness=False),
        _week_payload("2026-04-05", madness=False),
    ]

    result = run_collection_validations(contexts)

    assert result.ok, [issue.code for issue in result.issues]


def test_collection_validations_reject_wrong_madness_count_in_complete_iso_week():
    contexts = [
        _week_payload("2026-03-30", madness=False),
        _week_payload("2026-03-31", madness=False),
        _week_payload("2026-04-01", madness=False),
        _week_payload("2026-04-02", madness=False),
        _week_payload("2026-04-03", madness=False),
        _week_payload("2026-04-04", madness=False),
        _week_payload("2026-04-05", madness=False),
    ]

    result = run_collection_validations(contexts)

    assert not result.ok
    assert "weekly_madness_count_mismatch" in [issue.code for issue in result.issues]
