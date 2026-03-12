# test_remove_pre_loaded_words.py
from copy import deepcopy
from typing import Any, Dict

import pytest

from margana_score import remove_pre_loaded_words


@pytest.fixture()
def meta() -> Dict[str, Any]:
    # Note: your function expects the keys at the top-level, not nested under "meta"
    return {
        "date": "2025-12-14",
        "rows": 5,
        "cols": 5,
        "wordLength": 5,
        "columnIndex": 4,
        "diagonalDirection": "main",
        "verticalTargetWord": "smart",
        "diagonalTargetWord": "wheat",
        "madnessAvailable": True,
        "longestAnagramCount": 10,
        "userAnagram": "STALE",
        "skippedRows": [],
    }


@pytest.fixture()
def valid_items() -> list[dict[str, Any]]:
    return [
        {"word": "wands", "type": "row", "index": 0, "direction": "lr", "start_index": {"r": 0, "c": 0}, "end_index": {"r": 0, "c": 4}},
        {"word": "charm", "type": "row", "index": 1, "direction": "lr", "start_index": {"r": 1, "c": 0}, "end_index": {"r": 1, "c": 4}},
        {"word": "omega", "type": "row", "index": 2, "direction": "lr", "start_index": {"r": 2, "c": 0}, "end_index": {"r": 2, "c": 4}},
        {"word": "altar", "type": "row", "index": 3, "direction": "lr", "start_index": {"r": 3, "c": 0}, "end_index": {"r": 3, "c": 4}},
        {"word": "smart", "type": "row", "index": 4, "direction": "lr", "start_index": {"r": 4, "c": 0}, "end_index": {"r": 4, "c": 4}},
        {"word": "trams", "type": "row", "index": 4, "direction": "rl", "start_index": {"r": 4, "c": 4}, "end_index": {"r": 4, "c": 0}},
        {"word": "stale", "type": "anagram", "index": 0, "direction": "builder", "start_index": None, "end_index": None},
    ]


def test_does_not_remove_row_occurrences_of_target_words(meta, valid_items):
    """
    remove_pre_loaded_words only removes:
      - vertical target word when type=="column" and index==columnIndex
      - diagonal target word when type=="diagonal" and direction matches diagonalDirection/main_rev/etc
    So the row word "smart" should remain.
    """
    out = remove_pre_loaded_words(meta, deepcopy(valid_items))
    words = {it["word"] for it in out}

    assert "smart" in words
    assert "trams" in words
    assert "stale" in words


def test_removes_vertical_target_when_present_as_column_at_column_index(meta, valid_items):
    items = deepcopy(valid_items)
    items.append(
        {
            "word": "smart",
            "type": "column",
            "index": 4,  # must match meta["columnIndex"]
            "direction": "tb",
            "start_index": {"r": 0, "c": 4},
            "end_index": {"r": 4, "c": 4},
        }
    )

    out = remove_pre_loaded_words(meta, items)
    remaining = [(it["type"], it.get("index"), it["word"]) for it in out]

    assert ("column", 4, "smart") not in remaining
    # row instances remain
    assert ("row", 4, "smart") in remaining


def test_removes_reverse_vertical_target_too(meta, valid_items):
    items = deepcopy(valid_items)
    items.append(
        {
            "word": "trams",  # reverse of "smart"
            "type": "column",
            "index": 4,
            "direction": "bt",
            "start_index": {"r": 4, "c": 4},
            "end_index": {"r": 0, "c": 4},
        }
    )

    out = remove_pre_loaded_words(meta, items)
    assert not any(it["type"] == "column" and it.get("index") == 4 and it["word"] == "trams" for it in out)


def test_removes_diagonal_target_only_on_configured_diagonal(meta, valid_items):
    items = deepcopy(valid_items)

    # should be removed: main / main_rev (because meta diagonalDirection == "main")
    items.append(
        {
            "word": "wheat",
            "type": "diagonal",
            "index": 0,
            "direction": "main",
            "start_index": {"r": 0, "c": 0},
            "end_index": {"r": 4, "c": 4},
        }
    )
    items.append(
        {
            "word": "taehw",  # reverse of wheat
            "type": "diagonal",
            "index": 0,
            "direction": "main_rev",
            "start_index": {"r": 4, "c": 4},
            "end_index": {"r": 0, "c": 0},
        }
    )

    # should NOT be removed: same word but on the other diagonal bucket
    items.append(
        {
            "word": "wheat",
            "type": "diagonal",
            "index": 0,
            "direction": "anti",
            "start_index": {"r": 0, "c": 4},
            "end_index": {"r": 4, "c": 0},
        }
    )

    out = remove_pre_loaded_words(meta, items)

    assert not any(it["type"] == "diagonal" and it["direction"] == "main" and it["word"] == "wheat" for it in out)
    assert not any(it["type"] == "diagonal" and it["direction"] == "main_rev" and it["word"] == "taehw" for it in out)
    assert any(it["type"] == "diagonal" and it["direction"] == "anti" and it["word"] == "wheat" for it in out)


def test_length_guard_prevents_removal_when_word_length_mismatches(meta, valid_items):
    items = deepcopy(valid_items)
    items.append(
        {
            "word": "whea",  # len=4, meta["wordLength"]=5 => should never be excluded
            "type": "diagonal",
            "index": 0,
            "direction": "main",
            "start_index": {"r": 0, "c": 0},
            "end_index": {"r": 3, "c": 3},
        }
    )
    items.append(
        {
            "word": "smar",  # len=4, would otherwise look like vertical target prefix
            "type": "column",
            "index": 4,
            "direction": "tb",
            "start_index": {"r": 0, "c": 4},
            "end_index": {"r": 3, "c": 4},
        }
    )

    out = remove_pre_loaded_words(meta, items)
    words = {it["word"] for it in out}

    assert "whea" in words
    assert "smar" in words


def test_keeps_row_semordnilap_when_same_text_as_diagonal_target():
    # Regression: if dt/its reverse appears on a valid row, keep the row entries.
    # Only remove the fixed target diagonal path entries.
    meta = {
        "wordLength": 5,
        "columnIndex": 3,
        "diagonalDirection": "anti",
        "verticalTargetWord": "beech",
        "diagonalTargetWord": "serif",
    }
    items = [
        {"word": "fires", "type": "row", "index": 2, "direction": "lr"},
        {"word": "serif", "type": "row", "index": 2, "direction": "rl"},
        {"word": "serif", "type": "diagonal", "index": 0, "direction": "anti"},
        {"word": "fires", "type": "diagonal", "index": 0, "direction": "anti_rev"},
        # same text on a non-target diagonal must be kept
        {"word": "serif", "type": "diagonal", "index": 0, "direction": "main"},
    ]

    out = remove_pre_loaded_words(meta, items)

    assert any(it["type"] == "row" and it["direction"] == "lr" and it["word"] == "fires" for it in out)
    assert any(it["type"] == "row" and it["direction"] == "rl" and it["word"] == "serif" for it in out)
    assert not any(it["type"] == "diagonal" and it["direction"] == "anti" and it["word"] == "serif" for it in out)
    assert not any(it["type"] == "diagonal" and it["direction"] == "anti_rev" and it["word"] == "fires" for it in out)
    assert any(it["type"] == "diagonal" and it["direction"] == "main" and it["word"] == "serif" for it in out)
