from __future__ import annotations

import json
from typing import List, Dict, Any

import pytest

from margana_score.margana_madness import integrate_madness, detect_madness, find_madness_path


def _cells_from_rows(rows: List[str]) -> List[Dict[str, Any]]:
    cells = []
    for r, row in enumerate(rows):
        for c, ch in enumerate(row):
            cells.append({"r": r, "c": c, "letter": ch.upper(), "target": False, "targetType": None})
    return cells


def _body_from_rows(rows: List[str], madness: bool = True) -> Dict[str, Any]:
    return {
        "meta": {"rows": len(rows), "cols": len(rows[0]) if rows else 0, "madnessAvailable": bool(madness)},
        "cells": _cells_from_rows(rows),
    }


def test_flag_off_no_detection():
    rows = [
        "abcde",
        "fghij",
        "klmno",
        "pqrst",
        "uvwxy",
    ]
    body = _body_from_rows(rows, madness=False)
    payload = {"meta": {}, "valid_words_metadata": []}
    out = integrate_madness(payload, body)
    assert out["meta"].get("madnessFound") is False
    assert not any((it.get("type") == "madness") for it in out.get("valid_words_metadata", []))


def test_detect_margana_found_simple_path():
    # Craft a small grid with a straight line path for 'margana' diagonally
    # m a r g a n a
    rows = [
        "mzzzzzz",
        "zazzzzz",
        "zzrzzzz",
        "zzzgzzz",
        "zzzzazz",
        "zzzzznz",
        "zzzzzza",
    ]
    body = _body_from_rows(rows, madness=True)
    payload = {"meta": {}, "valid_words_metadata": []}
    out = integrate_madness(payload, body)
    assert out["meta"].get("madnessFound") is True
    assert out["meta"].get("madnessWord") == "margana"
    path = out["meta"].get("madnessPath")
    assert isinstance(path, list) and len(path) == 7  # 7 letters
    # Check metadata item exists
    assert any((it.get("type") == "madness" and it.get("word") == "margana") for it in out.get("valid_words_metadata", []))


def test_detect_fallback_anagram():
    # Grid with 'anagram' in a snaking path
    # a n a g r a m
    rows = [
        "azzzzzz",
        "znzzzzz",
        "zzazzzz",
        "zzzgzzz",
        "zzzzrzz",
        "zzzzzaz",
        "zzzzzzm",
    ]
    body = _body_from_rows(rows, madness=True)
    payload = {"meta": {}, "valid_words_metadata": []}
    out = integrate_madness(payload, body)
    assert out["meta"].get("madnessFound") is True
    assert out["meta"].get("madnessWord") in ("anagram", "margana")


def test_provided_path_honored():
    rows = [
        "abc",
        "def",
        "ghi",
    ]
    body = _body_from_rows(rows, madness=True)
    # Inject a provided path/word into meta
    body["meta"]["madnessWord"] = "margana"
    body["meta"]["madnessPath"] = [[0, 0], [0, 1]]  # short path; still should be honored as provided
    payload = {"meta": {}, "valid_words_metadata": []}
    out = integrate_madness(payload, body)
    assert out["meta"].get("madnessFound") is True
    assert out["meta"].get("madnessWord") == "margana"
    assert out["meta"].get("madnessPath") == [[0, 0], [0, 1]]


def test_not_found():
    rows = [
        "skulk",
        "madam",
        "argon",
        "waned",
        "other",
    ]
    body = _body_from_rows(rows, madness=True)
    payload = {"meta": {}, "valid_words_metadata": []}
    out = integrate_madness(payload, body)
    assert out["meta"].get("madnessFound") is False
    

def test_madness_excludes_invalid_rows():
    # Create a grid where 'margana' exists entirely on row 1, but that row is invalid.
    # Rows: 0,1,2; Cols: 7
    rows = [
        "zzzzzzz",
        "margana",
        "zzzzzzz",
    ]
    body = _body_from_rows(rows, madness=True)

    # Base payload with row_summaries marking row 1 invalid
    payload = {
        "meta": {},
        "valid_words_metadata": [],
        "row_summaries": [
            {"row": 0, "skipped": False, "word": "", "valid": True, "score": 0},
            {"row": 1, "skipped": False, "word": "margana", "valid": False, "score": 0},
            {"row": 2, "skipped": False, "word": "", "valid": True, "score": 0},
        ],
    }

    out = integrate_madness(payload, body)
    # Because row 1 is invalid, the madness path must be ignored
    assert out["meta"].get("madnessFound") is False
    vwm = out.get("valid_words_metadata") or []
    assert not any((it.get("type") == "madness") for it in vwm)


def test_madness_metadata_contains_scoring_fields():
    # Grid with a diagonal 'margana' path (same as earlier simple path)
    rows = [
        "mzzzzzz",
        "zazzzzz",
        "zzrzzzz",
        "zzzgzzz",
        "zzzzazz",
        "zzzzznz",
        "zzzzzza",
    ]
    body = _body_from_rows(rows, madness=True)
    payload = {"meta": {}, "valid_words_metadata": []}
    out = integrate_madness(payload, body)
    assert out["meta"].get("madnessFound") is True
    vwm = out.get("valid_words_metadata") or []
    madness_items = [it for it in vwm if it.get("type") == "madness"]
    assert madness_items, "madness metadata item should be present"
    it = madness_items[0]
    # Expected fields
    assert it.get("palindrome") is False
    assert it.get("semordnilap") is False
    assert isinstance(it.get("letter_value"), dict)
    assert isinstance(it.get("base_score"), int)
    assert isinstance(it.get("bonus"), int)
    assert isinstance(it.get("score"), int)
    # Score relationships
    assert it.get("base_score") == it.get("letter_sum")
    assert it.get("score") == it.get("base_score") + it.get("bonus")
    # letter_value should include at least these letters with their scores
    lv = it.get("letter_value") or {}
    for ch, val in {"m": 6, "a": 3, "r": 3, "g": 5, "n": 3}.items():
        assert lv.get(ch) == val
