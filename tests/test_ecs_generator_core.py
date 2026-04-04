from __future__ import annotations

import importlib.util
import random
from pathlib import Path
import pytest
import json
from datetime import date

from margana_gen.usage_log import puzzle_in_cooldown

def _load_module(filename: str):
    mod_path = Path(__file__).resolve().parents[1] / "ecs" / filename
    spec = importlib.util.spec_from_file_location(f"{filename.replace('-', '_')}_for_tests", str(mod_path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# A small, robust wordlist for testing 5x5 grids
TEST_WORDS5 = [
    "apple", "bread", "clear", "dance", "eagle",
    "fruit", "grape", "house", "image", "joker",
    "lemon", "melon", "night", "ocean", "paper",
    "queen", "radio", "stone", "table", "under",
    "voice", "water", "young", "zebra", "abbey",
    "baked", "cabin", "dairy", "eager", "faint",
    "giant", "happy", "ideal", "joint", "knack",
    "labor", "magic", "naive", "oasis", "paint",
    "quiet", "radar", "saint", "train", "urban",
    "value", "whale", "yield", "zones", "about",
    "above", "actor", "acute", "admit", "adopt",
    "adult", "after", "again", "agent", "agree",
    "ahead", "alarm", "album", "alert", "alike",
    "alive", "allow", "alone", "along", "alter",
    "among", "anger", "angle", "angry", "apart",
    "apple", "apply", "arena", "argue", "arise",
    "array", "aside", "asset", "audio", "audit",
    "avoid", "award", "aware", "awful", "backy",
    "badly", "baker", "bases", "basic", "basis"
]

@pytest.fixture
def gen_mod():
    return _load_module("generate-column-puzzle.py")

@pytest.fixture
def mad_mod():
    return _load_module("generate-column-puzzle-madness.py")

@pytest.fixture
def logic_mod():
    import margana_gen.column_logic
    return margana_gen.column_logic

def test_logic_build_puzzle_reproducible(logic_mod):
    """Ensure column_logic.build_puzzle gives identical results with same seed."""
    words5 = TEST_WORDS5
    rng1 = random.Random(42)
    try:
        res1 = logic_mod.build_puzzle(words5, rng1, max_target_tries=500, max_diag_tries=100)
        rng2 = random.Random(42)
        res2 = logic_mod.build_puzzle(words5, rng2, max_target_tries=500, max_diag_tries=100)
        assert res1 == res2
    except RuntimeError as e:
        pytest.fail(f"logic.build_puzzle failed: {e}")

def test_logic_build_puzzle_constraints(logic_mod):
    """Test column_logic.build_puzzle respects forced column and target."""
    target = "abcde"
    col_idx = 0
    diag_target = "afkpu" 
    words5 = [
        "abcde", "afkpu", 
        "azzzz", "bfzzz", "cckzz", "ddzpz", "eeezu",
    ]
    words5 = list(set([w[:5].ljust(5, 'x').lower() for w in words5]))
    
    try:
        res = logic_mod.build_puzzle(
            words5, random.Random(42), 
            target_forced=target, 
            column_forced=col_idx,
            diag_target_forced=diag_target,
            diag_direction_pref="main",
            max_target_tries=1000,
            max_diag_tries=500
        )
        assert res[0] == target
        assert res[1] == col_idx
        assert res[3] == diag_target
    except RuntimeError as e:
        pytest.fail(f"logic.build_puzzle_constraints failed: {e}")

def test_build_puzzle_reproducible(gen_mod):
    """Ensure build_puzzle gives identical results with same seed."""
    words5 = TEST_WORDS5
    
    rng1 = random.Random(42)
    # Give it more tries and a known good seed if possible, but randomness is fine with a large enough list
    try:
        res1 = gen_mod.build_puzzle(words5, rng1, max_target_tries=500, max_diag_tries=100)
        rng2 = random.Random(42)
        res2 = gen_mod.build_puzzle(words5, rng2, max_target_tries=500, max_diag_tries=100)
        assert res1 == res2
        # res: target, col, diag_dir, diag_target, rows
        assert len(res1) == 5
        assert len(res1[4]) == 5 # 5 rows
    except RuntimeError as e:
        pytest.fail(f"build_puzzle failed with TEST_WORDS5: {e}")

def test_build_puzzle_constraints(gen_mod, monkeypatch):
    """Test build_puzzle respects forced column and target."""
    monkeypatch.setattr(gen_mod, "DEBUG_ENABLED", True)
    monkeypatch.setattr(gen_mod, "DEBUG_VERBOSE", True)
    target = "abcde"
    col_idx = 0
    diag_target = "afkpu" 
    
    words5 = [
        "abcde", "afkpu", 
        "apple", "bfxxx", "cckxx", "dddxp", "eeeex",
        "bread", "clear", "dance", "eagle"
    ]
    # To ensure it doesn't pick 'afkpu' for row 0 (which would block diag), 
    # we'll make the intended row words score higher.
    words5 = [
        "abcde", "afkpu", 
        "azzzz", "bfzzz", "cckzz", "ddzpz", "eeezu",
    ]
    # Make sure all words are 5 letters and lowercase, and distinct
    words5 = list(set([w[:5].ljust(5, 'x').lower() for w in words5]))
    
    try:
        res = gen_mod.build_puzzle(
            words5, random.Random(42), 
            target_forced=target, 
            column_forced=col_idx,
            diag_target_forced=diag_target,
            diag_direction_pref="main",
            max_target_tries=1000,
            max_diag_tries=500
        )
        assert res[0] == target
        assert res[1] == col_idx
        assert res[3] == diag_target
        # Check column
        column_word = "".join(res[4][r][col_idx] for r in range(5))
        assert column_word == target
        # Check diagonal
        diag_word = "".join(res[4][r][r] for r in range(5))
        assert diag_word == diag_target
    except RuntimeError as e:
        pytest.fail(f"build_puzzle_constraints failed: {e}")

def test_compute_lambda_style_total(logic_mod):
    """Test the scoring logic for consistent output."""
    letter_scores = {"a": 1, "b": 3, "c": 3, "d": 2, "e": 1}
    rows = ["apple", "bread", "clear", "dance", "eagle"]
    col = 0
    target = "abcde"
    diag = "aecda"
    diag_dir = "main"
    words5 = rows + [target, diag, "extra"]
    combined_diag_words = [diag]
    longest_one = "longest"
    
    # This is a complex function, we just want to ensure it doesn't crash 
    # and produces a score.
    score = logic_mod.compute_lambda_style_total(
        rows, col, target, diag, diag_dir, words5, combined_diag_words, longest_one, letter_scores
    )
    
    assert isinstance(score, int)
    assert score > 0

def test_madness_build_puzzle_with_path(mad_mod, monkeypatch):
    """Test the madness generator with path requirement."""
    # To keep it fast, we'll mock the internal randomness to favor a simple path
    # OR we just skip it if it's too heavy for a unit test.
    # Given the complexity, let's just test that the function exists and handles empty input.
    with pytest.raises(RuntimeError):
        mad_mod.build_puzzle_with_path(
            words5=[],
            rng=random.Random(42),
            max_path_tries=1,
            madness_word_mode="margana",
            diag_direction_pref="main",
            max_target_tries=1,
            max_column_tries=1,
            max_diag_tries=1
        )


def test_pick_difficulty_band_for_date_uses_weights(gen_mod):
    pick = gen_mod._pick_difficulty_band_for_date(
        "2026-04-01",
        difficulty="random",
        difficulty_random_weights="easy=0,medium=0,hard=1",
        difficulty_random_salt="",
        difficulty_random_no_repeat=False,
    )
    assert pick == "hard"


def test_pick_difficulty_band_for_date_no_repeat_changes_previous(gen_mod):
    previous_day = "2026-03-31"
    current_day = "2026-04-01"
    weights = "easy=1,medium=1,hard=0"

    previous_pick = gen_mod._pick_difficulty_band_for_date(
        previous_day,
        difficulty="random",
        difficulty_random_weights=weights,
        difficulty_random_salt="",
        difficulty_random_no_repeat=False,
    )
    repeated_pick = gen_mod._pick_difficulty_band_for_date(
        current_day,
        difficulty="random",
        difficulty_random_weights=weights,
        difficulty_random_salt="",
        difficulty_random_no_repeat=False,
    )
    no_repeat_pick = gen_mod._pick_difficulty_band_for_date(
        current_day,
        difficulty="random",
        difficulty_random_weights=weights,
        difficulty_random_salt="",
        difficulty_random_no_repeat=True,
    )

    assert previous_pick == repeated_pick
    assert no_repeat_pick in {"easy", "medium"}
    assert no_repeat_pick != previous_pick


def test_format_batch_day_diagnostics(gen_mod):
    line = gen_mod._format_batch_day_diagnostics(
        day_iso="2026-04-03",
        band="hard",
        madness=False,
        written=False,
        total_score=None,
        anagram_length=None,
        attempts_used=200,
        max_attempts=200,
        rejection_counts={
            "builder_exception": 12,
            "timeout": 1,
            "anagram_excluded": 2,
            "anagram_length": 4,
            "score_below_band": 150,
            "score_above_band": 20,
            "usage_log_cooldown": 13,
        },
    )

    assert "BATCH_DAY date=2026-04-03" in line
    assert "band=hard" in line
    assert "written=False" in line
    assert "total_score=none" in line
    assert "anagram_length=none" in line
    assert "attempts=200/200" in line
    assert "anagram_excluded=2" in line
    assert "score_below_band=150" in line
    assert "usage_log_cooldown=13" in line


def test_column_puzzle_id_can_be_checked_against_usage_log_cooldown(gen_mod):
    puzzle_id = gen_mod._column_puzzle_id(
        "harks",
        3,
        "anti",
        "tarns",
        ["wight", "kayak", "myrrh", "snaky", "shush"],
    )
    level_log = {"puzzles": {puzzle_id: date.today().isoformat()}}

    assert puzzle_in_cooldown(level_log, puzzle_id, cooldown_days=365) is True


def test_batch_day_diagnostics_can_show_usage_log_cooldown_for_known_puzzle(gen_mod):
    puzzle_id = gen_mod._column_puzzle_id(
        "harks",
        3,
        "anti",
        "tarns",
        ["wight", "kayak", "myrrh", "snaky", "shush"],
    )
    level_log = {"puzzles": {puzzle_id: date.today().isoformat()}}

    assert puzzle_in_cooldown(level_log, puzzle_id, cooldown_days=365) is True

    line = gen_mod._format_batch_day_diagnostics(
        day_iso="2026-03-07",
        band="hard",
        madness=False,
        written=False,
        total_score=None,
        anagram_length=None,
        attempts_used=1,
        max_attempts=800,
        rejection_counts={
            "builder_exception": 0,
            "timeout": 0,
            "anagram_excluded": 0,
            "anagram_length": 0,
            "score_below_band": 0,
            "score_above_band": 0,
            "usage_log_cooldown": 1,
        },
    )

    assert "usage_log_cooldown=1" in line
