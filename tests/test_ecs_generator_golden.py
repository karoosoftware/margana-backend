from __future__ import annotations

import importlib.util
import random
import string
from pathlib import Path

import pytest


def _load_module(filename: str):
    mod_path = Path(__file__).resolve().parents[1] / "ecs" / filename
    spec = importlib.util.spec_from_file_location(f"{filename.replace('-', '_')}_golden", str(mod_path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TEST_WORDS5 = list(
    dict.fromkeys(
        [
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
            "apply", "arena", "argue", "arise", "array",
            "aside", "asset", "audio", "audit", "avoid",
            "award", "aware", "awful", "backy", "badly",
            "baker", "bases", "basic", "basis",
        ]
    )
)

FIXED_LETTER_SCORES = {ch: idx for idx, ch in enumerate(string.ascii_lowercase, start=1)}

EXPECTED_CLASSIC_PUZZLE = (
    "agree",
    4,
    "anti",
    "angle",
    ["arena", "along", "eager", "alike", "eagle"],
)

EXPECTED_CLASSIC_TOTAL = 4756


@pytest.fixture
def gen_mod():
    return _load_module("generate-column-puzzle.py")


@pytest.fixture
def mad_mod():
    return _load_module("generate-column-puzzle-madness.py")


def test_generate_column_puzzle_golden_output(gen_mod, monkeypatch):
    monkeypatch.setattr(gen_mod, "LETTER_SCORES", FIXED_LETTER_SCORES)

    puzzle = gen_mod.build_puzzle(
        TEST_WORDS5,
        random.Random(42),
        max_target_tries=500,
        max_diag_tries=100,
    )

    total = gen_mod.compute_lambda_style_total(
        rows=puzzle[4],
        col=puzzle[1],
        target=puzzle[0],
        diag=puzzle[3],
        diag_dir=puzzle[2],
        words5=TEST_WORDS5,
        combined_diag_words=TEST_WORDS5,
        longest_one="radar",
    )

    assert puzzle == EXPECTED_CLASSIC_PUZZLE
    assert total == EXPECTED_CLASSIC_TOTAL


def test_generate_column_puzzle_madness_golden_output(mad_mod, monkeypatch):
    monkeypatch.setattr(mad_mod, "LETTER_SCORES", FIXED_LETTER_SCORES)

    puzzle = mad_mod.build_puzzle(
        TEST_WORDS5,
        random.Random(42),
        max_target_tries=500,
        max_diag_tries=100,
    )

    assert puzzle == EXPECTED_CLASSIC_PUZZLE
