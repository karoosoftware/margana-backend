from __future__ import annotations

import importlib.util
import random
from pathlib import Path


def _load_madness_generator_module():
    mod_path = Path(__file__).resolve().parents[1] / "generate-column-puzzle-madness.py"
    spec = importlib.util.spec_from_file_location(
        "generate_column_puzzle_madness_for_tests",
        str(mod_path),
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def test_choose_rows_respects_horizontal_exclude_set_madness():
    mod = _load_madness_generator_module()

    words5 = [
        "aaaaa",
        "apple",
        "bbbbb",
        "bbxya",
        "ccccc",
        "cacaa",
        "ddddd",
        "daada",
        "eeeee",
        "eaaae",
        "abcde",
    ]
    excluded = {"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee"}

    rows = mod.choose_rows_for_column_and_diag(
        target_col="abcde",
        column=0,
        target_diag="abcde",
        words5=words5,
        rng=random.Random(7),
        diag_direction="main",
        horizontal_exclude_set=excluded,
    )

    assert rows is not None
    assert len(rows) == 5
    assert all(w not in excluded for w in rows)

    for r, w in enumerate(rows):
        assert w[0] == "abcde"[r]
        assert w[r] == "abcde"[r]


def test_forced_targets_can_still_use_excluded_words_madness():
    mod = _load_madness_generator_module()

    words5 = [
        "aaaaa",
        "apple",
        "bbbbb",
        "bbxya",
        "ccccc",
        "cacaa",
        "ddddd",
        "daada",
        "eeeee",
        "eaaae",
        "abcde",
    ]
    excluded = {"abcde", "aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee"}

    target, col, ddir, diag, rows = mod.build_puzzle(
        words5=words5,
        rng=random.Random(3),
        target_forced="abcde",
        column_forced=0,
        diag_target_forced="abcde",
        diag_direction_pref="main",
        max_target_tries=50,
        max_column_tries=5,
        max_diag_tries=50,
        horizontal_exclude_set=excluded,
    )

    assert target == "abcde"
    assert diag == "abcde"
    assert col == 0
    assert ddir == "main"
    assert all(w not in excluded for w in rows)
