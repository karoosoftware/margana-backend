from __future__ import annotations

from pathlib import Path

from margana_gen.generator_difficulty import band_for_total_score
from margana_gen.generator_resources import (
    load_letter_scores,
    resolve_generator_resource_paths,
)


def test_resolve_generator_resource_paths_uses_repo_root_and_usage_log_name():
    script_path = Path("/tmp/example/ecs/generate-column-puzzle.py")
    expected_root = script_path.resolve().parents[1]

    paths = resolve_generator_resource_paths(
        script_path=script_path,
        usage_log_filename="usage-log.json",
    )

    assert paths.resources_dir == expected_root
    assert paths.word_list_default == expected_root / "margana-word-list.txt"
    assert paths.horizontal_exclude_words == expected_root / "horizontal-exclude-words.txt"
    assert paths.letter_scores_file == expected_root / "letter-scores-v3.json"
    assert paths.usage_log_file == expected_root / "usage-log.json"


def test_load_letter_scores_normalizes_keys_and_filters_non_numeric_values(tmp_path):
    scores_path = tmp_path / "letter-scores-v3.json"
    scores_path.write_text('{"A": 1, "B": 2.0, "C": "x"}', encoding="utf-8")

    scores = load_letter_scores(scores_path)

    assert scores == {"a": 1, "b": 2}


def test_band_for_total_score_covers_all_bands_and_boundaries():
    assert band_for_total_score(160) == "easy"
    assert band_for_total_score(179) == "easy"
    assert band_for_total_score(180) == "medium"
    assert band_for_total_score(199) == "medium"
    assert band_for_total_score(200) == "hard"
    assert band_for_total_score(260) == "hard"
