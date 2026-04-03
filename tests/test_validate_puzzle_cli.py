from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _load_cli_module():
    root = Path(__file__).resolve().parents[1]
    mod_path = root / "ecs" / "validate-puzzle.py"
    spec = importlib.util.spec_from_file_location("validate_puzzle_cli", str(mod_path))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_validate_puzzle_cli_accepts_valid_payload(tmp_path):
    cli = _load_cli_module()
    root = Path(__file__).resolve().parents[1]
    payload_dir = root / "tests" / "fixtures" / "payloads" / "sample-day"

    exit_code = cli.main(["--payload-dir", str(payload_dir)])

    assert exit_code == 0


def test_validate_puzzle_cli_recursively_finds_payloads():
    cli = _load_cli_module()
    root = Path(__file__).resolve().parents[1]
    payload_root = root / "tests" / "fixtures" / "payloads"

    exit_code = cli.main(["--payload-dir", str(payload_root)])

    assert exit_code == 0


def test_validate_puzzle_cli_accepts_valid_complete_week_fixture():
    cli = _load_cli_module()
    root = Path(__file__).resolve().parents[1]
    payload_root = root / "tests" / "fixtures" / "weekly-validation" / "valid-week"

    exit_code = cli.main(["--payload-dir", str(payload_root), "--summary-only"])

    assert exit_code == 0


def test_validate_puzzle_cli_rejects_invalid_payload(tmp_path):
    cli = _load_cli_module()
    completed = tmp_path / "margana-completed.json"
    _write_json(
        completed,
        {
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
                {"word": "gaudy", "type": "column", "index": 0, "direction": "tb", "base_score": 10, "bonus": 0, "score": 10}
            ],
            "total_score": 10,
        },
    )

    exit_code = cli.main(["--payload-dir", str(tmp_path)])

    assert exit_code == 1


def test_validate_puzzle_cli_rejects_horizontal_exclude_words(tmp_path):
    cli = _load_cli_module()
    completed = tmp_path / "margana-completed.json"
    exclude = tmp_path / "horizontal-exclude-words.txt"
    _write_json(
        completed,
        {
            "meta": {"rows": 5, "cols": 5, "wordLength": 5},
            "grid_rows": ["views", "vodka", "lager", "queue", "furze"],
            "valid_words": {"rows": {"lr": ["views", "vodka", "lager", "queue", "furze"], "rl": []}},
            "valid_words_metadata": [],
            "total_score": 0,
        },
    )
    exclude.write_text("views\n", encoding="utf-8")

    exit_code = cli.main(["--payload-dir", str(tmp_path), "--horizontal-exclude-file", str(exclude)])

    assert exit_code == 1


def test_validate_puzzle_cli_verbose_prints_issue_details(tmp_path, capsys):
    cli = _load_cli_module()
    completed = tmp_path / "margana-completed.json"
    _write_json(
        completed,
        {
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
                {"word": "gaudy", "type": "column", "index": 0, "direction": "tb", "base_score": 10, "bonus": 0, "score": 10}
            ],
            "total_score": 10,
        },
    )

    exit_code = cli.main(["--payload-dir", str(tmp_path), "--verbose"])
    out = capsys.readouterr().out

    assert exit_code == 1
    assert "[error] fixed_target_word_scored:" in out


def test_validate_puzzle_cli_summary_only_suppresses_issue_details(tmp_path, capsys):
    cli = _load_cli_module()
    completed = tmp_path / "margana-completed.json"
    _write_json(
        completed,
        {
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
                {"word": "gaudy", "type": "column", "index": 0, "direction": "tb", "base_score": 10, "bonus": 0, "score": 10}
            ],
            "total_score": 10,
        },
    )

    exit_code = cli.main(["--payload-dir", str(tmp_path), "--summary-only"])
    out = capsys.readouterr().out

    assert exit_code == 1
    assert "[error] fixed_target_word_scored:" not in out
    assert "Validation completed:" in out


@pytest.mark.parametrize(
    ("folder_name", "expected_snippet"),
    [
        ("fixed-target-scored", "fixed_target_word_scored"),
        ("non-zero-bonus", "non_zero_bonus"),
        ("semi-mismatch", "semi_completed_visible_letter_mismatch"),
    ],
)
def test_validate_puzzle_cli_failing_example_fixtures(folder_name, expected_snippet, capsys):
    cli = _load_cli_module()
    root = Path(__file__).resolve().parents[1]
    payload_dir = root / "tests" / "fixtures" / "validation-examples" / folder_name

    exit_code = cli.main(["--payload-dir", str(payload_dir), "--verbose"])
    out = capsys.readouterr().out

    assert exit_code == 1
    assert expected_snippet in out


def test_validate_puzzle_cli_rejects_invalid_complete_week_fixture(capsys):
    cli = _load_cli_module()
    root = Path(__file__).resolve().parents[1]
    payload_root = root / "tests" / "fixtures" / "weekly-validation" / "invalid-week"

    exit_code = cli.main(["--payload-dir", str(payload_root), "--verbose"])
    out = capsys.readouterr().out

    assert exit_code == 1
    assert "weekly_madness_count_mismatch" in out
