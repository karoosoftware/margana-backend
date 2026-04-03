from __future__ import annotations

import json
from pathlib import Path

from margana_gen import generator_bootstrap


def test_load_horizontal_exclude_words_filters_comments_and_non_five_letter_words(tmp_path):
    exclude_file = tmp_path / "horizontal-exclude-words.txt"
    exclude_file.write_text("# comment\nviews\nqueue\nbad!\nlonger\n", encoding="utf-8")

    result = generator_bootstrap.load_horizontal_exclude_words(exclude_file)

    assert result == {"views", "queue"}


def test_ensure_words_file_returns_existing_local_path(tmp_path):
    words_file = tmp_path / "margana-word-list.txt"
    words_file.write_text("alpha\n", encoding="utf-8")

    result = generator_bootstrap.ensure_words_file(
        words_path=words_file,
        bucket="unused",
        key="unused",
        allow_s3_download=True,
    )

    assert result == words_file.resolve()


def test_ensure_words_file_raises_when_missing_and_s3_disabled(tmp_path):
    missing_file = tmp_path / "missing.txt"

    try:
        generator_bootstrap.ensure_words_file(
            words_path=missing_file,
            bucket="unused",
            key="unused",
            allow_s3_download=False,
        )
    except FileNotFoundError as exc:
        assert str(missing_file.resolve()) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_load_usage_log_with_optional_s3_sync_reads_local_file_without_sync(tmp_path):
    usage_file = tmp_path / "usage-log.json"
    usage_file.write_text(json.dumps({"column_puzzle": {"puzzles": {"abc": "2026-01-01"}}}), encoding="utf-8")

    result = generator_bootstrap.load_usage_log_with_optional_s3_sync(
        bucket="unused",
        key="unused",
        usage_log_path=usage_file,
        sync_from_s3=False,
    )

    assert result == {"column_puzzle": {"puzzles": {"abc": "2026-01-01"}}}


def test_save_usage_log_with_optional_s3_sync_writes_local_file(tmp_path):
    usage_file = tmp_path / "usage-log.json"
    usage_log = {"column_puzzle": {"puzzles": {"abc": "2026-01-01"}}}

    generator_bootstrap.save_usage_log_with_optional_s3_sync(
        usage_log=usage_log,
        usage_log_path=usage_file,
        bucket="unused",
        key="unused",
        sync_to_s3=False,
    )

    assert json.loads(usage_file.read_text(encoding="utf-8")) == usage_log
