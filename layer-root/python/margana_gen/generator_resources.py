from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


WORDLIST_S3_KEY_DEFAULT = "word-lists/margana-word-list.txt"


@dataclass(frozen=True)
class GeneratorResourcePaths:
    resources_dir: Path
    word_list_default: Path
    horizontal_exclude_words: Path
    letter_scores_file: Path
    usage_log_file: Path


def resolve_generator_resource_paths(*, script_path: Path, usage_log_filename: str) -> GeneratorResourcePaths:
    resources_dir = script_path.parents[1].resolve()
    return GeneratorResourcePaths(
        resources_dir=resources_dir,
        word_list_default=resources_dir / "margana-word-list.txt",
        horizontal_exclude_words=resources_dir / "horizontal-exclude-words.txt",
        letter_scores_file=resources_dir / "letter-scores-v3.json",
        usage_log_file=(resources_dir / usage_log_filename).resolve(),
    )


def load_letter_scores(scores_path: Path) -> dict[str, int]:
    try:
        with open(scores_path, "r", encoding="utf-8") as scores_file:
            raw_scores = json.load(scores_file)
    except Exception:
        raw_scores = {}
    return {
        str(key).lower(): int(value)
        for key, value in raw_scores.items()
        if isinstance(value, (int, float))
    }
