#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from margana_gen.validation import PuzzleValidationContext, rules_for_preset, run_collection_validations, run_validations


def _load_payload(path_str: str) -> dict:
    path = Path(path_str).resolve()
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_word_set(path_str: str | None) -> set[str]:
    if not path_str:
        return set()
    path = Path(path_str).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Horizontal exclude file not found: {path}")
    words: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        word = line.strip().lower()
        if not word or word.startswith("#"):
            continue
        words.add(word)
    return words


def _discover_payload_sets(payload_dir: str) -> list[tuple[Path, Path | None]]:
    base = Path(payload_dir).resolve()
    if base.is_file():
        raise FileNotFoundError(f"{base} is a file; expected a directory")
    if not base.exists():
        raise FileNotFoundError(f"{base} does not exist")

    direct_completed = base / "margana-completed.json"
    if direct_completed.exists():
        direct_semi = base / "margana-semi-completed.json"
        return [(direct_completed, direct_semi if direct_semi.exists() else None)]

    payload_sets: list[tuple[Path, Path | None]] = []
    for completed_path in sorted(base.rglob("margana-completed.json")):
        parent = completed_path.parent
        semi_path = parent / "margana-semi-completed.json"
        payload_sets.append((completed_path, semi_path if semi_path.exists() else None))

    if not payload_sets:
        raise FileNotFoundError(f"No margana-completed.json files found under {base}")

    return payload_sets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate generated Margana puzzle payloads.")
    parser.add_argument("--payload-dir", required=True, help="Directory containing margana-completed.json and margana-semi-completed.json")
    parser.add_argument(
        "--preset",
        choices=["default", "strict"],
        default="default",
        help="Validation preset to run.",
    )
    parser.add_argument("--min-anagram-len", type=int, default=8, help="Minimum allowed longest-anagram length.")
    parser.add_argument("--max-anagram-len", type=int, default=10, help="Maximum allowed longest-anagram length.")
    parser.add_argument("--horizontal-exclude-file", default=None, help="Optional path to horizontal-exclude-words.txt")
    parser.add_argument("--anagram-exclude-file", default=None, help="Optional path to anagram-exclude-words.txt")
    parser.add_argument("--fail-on-warning", action="store_true", help="Return non-zero if warnings are present.")
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--verbose", action="store_true", help="Print every validation issue for every payload.")
    output_group.add_argument("--summary-only", action="store_true", help="Print only per-payload and final summaries.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload_sets = _discover_payload_sets(args.payload_dir)
    horizontal_exclude_words = _load_word_set(args.horizontal_exclude_file)
    anagram_exclude_words = _load_word_set(args.anagram_exclude_file)
    rules = rules_for_preset(
        args.preset,
        min_anagram_len=int(args.min_anagram_len),
        max_anagram_len=int(args.max_anagram_len),
    )

    total_issues = 0
    total_errors = 0
    total_warnings = 0
    contexts: list[PuzzleValidationContext] = []

    for completed_path, semi_completed_path in payload_sets:
        completed_payload = _load_payload(str(completed_path))
        semi_completed_payload = _load_payload(str(semi_completed_path)) if semi_completed_path else None
        ctx = PuzzleValidationContext(
            completed_payload=completed_payload,
            semi_completed_payload=semi_completed_payload,
            horizontal_exclude_words=horizontal_exclude_words,
            anagram_exclude_words=anagram_exclude_words,
        )
        contexts.append(ctx)
        result = run_validations(ctx, rules)

        issue_count = len(result.issues)
        error_count = sum(1 for issue in result.issues if issue.level == "error")
        warning_count = sum(1 for issue in result.issues if issue.level == "warning")
        total_issues += issue_count
        total_errors += error_count
        total_warnings += warning_count

        print(
            f"{completed_path.parent}: preset={args.preset} issues={issue_count} "
            f"errors={error_count} warnings={warning_count}"
        )
        if args.verbose:
            for issue in result.issues:
                print(f"[{issue.level}] {issue.code}: {issue.message}")

    collection_result = run_collection_validations(contexts)
    collection_issue_count = len(collection_result.issues)
    collection_error_count = sum(1 for issue in collection_result.issues if issue.level == "error")
    collection_warning_count = sum(1 for issue in collection_result.issues if issue.level == "warning")
    total_issues += collection_issue_count
    total_errors += collection_error_count
    total_warnings += collection_warning_count
    if args.verbose:
        for issue in collection_result.issues:
            print(f"[{issue.level}] {issue.code}: {issue.message}")

    print(
        f"Validation completed: payloads={len(payload_sets)} issues={total_issues} "
        f"errors={total_errors} warnings={total_warnings}"
    )

    if total_errors > 0:
        return 1
    if args.fail_on_warning and total_warnings > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
