#!/usr/bin/env python3
"""Pattern-centric fixed-letter analysis for 5-letter words.

Generates all 1-fixed and 2-fixed letter patterns (a-z across all indices),
then counts how many words fit each pattern.
"""

from __future__ import annotations

from itertools import combinations, product
from pathlib import Path
import argparse
import string


def load_exclude_words(path: Path, length: int) -> set[str]:
    excludes: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        w = "".join(ch for ch in line.strip().lower() if ch.isalpha())
        if len(w) == length:
            excludes.add(w)
    return excludes


def load_words(path: Path, length: int, exclude_words: set[str] | None = None) -> list[str]:
    words: set[str] = set()
    exclude_words = exclude_words or set()
    for line in path.read_text(encoding="utf-8").splitlines():
        w = "".join(ch for ch in line.strip().lower() if ch.isalpha())
        if len(w) == length and w not in exclude_words:
            words.add(w)
    return sorted(words)


def pattern_string(length: int, pos_a: int, ch_a: str, pos_b: int | None = None, ch_b: str | None = None) -> str:
    slots = ["_"] * length
    slots[pos_a] = ch_a
    if pos_b is not None and ch_b is not None:
        slots[pos_b] = ch_b
    return " ".join(slots)


def count_for_pattern(words: list[str], pos_a: int, ch_a: str, pos_b: int | None = None, ch_b: str | None = None) -> tuple[int, list[str]]:
    if pos_b is None:
        matches = [w for w in words if w[pos_a] == ch_a]
    else:
        matches = [w for w in words if w[pos_a] == ch_a and w[pos_b] == ch_b]
    return len(matches), matches


def build_output(words: list[str], length: int, include_words: bool, include_zero: bool) -> list[str]:
    letters = string.ascii_lowercase
    rows: list[tuple[int, int, str, str, str]] = []

    # 1-fixed patterns
    for pos, ch in product(range(length), letters):
        cnt, matches = count_for_pattern(words, pos, ch)
        if cnt == 0 and not include_zero:
            continue
        patt = pattern_string(length, pos, ch)
        fit_words = ",".join(matches) if include_words else ""
        rows.append((cnt, 1, patt, fit_words, f"pos{pos}={ch}"))

    # 2-fixed patterns
    for pos_a, pos_b in combinations(range(length), 2):
        for ch_a, ch_b in product(letters, letters):
            cnt, matches = count_for_pattern(words, pos_a, ch_a, pos_b, ch_b)
            if cnt == 0 and not include_zero:
                continue
            patt = pattern_string(length, pos_a, ch_a, pos_b, ch_b)
            fit_words = ",".join(matches) if include_words else ""
            rows.append((cnt, 2, patt, fit_words, f"pos{pos_a}={ch_a},pos{pos_b}={ch_b}"))

    # Hardest -> easiest: smaller counts first
    rows.sort(key=lambda r: (r[0], r[1], r[2]))

    lines = [
        "# fixed\tpattern\tcount" + ("\twords" if include_words else ""),
        "# fixed=1 or 2; count = number of words fitting this pattern",
        "# sorted hardest->easiest (lowest count first)",
    ]
    for cnt, fixed, patt, fit_words, _ in rows:
        if include_words:
            lines.append(f"{fixed}\t{patt}\t{cnt}\t{fit_words}")
        else:
            lines.append(f"{fixed}\t{patt}\t{cnt}")
    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Count words per fixed-letter pattern.")
    parser.add_argument("--word-list", default="python/margana-word-list.txt", help="Path to source word list file.")
    parser.add_argument("--length", type=int, default=5, help="Target word length (default: 5).")
    parser.add_argument("--output", default="python/fixed-letter-difficulty-5.txt", help="Output report path.")
    parser.add_argument(
        "--exclude-words-file",
        default=None,
        help="Optional path to words to exclude from analysis (one word per line).",
    )
    parser.add_argument(
        "--include-words",
        action="store_true",
        help="Include matching words per pattern (large output).",
    )
    parser.add_argument(
        "--include-zero",
        action="store_true",
        help="Include impossible patterns (count=0). Default is playable only.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    exclude_words: set[str] = set()
    if args.exclude_words_file:
        exclude_words = load_exclude_words(Path(args.exclude_words_file), args.length)

    words = load_words(Path(args.word_list), args.length, exclude_words=exclude_words)
    lines = build_output(words, args.length, include_words=bool(args.include_words), include_zero=bool(args.include_zero))
    Path(args.output).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote: {args.output}")
    print(f"words={len(words)}")
    print(f"excluded_words={len(exclude_words)}")


if __name__ == "__main__":
    main()
