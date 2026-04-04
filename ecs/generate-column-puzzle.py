#!/usr/bin/env python3
"""
Generate a new puzzle type:
- Pick a 5-letter target word from the word list (resources/margana-word-list.txt).
- Create 5 row words (each length 5) such that, when placed in a 5x5 grid,
  one of the columns (0..4) spells out the target word top-to-bottom.

This script reuses the existing word loading utilities in margana_gen.word_graph.
It outputs a JSON file and prints the same JSON to stdout.

Example:
  python3 scripts/generate-column-puzzle.py --seed 42
  python3 scripts/generate-column-puzzle.py --target apple --column 2
  python3 scripts/generate-column-puzzle.py --output resources/column-puzzle.json

  source python/.venv/bin/activate
  python3 ecs/generate-column-puzzle.py --environment preprod --year 2026 --iso-week 6 --diag-direction random --madness-word both --max-path-tries 400 --max-target-tries 300 --max-diag-tries 200 --use-s3-path-layout

"""
from __future__ import annotations

import argparse
import json
import random
import time
import calendar
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import List, Optional, Tuple, Dict
import os

# Reuse existing module for word loading
from margana_gen.word_graph import load_words, longest_constructible_words, constructible_words_min_length
from margana_gen.usage_log import load_usage_log, save_usage_log, puzzle_in_cooldown, record_puzzle
from margana_gen.s3_utils import download_usage_log_from_s3, upload_usage_log_to_s3, download_word_list_from_s3
from margana_gen import column_logic
from margana_gen.generator_bootstrap import (
    ensure_words_file,
    load_anagram_exclude_words,
    load_horizontal_exclude_words,
    load_usage_log_with_optional_s3_sync,
    save_usage_log_with_optional_s3_sync,
)
from margana_gen.generator_difficulty import DIFFICULTY_BANDS
from margana_gen.completed_payload_builder import build_completed_payload
from margana_gen.generator_resources import (
    WORDLIST_S3_KEY_DEFAULT,
    load_letter_scores,
    resolve_generator_resource_paths,
)
from margana_gen.generator_scoring import compute_basic_total, make_score_word
from margana_gen.payload_io import write_payload_pair
from margana_gen.semi_completed_builder import (
    build_semi_completed_payload,
    shuffle_word_deterministic,
)
from margana_gen.valid_word_items_builder import build_valid_word_items
from margana_gen.valid_words_builder import build_valid_words_map
from margana_gen.valid_words_metadata_builder import (
    build_valid_words_metadata,
    enrich_valid_words_metadata,
)
from margana_score import remove_pre_loaded_words

# Optional import of madness builder from sibling file (path-based to handle hyphen in filename)
import importlib.util as _importlib_util

_SCRIPT_PATH = Path(__file__).resolve()
_MADNESS_PATH = (_SCRIPT_PATH.parent / "generate-column-puzzle-madness.py").resolve()
_gen_mad_spec = _importlib_util.spec_from_file_location("gen_madness_module", str(_MADNESS_PATH))
_gen_mad_mod = None
if _gen_mad_spec is not None and _gen_mad_spec.loader is not None:
    _gen_mad_mod = _importlib_util.module_from_spec(_gen_mad_spec)
    _gen_mad_spec.loader.exec_module(_gen_mad_mod)  # type: ignore

build_puzzle_with_path = getattr(_gen_mad_mod, "build_puzzle_with_path", None)

# ---------- PATHS ----------
SCRIPT_PATH = Path(__file__).resolve()
RESOURCE_PATHS = resolve_generator_resource_paths(
    script_path=SCRIPT_PATH,
    usage_log_filename="margana-puzzle-usage-log.json",
)
RESOURCES_DIR = RESOURCE_PATHS.resources_dir
WORD_LIST_DEFAULT = RESOURCE_PATHS.word_list_default
WORDLIST_HORIZONTAL_EXCLUDE = RESOURCE_PATHS.horizontal_exclude_words
WORDLIST_ANAGRAM_EXCLUDE = RESOURCE_PATHS.anagram_exclude_words
USAGE_LOG_FILE = RESOURCE_PATHS.usage_log_file
USAGE_S3_KEY_DEFAULT = "usage-logs/margana-puzzle-usage-log.json"

LETTER_SCORES = load_letter_scores(RESOURCE_PATHS.letter_scores_file)

ANAGRAM_LEN_DEFAULTS = {"min": 8, "max": 10}

# ---------- DEBUG HELPERS ----------
# Simple global flags and helpers to print the generator's internal search when requested.
DEBUG_ENABLED = False
DEBUG_VERBOSE = False

def dbg(msg: str, *, verbose: bool = False):
    try:
        if not DEBUG_ENABLED:
            return
        if verbose and not DEBUG_VERBOSE:
            return
        print(f"[DEBUG] {msg}")
    except Exception:
        # never crash on debug output
        pass


def _parse_difficulty_random_weights(weights: str) -> dict[str, int]:
    parsed = {"easy": 2, "medium": 4, "hard": 3}
    try:
        for part in str(weights or "").split(","):
            if not part.strip():
                continue
            key, value = part.split("=")
            key = key.strip()
            value_int = int(value.strip())
            if key in parsed and value_int >= 0:
                parsed[key] = value_int
    except Exception:
        pass
    return parsed


def _pick_difficulty_band_for_date(
    day_iso: str,
    *,
    difficulty: str,
    difficulty_random_weights: str,
    difficulty_random_salt: str,
    difficulty_random_no_repeat: bool,
) -> str | None:
    if difficulty != "random":
        return difficulty if difficulty in DIFFICULTY_BANDS else None

    seed_key = f"{day_iso}|{difficulty_random_salt}"
    rnd = random.Random(seed_key)
    weights = _parse_difficulty_random_weights(difficulty_random_weights)
    pool = (
        ["easy"] * int(weights.get("easy", 0))
        + ["medium"] * int(weights.get("medium", 0))
        + ["hard"] * int(weights.get("hard", 0))
    )
    if not pool:
        pool = ["easy", "medium", "hard"]

    pick = rnd.choice(pool)
    if difficulty_random_no_repeat:
        try:
            previous_day = (date.fromisoformat(day_iso) - timedelta(days=1)).isoformat()
            previous_pick = random.Random(f"{previous_day}|{difficulty_random_salt}").choice(pool)
            if previous_pick == pick and len(set(pool)) > 1:
                alternative = pick
                tries = 0
                while alternative == pick and tries < 5:
                    alternative = rnd.choice(pool)
                    tries += 1
                pick = alternative
        except Exception:
            pass

    return pick


def _format_batch_day_diagnostics(
    *,
    day_iso: str,
    band: str | None,
    madness: bool,
    written: bool,
    total_score: int | None,
    anagram_length: int | None,
    attempts_used: int,
    max_attempts: int,
    rejection_counts: dict[str, int],
) -> str:
    ordered_keys = [
        "builder_exception",
        "timeout",
        "anagram_excluded",
        "anagram_length",
        "score_below_band",
        "score_above_band",
        "usage_log_cooldown",
    ]
    counts = " ".join(f"{key}={int(rejection_counts.get(key, 0))}" for key in ordered_keys)
    return (
        f"BATCH_DAY date={day_iso}"
        f" band={(band or 'none')}"
        f" madness={bool(madness)}"
        f" written={bool(written)}"
        f" total_score={(total_score if total_score is not None else 'none')}"
        f" anagram_length={(anagram_length if anagram_length is not None else 'none')}"
        f" attempts={attempts_used}/{max_attempts}"
        f" {counts}"
    )


# -----------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a 5x5 column-word puzzle JSON.")
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility.")
    p.add_argument("--environment", type=str, default="dev", choices=["dev", "preprod", "prod"],
                   help="Environment that you are working on, or 'dev' to choose automatically (default).")
    p.add_argument(
        "--words-file", type=str, default=str(WORD_LIST_DEFAULT),
        help="Path to the word list file (default: python/margana-word-list.txt).",
    )
    p.add_argument(
        "--target", type=str, default=None,
        help="Optional 5-letter target word to force for the column (must exist in the word list).",
    )
    p.add_argument(
        "--column", type=int, default=None, choices=[0, 1, 2, 3, 4],
        help="Optional column index (0-4) that must spell the target; default: choose automatically.",
    )
    p.add_argument(
        "--diag-target", type=str, default=None,
        help="Optional 5-letter diagonal target word (along the chosen diagonal). If omitted, the script will search for one.",
    )
    p.add_argument(
        "--diag-direction", type=str, choices=["main", "anti", "random"], default="random",
        help="Diagonal direction: 'main' (a1->e5), 'anti' (a5->e1), or 'random' to choose automatically (default).",
    )

    # Madness integration controls
    p.add_argument("--require-madness", action="store_true",
                   help="Force generation of a Margana Madness puzzle (path-first builder).")
    p.add_argument("--madness-policy", type=str, default="auto", choices=["auto", "off", "force"],
                   help="auto: ensure quota by emitting madness when needed; off: never unless required; force: always madness.")
    p.add_argument("--madness-window-days", type=int, default=14,
                   help="Rolling window size for madness quota (default: 14 for biweekly).")
    p.add_argument("--madness-min-per-window", type=int, default=1,
                   help="Minimum madness puzzles required in each rolling window (default: 1).")
    p.add_argument("--madness-word", type=str, default="margana", choices=["margana", "anagram", "both"],
                   help="Which madness word(s) to try when path-first (default: margana).")
    p.add_argument("--max-path-tries", type=int, default=400,
                   help="Max 8-neighbor paths to try for the madness word (default: 400).")
    p.add_argument("--per-run-seconds", type=float, default=None,
                   help="Optional hard timeout per generation attempt (seconds).")
    p.add_argument(
        "--max-row-backtrack-visits", type=int, default=50000,
        help="Hard cap for CSP row backtracking visits in Madness builder (default: 50000).",
    )

    # Usage/cooldown controls
    p.add_argument(
        "--cooldown-days", type=int, default=1826,
        help="Days before a column puzzle can be reused (default: 1826, ~5 years).",
    )
    p.add_argument(
        "--max-usage-tries", type=int, default=50,
        help="Attempts to find a non-repeated puzzle before giving up (default: 50).",
    )
    p.add_argument("--no-s3-usage", action="store_true",
                   help="Skip syncing usage log with S3 (no download, no upload).")
    p.add_argument("--s3-bucket", type=str, default="margana-word-game",
                   help="Override S3 bucket for usage log.")
    p.add_argument("--usage-s3-key", type=str, default=USAGE_S3_KEY_DEFAULT,
                   help="Override S3 key for usage log.")
    # Batch generation flags
    p.add_argument("--year", type=int, default=None, help="Year for batch generation (ISO week or month mode)")
    p.add_argument("--iso-week", type=int, default=None, help="ISO week number (1-53) for batch generation")
    p.add_argument("--month", type=int, default=None, help="Calendar month (1-12) for batch generation")
    p.add_argument("--on-exist", type=str, default="fail", choices=["fail", "skip", "overwrite"],
                   help="Behavior when a day's output folder already exists (default: fail)")
    p.add_argument("--output-root", type=str, default=str((RESOURCES_DIR / "tmp" / "payloads").resolve()),
                   help="Root folder to write batch outputs into")
    p.add_argument("--use-s3-path-layout", action="store_true",
                   help="When set in batch mode, write outputs under <output-root>/<s3-path-prefix>/<YYYY>/<MM>/<DD>/...")
    p.add_argument("--s3-path-prefix", type=str, default="public/daily-puzzles",
                   help="Prefix to use for S3-style path layout (default: public/daily-puzzles)")
    p.add_argument("--madness-count", type=int, default=4,
                   help="Default number of Madness days in a month (month mode)")
    p.add_argument("--madness-random-salt", type=str, default="",
                   help="Optional salt for deterministic randomness in batch allocation")
    p.add_argument("--madness-dates", type=str, default=None,
                   help="Comma-separated YYYY-MM-DD dates to force Madness in batch mode; overrides defaults")
    p.add_argument(
        "--max-target-tries", type=int, default=500,
        help="Max different column targets to try before giving up (when --target not specified).",
    )
    p.add_argument(
        "--max-column-tries", type=int, default=5,
        help="Max columns to try per column target when --column not specified.",
    )
    p.add_argument(
        "--max-diag-tries", type=int, default=200,
        help="Max different diagonal targets to try per (column target, column) when --diag-target not specified.",
    )

    # Debugging options
    p.add_argument("--debug", action="store_true",
                   help="Enable debug output showing the generator's search process.")
    p.add_argument("--debug-verbose", action="store_true",
                   help="Enable verbose debug output (includes candidate lists and scores).")

    # Difficulty gating (soft filters, optional)
    p.add_argument("--difficulty", type=str, default="random",
                   choices=["easy", "medium", "hard", "random"],
                   help="Difficulty band to enforce (default: random). Options: easy, medium, hard, or random.")
    p.add_argument("--min-total-score", type=int, default=None,
                   help="If set, require total_score >= this value (soft reject otherwise).")
    p.add_argument("--max-total-score", type=int, default=None,
                   help="If set, require total_score <= this value (soft reject otherwise).")
    p.add_argument("--min-anagram-len", type=int, default=ANAGRAM_LEN_DEFAULTS["min"],
                   help="Minimum longest-anagram length to accept (default 7).")
    p.add_argument("--max-anagram-len", type=int, default=ANAGRAM_LEN_DEFAULTS["max"],
                   help="Maximum longest-anagram length to accept (default 10).")
    p.add_argument("--difficulty-random-weights", type=str, default="easy=2,medium=4,hard=3",
                   help="Weights for --difficulty random, e.g. 'easy=2,medium=4,hard=3'.")
    p.add_argument("--difficulty-random-no-repeat", action="store_true",
                   help="When using --difficulty random, try to avoid repeating the same band as the previous day.")
    p.add_argument("--difficulty-random-salt", type=str, default="",
                   help="Optional salt string mixed into the deterministic random band picker for --difficulty random.")

    return p.parse_args()

def load_horizontal_exclude_set() -> set[str]:
    exclude_path = Path(WORDLIST_HORIZONTAL_EXCLUDE)
    horizontal_exclude_set = load_horizontal_exclude_words(exclude_path)
    if not exclude_path.exists():
        dbg(f"horizontal exclude file not found at {exclude_path}; continuing with no excludes")
        return horizontal_exclude_set
    dbg(f"loaded {len(horizontal_exclude_set)} horizontal exclude words from {exclude_path}")
    return horizontal_exclude_set


def load_anagram_exclude_set() -> set[str]:
    exclude_path = Path(WORDLIST_ANAGRAM_EXCLUDE)
    anagram_exclude_set = load_anagram_exclude_words(exclude_path)
    if not exclude_path.exists():
        dbg(f"anagram exclude file not found at {exclude_path}; continuing with no excludes")
        return anagram_exclude_set
    dbg(f"loaded {len(anagram_exclude_set)} anagram exclude words from {exclude_path}")
    return anagram_exclude_set

def choose_rows_for_column_and_diag(
        target_col: str,
        column: int,
        target_diag: str,
        words5: List[str],
        rng: random.Random,
        diag_direction: str,
        horizontal_exclude_set: set[str],
) -> Optional[List[str]]:
    return column_logic.choose_rows_for_column_and_diag(
        target_col=target_col,
        column=column,
        target_diag=target_diag,
        words5=words5,
        rng=rng,
        diag_direction=diag_direction,
        horizontal_exclude_set=horizontal_exclude_set,
    )


def build_puzzle(
        words5: List[str],
        rng: random.Random,
        target_forced: Optional[str] = None,
        column_forced: Optional[int] = None,
        diag_target_forced: Optional[str] = None,
        diag_direction_pref: str = "random",
        max_target_tries: int = 500,
        max_column_tries: int = 5,
        max_diag_tries: int = 200,
        horizontal_exclude_set: Optional[set[str]] = None,
) -> Tuple[str, int, str, str, List[str]]:
    return column_logic.build_puzzle(
        words5=words5,
        rng=rng,
        target_forced=target_forced,
        column_forced=column_forced,
        diag_target_forced=diag_target_forced,
        diag_direction_pref=diag_direction_pref,
        max_target_tries=max_target_tries,
        max_column_tries=max_column_tries,
        max_diag_tries=max_diag_tries,
        horizontal_exclude_set=horizontal_exclude_set,
    )


import hashlib


def _column_puzzle_id(vertical_target: str, column: int, diag_dir: str, diag_target: str, rows: List[str]) -> str:
    base = f"V:{vertical_target}|C:{column}|D:{diag_dir}|T:{diag_target}|R:{'>'.join(rows)}"
    dbg(f"  base='{base}'")
    return hashlib.sha1(base.encode('utf-8')).hexdigest()[:10]


def _column_layout_id(vertical_target: str, column: int, diag_dir: str, diag_target: str) -> str:
    """
    Stable ID for the underlying layout (targets and positions) without the specific row words.
    Mirrors the chain_id concept from the main generator.
    """
    base = f"V:{vertical_target}|C:{column}|D:{diag_dir}|T:{diag_target}"
    return hashlib.sha1(base.encode('utf-8')).hexdigest()[:10]


# Global target for anagram bonus (length of the hidden longest anagram)
ANAGRAM_TARGET_LEN = 0

# Configurable anagram bonus points (default 20) — mirror Lambda env
try:
    ANAGRAM_BONUS_POINTS = int(os.getenv("MARGANA_ANAGRAM_BONUS", "20"))
except Exception:
    ANAGRAM_BONUS_POINTS = 20


def bonus_for_valid_word(item: dict) -> int:
    """
    Align with lambda_margana_results._bonus_for_valid_word:
    - Fixed anagram bonus policy: award 20 points when the submitted anagram uses exactly the
      hidden longest anagram letter count (target length). Otherwise 0.
    - Never reveal or depend on the actual anagram string.
    Always return an int.
    """
    try:
        t = str(item.get("type") or "").strip().lower()
        if t != "anagram":
            return 0
        w = str(item.get("word") or "").strip()
        n = int(ANAGRAM_TARGET_LEN or 0)
        if n <= 0:
            return 0
        # Fixed anagram bonus via env: award configured points when the submitted anagram matches the target length
        return int(ANAGRAM_BONUS_POINTS) if len(w) == n else 0
    except Exception:
        return 0


def compute_lambda_style_total(rows: List[str], col: int, target: str, diag: str, diag_dir: str, words5: List[str],
                               combined_diag_words: List[str], longest_one: str) -> int:
    return column_logic.compute_lambda_style_total(
        rows=rows,
        col=col,
        target=target,
        diag=diag,
        diag_dir=diag_dir,
        words5=words5,
        combined_diag_words=combined_diag_words,
        longest_one=longest_one,
        letter_scores=LETTER_SCORES,
    )

def main():
    args = parse_args()

    # Initialize debug configuration
    global DEBUG_ENABLED, DEBUG_VERBOSE
    DEBUG_ENABLED = bool(getattr(args, 'debug', False))
    DEBUG_VERBOSE = bool(getattr(args, 'debug_verbose', False))
    if DEBUG_ENABLED:
        dbg("Debugging enabled" + (" (verbose)" if DEBUG_VERBOSE else ""))

    rng = random.Random(args.seed)

    s3_word_bucket = f"margana-word-game-{args.environment}"

    # Ensure words file exists; if not, try to fetch from S3
    words_path = ensure_words_file(
        words_path=Path(args.words_file),
        bucket=s3_word_bucket,
        key=WORDLIST_S3_KEY_DEFAULT,
        allow_s3_download=True,
        logger=dbg,
    )

    words_by_len, _all = load_words(str(words_path))
    words5 = words_by_len.get(5, [])

    horizontal_exclude_set = load_horizontal_exclude_set()
    anagram_exclude_set = load_anagram_exclude_set()

    dbg(f"loaded words: len2={len(words_by_len.get(2, []))} len3={len(words_by_len.get(3, []))} len4={len(words_by_len.get(4, []))} len5={len(words5)}")
    # Also prepare combined dictionary for diagonal edge-to-edge detection (3-5 letters)
    words3 = words_by_len.get(3, [])
    words4 = words_by_len.get(4, [])
    _combined_diag_words = list(set([w.lower() for w in (words3 + words4 + words5)]))

    # Usage log: optionally download from S3, then load
    usage_log = load_usage_log_with_optional_s3_sync(
        bucket=s3_word_bucket,
        key=args.usage_s3_key,
        usage_log_path=USAGE_LOG_FILE,
        sync_from_s3=not args.no_s3_usage,
        logger=dbg,
    )
    if DEBUG_ENABLED and (not isinstance(usage_log, dict) or not usage_log):
        dbg("usage log missing/empty or invalid JSON -> initialized to empty {}")
    dbg(f"usage log loaded: keys={list(usage_log.keys())}")
    # Ensure a dedicated section for column puzzles exists
    column_log = usage_log.setdefault("column_puzzle", {"puzzles": {}})
    dbg(f"existing column_puzzle entries={len(column_log.get('puzzles', {}))}")

    # ===== Batch generation helpers (ISO week / month) =====
    # Minimal helpers to build valid_words and metadata for batch outputs
    def _simple_valid_words_metadata(rows_in: List[str]) -> List[dict]:
        items: List[dict] = []
        for rr in range(5):
            w = rows_in[rr]
            item = {
                "word": w,
                "type": "row",
                "index": rr,
                "direction": "lr",
                "start_index": {"r": rr, "c": 0},
                "end_index": {"r": rr, "c": 4},
                "palindrome": (w == w[::-1] and len(w) > 0),
                "semordnilap": False,
                # Provide basic letter info; scoring is captured via total_score separately
                "letters": [ch for ch in w.lower()],
                "letter_scores": [0 for _ in w],
                "letter_sum": 0,
                "base_score": 0,
                "bonus": 0,
                "score": 0,
            }
            items.append(item)
        return items

    def _simple_build_valid_words_map(rows_in: List[str]) -> dict:
        return {
            "rows": {"lr": [r for r in rows_in], "rl": []},
            "columns": {"tb": [], "bt": []},
            "diagonals": {"main": [], "main_rev": [], "anti": [], "anti_rev": []},
            "anagram": [],
        }

    def _dates_for_iso_week(y: int, w: int) -> List[date]:
        # ISO weeks: Monday=1 .. Sunday=7
        try:
            return [date.fromisocalendar(y, w, d) for d in range(1, 8)]
        except Exception as e:
            raise ValueError(f"Invalid ISO week: year={y} week={w}: {e}")

    def _dates_for_month(y: int, m: int) -> List[date]:
        if m < 1 or m > 12:
            raise ValueError("--month must be between 1 and 12")
        _, ndays = calendar.monthrange(y, m)
        return [date(y, m, d) for d in range(1, ndays + 1)]

    def _fmt_ddmmyyyy(d: date) -> str:
        return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"

    def _deterministic_pick(rng_key: str, choices: List[date], k: int) -> List[date]:
        r = random.Random(rng_key)
        picks = choices[:]
        r.shuffle(picks)
        return picks[: max(0, min(k, len(picks)))]

    # --- Margana Madness helpers (defined early so they are available in both batch and single-day paths) ---
    def _find_madness_path_local(grid_rows: List[str], word: str) -> List[Tuple[int, int]]:
        """8-neighbor DFS to find a contiguous path that spells `word` over `grid_rows`.
        Returns list of (r,c) or [] if not found. Mirrors the Lambda’s search.
        """
        try:
            target = (word or "").strip().lower()
            if not target:
                return []
            R = len(grid_rows)
            if R == 0:
                return []
            C = len(grid_rows[0])
            dirs = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
            visited = [[False] * C for _ in range(R)]
            path: List[Tuple[int, int]] = []

            def dfs(r: int, c: int, i: int) -> bool:
                if r < 0 or r >= R or c < 0 or c >= C:
                    return False
                if visited[r][c]:
                    return False
                if grid_rows[r][c].lower() != target[i]:
                    return False
                visited[r][c] = True
                path.append((r, c))
                if i == len(target) - 1:
                    return True
                for dr, dc in dirs:
                    if dfs(r + dr, c + dc, i + 1):
                        return True
                path.pop()
                visited[r][c] = False
                return False

            for r in range(R):
                for c in range(C):
                    if grid_rows[r][c].lower() == target[0]:
                        if dfs(r, c, 0):
                            return list(path)
            return []
        except Exception:
            return []

    def _append_madness_item_to_metadata(
            grid_rows: List[str],
            items: List[dict],
            letter_scores: Dict[str, int],
            *,
            madness_available: bool,
            madness_word: Optional[str],
            madness_path: Optional[List[Tuple[int, int]]],
    ) -> Optional[dict]:
        """
        Append a `type: "madness"` item to `items` when available. Returns a dict with
        derived madness meta fields (madnessFound, madnessWord, madnessPath, madnessScore)
        that you may merge into `meta` if desired; otherwise returns None.

        Notes:
        - Scoring (base_score/bonus/score) is not set here; it will be filled by the
          existing scoring loop (generator keeps bonus = 0).
        - Uses provided `madness_path` when valid; otherwise tries to find a path for
          "margana" or "anagram" locally.
        """
        try:
            if not madness_available:
                return {"madnessFound": False}

            word = (madness_word or "").strip().lower() or None
            coords: Optional[List[Tuple[int, int]]] = None

            # Prefer provided path when valid
            if madness_path and isinstance(madness_path, list) and word:
                try:
                    # Ensure elements are tuples (r, c)
                    coords = [(int(r), int(c)) for (r, c) in madness_path]
                except Exception:
                    coords = None

            # Fallback: try to discover locally if path/word missing
            if not coords or not word:
                for candidate in ("margana", "anagram"):
                    path = _find_madness_path_local(grid_rows, candidate)
                    if path:
                        coords = path
                        word = candidate
                        break

            if not coords or not word:
                return {"madnessFound": False}

            # Don’t duplicate if already present
            already = any((str(it.get("type") or "").lower() == "madness") for it in items)
            # Precompute letters and letter_sum for convenience
            letters_seq = [grid_rows[r][c] for (r, c) in coords]
            letter_scores_seq = [int(letter_scores.get(ch.lower(), 0)) for ch in letters_seq]
            letter_sum = int(sum(letter_scores_seq))

            if not already:
                item = {
                    "word": str(word).lower(),
                    "type": "madness",
                    "index": 0,
                    "direction": "path",
                    "coords": [{"r": int(r), "c": int(c)} for (r, c) in coords],
                    "start_index": {"r": int(coords[0][0]), "c": int(coords[0][1])} if coords else None,
                    "end_index": {"r": int(coords[-1][0]), "c": int(coords[-1][1])} if coords else None,
                    # Pre-fill breakdowns (scoring loop will still recompute aggregates uniformly)
                    "letters": [ch.lower() for ch in letters_seq],
                    "letter_scores": letter_scores_seq,
                    "letter_sum": letter_sum,
                }
                items.append(item)

            # Return meta fields for optional merge
            return {
                "madnessFound": True,
                "madnessWord": str(word).lower(),
                "madnessPath": [[int(r), int(c)] for (r, c) in coords],
                # Keep this aligned with generator semantics: sum of path letter scores only
                "madnessScore": int(letter_sum),
            }
        except Exception:
            # Never break the pipeline because of madness integration
            return {"madnessFound": False}

    def _build_final_scored_results(
            *,
            grid_rows: List[str],
            words5_local: List[str],
            combined_diag_words_local: List[str],
            longest_anagram: str,
            target_word: str,
            column_index: int,
            diagonal_word: str,
            diagonal_direction: str,
            letter_scores_local: Dict[str, int],
            score_word_local,
            madness_available: bool,
            madness_word: Optional[str],
            madness_path: Optional[List[Tuple[int, int]]],
            diagonal_lengths: set[int],
    ) -> tuple[List[dict], dict, int]:
        valid_words_metadata = build_valid_words_metadata(
            build_valid_word_items(
                grid_rows=grid_rows,
                words5=words5_local,
                diagonal_words=combined_diag_words_local,
                diagonal_lengths=diagonal_lengths,
                include_coordinates=True,
            ),
            longest_anagram=longest_anagram,
        )
        exclusion_meta = {
            "wordLength": 5,
            "columnIndex": column_index,
            "diagonalDirection": diagonal_direction,
            "verticalTargetWord": target_word,
            "diagonalTargetWord": diagonal_word,
        }
        valid_words_metadata = remove_pre_loaded_words(exclusion_meta, valid_words_metadata)
        _append_madness_item_to_metadata(
            grid_rows=grid_rows,
            items=valid_words_metadata,
            letter_scores=letter_scores_local,
            madness_available=bool(madness_available),
            madness_word=madness_word,
            madness_path=madness_path,
        )
        enrich_valid_words_metadata(
            valid_words_metadata,
            letter_scores=letter_scores_local,
            score_word=score_word_local,
            semordnilap_words=combined_diag_words_local,
        )
        lambda_total_score = sum(int(it.get("score") or 0) for it in valid_words_metadata)
        valid_words_map = build_valid_words_map(valid_words_metadata, combined_diag_words_local)
        return valid_words_metadata, valid_words_map, lambda_total_score

    # Determine if batch mode requested
    batch_year = getattr(args, 'year', None)
    batch_week = getattr(args, 'iso_week', None)
    batch_month = getattr(args, 'month', None)
    if batch_year is not None and ((batch_week is not None) ^ (batch_month is not None)):
        # Build date list
        if batch_week is not None:
            dates_list = _dates_for_iso_week(int(batch_year), int(batch_week))
            # Exactly 1 madness by default; allow override via --madness-dates
            override = str(getattr(args, 'madness_dates', '') or '')
            if override.strip():
                madness_set = {x.strip() for x in override.split(',') if x.strip()}
            else:
                key = f"{int(batch_year)}-W{int(batch_week):02d}|{getattr(args, 'madness_random_salt', '')}"
                chosen = _deterministic_pick(key, dates_list, 1)
                madness_set = {d.isoformat() for d in chosen}
            out_root = Path(getattr(args, 'output_root')) / f"{int(batch_year):04d}-W{int(batch_week):02d}"
        else:
            m = int(batch_month)
            dates_list = _dates_for_month(int(batch_year), m)
            override = str(getattr(args, 'madness_dates', '') or '')
            if override.strip():
                madness_set = {x.strip() for x in override.split(',') if x.strip()}
            else:
                # One Madness day per ISO week in the month (default up to 4)
                count = int(getattr(args, 'madness_count', 4))
                salt = str(getattr(args, 'madness_random_salt', ''))
                # Group dates by ISO week number
                weeks_map = {}
                for _d in dates_list:
                    wk = _d.isocalendar()[1]
                    weeks_map.setdefault(wk, []).append(_d)
                madness_picks = []
                for wk in sorted(weeks_map.keys()):
                    if count is not None and len(madness_picks) >= count:
                        break
                    week_days = weeks_map[wk][:]
                    # Deterministic shuffle per week using year-month-week and optional salt
                    rnd = random.Random(f"{int(batch_year)}-{m:02d}-W{int(wk):02d}|{salt}")
                    rnd.shuffle(week_days)
                    madness_picks.append(week_days[0])
                madness_set = {d.isoformat() for d in madness_picks}
            out_root = Path(getattr(args, 'output_root')) / f"{int(batch_year):04d}-{m:02d}"

        on_exist = str(getattr(args, 'on_exist', 'fail'))
        out_root.mkdir(parents=True, exist_ok=True)

        # Batch summary counters
        batch_start = time.perf_counter()
        total_days = 0
        written = 0
        skipped = 0
        madness_days = []
        bands_used = {"easy": 0, "medium": 0, "hard": 0, "skipped": 0}

        # Prepare shared helpers from outer scope (words5, usage_log, column_log etc.)
        for d in dates_list:
            total_days += 1
            day_iso = d.isoformat()
            if bool(getattr(args, 'use_s3_path_layout', False)):
                # Write under <output-root>/<s3-path-prefix>/<YYYY>/<MM>/<DD>/
                prefix = str(getattr(args, 's3_path_prefix', 'public/daily-puzzles')).strip('/')
                y = f"{d.year:04d}"
                m = f"{d.month:02d}"
                day = f"{d.day:02d}"
                day_dir = (Path(getattr(args, 'output_root')) / prefix / y / m / day)
            else:
                day_dir = out_root / day_iso
            if day_dir.exists():
                if on_exist == 'fail':
                    raise FileExistsError(f"Output already exists for {day_iso}: {day_dir}")
                elif on_exist == 'skip':
                    skipped += 1
                    continue
                elif on_exist == 'overwrite':
                    # Clear directory
                    for pth in day_dir.glob('*'):
                        try:
                            pth.unlink()
                        except Exception:
                            pass
            day_dir.mkdir(parents=True, exist_ok=True)

            # For this day, force madness if allocated
            force_mad_this_day = (day_iso in madness_set)
            if force_mad_this_day:
                madness_days.append(day_iso)

            selected_band_for_day = None
            base_band_min_for_day = getattr(args, 'min_total_score', None)
            base_band_max_for_day = getattr(args, 'max_total_score', None)
            try:
                if (not force_mad_this_day) and base_band_min_for_day is None and base_band_max_for_day is None:
                    selected_band_for_day = _pick_difficulty_band_for_date(
                        day_iso,
                        difficulty=str(getattr(args, "difficulty", "random")),
                        difficulty_random_weights=str(getattr(args, "difficulty_random_weights", "")),
                        difficulty_random_salt=str(getattr(args, "difficulty_random_salt", "")),
                        difficulty_random_no_repeat=bool(getattr(args, "difficulty_random_no_repeat", False)),
                    )
                    if selected_band_for_day:
                        b = DIFFICULTY_BANDS.get(selected_band_for_day, {})
                        base_band_min_for_day = b.get('min_score')
                        base_band_max_for_day = b.get('max_score')
            except Exception:
                selected_band_for_day = None

            # ----- Single-day generation (inline, minimal duplication) -----
            # Configure per-run timeout if provided
            _timer_set = False
            _timeout_seconds = float(getattr(args, 'per_run_seconds', 0) or 0)

            class _RunTimeout(Exception):
                pass

            def _timeout_handler(signum, frame):
                raise _RunTimeout()

            attempt = 0
            built = False
            max_usage_tries = int(getattr(args, 'max_usage_tries', 50))
            min_len = int(getattr(args, 'min_anagram_len', ANAGRAM_LEN_DEFAULTS['min']))
            max_len = int(getattr(args, 'max_anagram_len', ANAGRAM_LEN_DEFAULTS['max']))
            accepted_total_score: int | None = None
            accepted_anagram_length: int | None = None
            rejection_counts = {
                "builder_exception": 0,
                "timeout": 0,
                "anagram_excluded": 0,
                "anagram_length": 0,
                "score_below_band": 0,
                "score_above_band": 0,
                "usage_log_cooldown": 0,
            }
            while attempt < max_usage_tries and not built:
                attempt += 1
                # Choose builder
                use_madness = bool(build_puzzle_with_path) and bool(force_mad_this_day)
                # Set watchdog
                _timed_out_attempt = False
                try:
                    if _timeout_seconds > 0:
                        import signal
                        try:
                            signal.signal(signal.SIGALRM, _timeout_handler)
                            signal.setitimer(signal.ITIMER_REAL, _timeout_seconds)
                            _timer_set = True
                        except Exception:
                            _timer_set = False

                    if use_madness:
                        diag_pref = args.diag_direction if getattr(args, 'diag_direction', None) else 'main'
                        mw = str(getattr(args, 'madness_word', 'margana'))
                        ttries = int(getattr(args, 'max_target_tries', 500))
                        ctries = int(getattr(args, 'max_column_tries', 5))
                        dtries = int(getattr(args, 'max_diag_tries', 200))
                        ptries = int(getattr(args, 'max_path_tries', 400))
                        max_row_visits = int(getattr(args, 'max_row_backtrack_visits', 0) or 0)
                        kwargs = dict(
                            words5=words5,
                            rng=rng,
                            max_path_tries=ptries,
                            madness_word_mode=mw,
                            diag_direction_pref=diag_pref,
                            max_target_tries=ttries,
                            max_column_tries=ctries,
                            max_diag_tries=dtries,
                            horizontal_exclude_set=horizontal_exclude_set,
                        )
                        if max_row_visits > 0:
                            kwargs["max_row_backtrack_visits"] = max_row_visits
                        try:
                            target, col, diag_dir, diag, rows, _path, _mad_word = build_puzzle_with_path(**kwargs)
                        except Exception:
                            rejection_counts["builder_exception"] += 1
                            continue
                        _was_mad = True
                        _mad_path = list(_path) if _path else None
                        _mad_word = str(_mad_word) if _mad_word else None
                    else:
                        target, col, diag_dir, diag, rows = column_logic.build_puzzle(
                            words5=words5,
                            rng=rng,
                            target_forced=args.target,
                            column_forced=args.column,
                            diag_target_forced=args.diag_target,
                            diag_direction_pref=args.diag_direction,
                            max_target_tries=args.max_target_tries,
                            max_column_tries=args.max_column_tries,
                            max_diag_tries=args.max_diag_tries,
                            horizontal_exclude_set=horizontal_exclude_set,
                        )
                        _was_mad = False
                        _mad_path = None
                        _mad_word = None
                except RuntimeError:
                    rejection_counts["builder_exception"] += 1
                    continue
                except _RunTimeout:
                    _timed_out_attempt = True
                finally:
                    if _timer_set:
                        try:
                            import signal
                            signal.setitimer(signal.ITIMER_REAL, 0)
                        except Exception:
                            pass
                if _timed_out_attempt:
                    rejection_counts["timeout"] += 1
                    continue

                # Gates and totals
                anagram_pool_try = "".join(rows)
                longest_candidates_try = longest_constructible_words(anagram_pool_try, _all)
                rows_set_try = set(rows)
                longest_candidates_try = [
                    w for w in longest_candidates_try
                    if w not in rows_set_try and w not in anagram_exclude_set
                ]

                def _pick_longest_upto10_try(cands: List[str]) -> str:
                    cands = [w for w in cands if len(w) <= 10]
                    if not cands:
                        return ""
                    max_len = max(len(w) for w in cands)
                    pool = [w for w in cands if len(w) == max_len]
                    rng.shuffle(pool)
                    return pool[0]

                longest_one_try = _pick_longest_upto10_try(longest_candidates_try)
                if not longest_one_try:
                    all_constructible_try = constructible_words_min_length(anagram_pool_try, _all, 1)
                    upto10_unfiltered_try = [
                        w for w in all_constructible_try
                        if len(w) <= 10 and w not in rows_set_try
                    ]
                    upto10_try = [
                        w for w in upto10_unfiltered_try
                        if w not in anagram_exclude_set
                    ]
                    if upto10_try:
                        max_len = max(len(w) for w in upto10_try)
                        pool = [w for w in upto10_try if len(w) == max_len]
                        rng.shuffle(pool)
                        longest_one_try = pool[0]
                    else:
                        if upto10_unfiltered_try:
                            rejection_counts["anagram_excluded"] += 1
                            continue
                        longest_one_try = ""

                _letter_scores_local = LETTER_SCORES
                _score_word_local = make_score_word(_letter_scores_local)
                valid_words_metadata, valid_words_map, lambda_total_score = _build_final_scored_results(
                    grid_rows=rows,
                    words5_local=words5,
                    combined_diag_words_local=_combined_diag_words,
                    longest_anagram=longest_one_try,
                    target_word=target,
                    column_index=col,
                    diagonal_word=diag,
                    diagonal_direction=diag_dir,
                    letter_scores_local=_letter_scores_local,
                    score_word_local=_score_word_local,
                    madness_available=bool(_was_mad),
                    madness_word=_mad_word,
                    madness_path=_mad_path,
                    diagonal_lengths={3, 4, 5},
                )

                # Apply minimal gates consistent with current args
                # Resolve difficulty band per DAY in batch mode
                band_min = base_band_min_for_day
                band_max = base_band_max_for_day
                picked_band = selected_band_for_day
                try:
                    # Madness days: keep the spirit of no band gating, but enforce a safety floor
                    # so totals don't drop below the easy-band lower bound. Do NOT clear any
                    # explicit max if provided by CLI.
                    if _was_mad:
                        picked_band = None
                        # Derive the easy-band floor directly from DIFFICULTY_BANDS,
                        # so changes to the table propagate here automatically.
                        _easy_floor = DIFFICULTY_BANDS.get('easy', {}).get('min_score')
                        try:
                            _easy_floor = int(_easy_floor) if _easy_floor is not None else None
                        except Exception:
                            _easy_floor = None
                        # Honor any explicit min but never allow below the easy floor (if available)
                        if _easy_floor is not None:
                            if band_min is None:
                                band_min = _easy_floor
                            else:
                                try:
                                    band_min = max(int(band_min), _easy_floor)
                                except Exception:
                                    band_min = _easy_floor
                        # Keep band_max as-is (may be None or explicitly set on CLI)
                except Exception:
                    # On any error, fall back to whatever explicit min/max were set
                    pass

                if len(longest_one_try) < min_len or len(longest_one_try) > max_len:
                    rejection_counts["anagram_length"] += 1
                    continue
                if band_min is not None and lambda_total_score < int(band_min):
                    rejection_counts["score_below_band"] += 1
                    continue
                if band_max is not None and lambda_total_score > int(band_max):
                    rejection_counts["score_above_band"] += 1
                    continue

                # Cooldown and record using this day's date
                pid_try = _column_puzzle_id(target, col, diag_dir, diag, rows)
                if puzzle_in_cooldown(column_log, pid_try, args.cooldown_days):
                    rejection_counts["usage_log_cooldown"] += 1
                    continue
                record_puzzle(column_log, pid_try, day_iso)
                if _was_mad:
                    column_log.setdefault('madness_puzzles', {})[pid_try] = day_iso

                # Build payload documents with full scoring metadata (match single-day output)
                layout_id = _column_layout_id(target, col, diag_dir, diag)
                longest_one = longest_one_try

                longest_one = longest_one_try

                # Madness score via builder scoring if available
                _madness_score = None
                try:
                    if _was_mad and _mad_word:
                        _score_fn = getattr(_gen_mad_mod, "_score_word", None)
                        if callable(_score_fn):
                            _madness_score = int(_score_fn(_mad_word))
                except Exception:
                    _madness_score = None

                # Determine difficulty band metadata for payloads
                try:
                    _diff_type = str(getattr(args, 'difficulty', 'random'))
                except Exception:
                    _diff_type = 'random'
                if bool(_was_mad):
                    _diff_applied = 'skipped'
                else:
                    if _diff_type == 'random':
                        _diff_applied = selected_band_for_day if selected_band_for_day else 'random'
                    else:
                        _diff_applied = _diff_type
                # Track band usage for batch summary
                try:
                    if _diff_applied in ('easy', 'medium', 'hard'):
                        bands_used[_diff_applied] = int(bands_used.get(_diff_applied, 0)) + 1
                    elif _diff_applied == 'skipped':
                        bands_used['skipped'] = int(bands_used.get('skipped', 0)) + 1
                except Exception:
                    pass

                completed_doc = build_completed_payload(
                    saved_at=datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
                    date=day_iso,
                    vertical_target_word=target,
                    column_index=col,
                    diagonal_direction=diag_dir,
                    diagonal_target_word=diag,
                    rows=rows,
                    longest_anagram=longest_one,
                    valid_words=valid_words_map,
                    valid_words_metadata=valid_words_metadata,
                    total_score=lambda_total_score,
                    meta_extra={
                        "madnessAvailable": bool(_was_mad),
                        "madnessWord": (_mad_word if _was_mad else None),
                        "madnessDirection": "forward" if _was_mad else None,
                        "madnessPath": (_mad_path if _was_mad else None),
                        "madnessScore": (_madness_score if (_was_mad and _madness_score is not None) else None),
                        "difficultyBandType": _diff_type,
                        "difficultyBandApplied": _diff_applied,
                    },
                )

                longest_anagram_shuffled = None
                if longest_one:
                    longest_anagram_shuffled = shuffle_word_deterministic(
                        longest_one.lower(),
                        f"{layout_id}|{day_iso}|{longest_one}",
                    )

                semi_output = build_semi_completed_payload(
                    date=day_iso,
                    puzzle_id=pid_try,
                    layout_id=layout_id,
                    vertical_target_word=target,
                    column_index=col,
                    diagonal_direction=diag_dir,
                    diagonal_target_word=diag,
                    rows=rows,
                    longest_anagram_count=len(longest_one),
                    longest_anagram_shuffled=longest_anagram_shuffled,
                    extra_fields={
                        "madnessAvailable": bool(_was_mad),
                        "difficultyBandType": _diff_type,
                        "difficultyBandApplied": _diff_applied,
                    },
                )

                write_payload_pair(
                    day_dir,
                    completed_payload=completed_doc,
                    semi_completed_payload=semi_output,
                )

                accepted_total_score = int(lambda_total_score)
                accepted_anagram_length = len(longest_one)
                written += 1
                built = True
                # End while attempts

            print(
                _format_batch_day_diagnostics(
                    day_iso=day_iso,
                    band=("skipped" if force_mad_this_day else selected_band_for_day),
                    madness=force_mad_this_day,
                    written=built,
                    total_score=accepted_total_score,
                    anagram_length=accepted_anagram_length,
                    attempts_used=attempt,
                    max_attempts=max_usage_tries,
                    rejection_counts=rejection_counts,
                )
            )

        # End for each day
        # Save usage once and optionally upload
        save_usage_log_with_optional_s3_sync(
            usage_log=usage_log,
            usage_log_path=USAGE_LOG_FILE,
            bucket=s3_word_bucket,
            key=args.usage_s3_key,
            sync_to_s3=not args.no_s3_usage,
        )

        # Print summary and return
        duration_seconds = float(max(0.0, time.perf_counter() - batch_start))
        # Human format like 1m23.456s or 0.123s
        if duration_seconds >= 60.0:
            minutes = int(duration_seconds // 60)
            seconds = duration_seconds - (minutes * 60)
            duration_human = f"{minutes}m{seconds:06.3f}s"
        else:
            duration_human = f"{duration_seconds:.3f}s"
        print(json.dumps({
            "mode": "batch",
            "year": int(batch_year),
            "iso_week": (int(batch_week) if batch_week is not None else None),
            "month": (int(batch_month) if batch_month is not None else None),
            "output_root": str(out_root.resolve()),
            "days": total_days,
            "written": written,
            "skipped": skipped,
            "madness_days": madness_days,
            "bands_used": bands_used,
            "duration_seconds": round(duration_seconds, 3),
            "duration_human": duration_human,
        }, indent=2))
        return

    # Preview values to carry from accepted candidate to final payload
    _precomputed_longest_one: Optional[str] = None
    _precomputed_valid_words_metadata: Optional[List[dict]] = None
    _precomputed_valid_words_map: Optional[dict] = None
    _precomputed_lambda_total_score: Optional[int] = None
    _accepted_total: Optional[int] = None

    # Madness policy helpers
    def _today_iso_from_args(a) -> str:
        # Simplified: always use today's date (puzzle-date flag removed)
        return datetime.now(timezone.utc).date().isoformat()

    def _parse_date(d: str):
        try:
            return datetime.fromisoformat(d).date()
        except Exception:
            return None

    def _should_emit_madness(now_iso: str) -> bool:
        # Overrides
        if bool(getattr(args, 'require_madness', False)):
            return True
        pol = str(getattr(args, 'madness_policy', 'auto'))
        if pol == 'force':
            return True
        if pol == 'off':
            return False
        # auto policy
        window_days = int(getattr(args, 'madness_window_days', 14) or 14)
        min_per = int(getattr(args, 'madness_min_per_window', 1) or 1)
        if window_days <= 0 or min_per <= 0:
            return False
        # Count entries in madness_puzzles within [now - window_days + 1, now]
        mad_map = column_log.setdefault('madness_puzzles', {})
        now_d = _parse_date(now_iso)
        if not now_d:
            return False
        cutoff = now_d - timedelta(days=window_days - 1)
        cnt = 0
        for _pid, date_iso in list(mad_map.items()):
            if isinstance(date_iso, dict):
                # tolerate object form {date:..., madness:true}
                date_val = date_iso.get('date')
            else:
                date_val = date_iso
            dd = _parse_date(str(date_val))
            if dd and dd >= cutoff and dd <= now_d:
                cnt += 1
        return cnt < min_per

    # Try building until we find a puzzle not in cooldown
    # Track madness metadata for payload
    _was_madness: bool = False
    _madness_path: Optional[List[Tuple[int, int]]] = None
    _madness_word: Optional[str] = None

    # Helper to validate the madness path in the final grid (8-neighbor, no reuse)
    def _validate_madness_path(grid_rows: List[str], path: List[Tuple[int, int]], word: str) -> bool:
        try:
            if not path or not word or len(path) != len(word):
                return False
            R = C = 5
            used = set()
            for i, (r, c) in enumerate(path):
                if not (0 <= r < R and 0 <= c < C):
                    return False
                if (r, c) in used:
                    return False
                if grid_rows[r][c].lower() != word[i].lower():
                    return False
                if i > 0:
                    pr, pc = path[i - 1]
                    if abs(pr - r) > 1 or abs(pc - c) > 1:
                        return False
                used.add((r, c))
            return True
        except Exception:
            return False

    from typing import Optional, Tuple

    def _find_madness_path_local(grid_rows: List[str], word: str) -> List[Tuple[int, int]]:
        """8-neighbor DFS to find a contiguous path that spells `word` over `grid_rows`.
        Returns list of (r,c) or [] if not found. Mirrors the Lambda’s search.
        """
        try:
            target = (word or "").strip().lower()
            if not target:
                return []
            R = len(grid_rows)
            if R == 0:
                return []
            C = len(grid_rows[0])
            dirs = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
            visited = [[False] * C for _ in range(R)]
            path: List[Tuple[int, int]] = []

            def dfs(r: int, c: int, i: int) -> bool:
                if r < 0 or r >= R or c < 0 or c >= C:
                    return False
                if visited[r][c]:
                    return False
                if grid_rows[r][c].lower() != target[i]:
                    return False
                visited[r][c] = True
                path.append((r, c))
                if i == len(target) - 1:
                    return True
                for dr, dc in dirs:
                    if dfs(r + dr, c + dc, i + 1):
                        return True
                path.pop()
                visited[r][c] = False
                return False

            for r in range(R):
                for c in range(C):
                    if grid_rows[r][c].lower() == target[0]:
                        if dfs(r, c, 0):
                            return list(path)
            return []
        except Exception:
            return []

    def _append_madness_item_to_metadata(
            grid_rows: List[str],
            items: List[dict],
            letter_scores: Dict[str, int],
            *,
            madness_available: bool,
            madness_word: Optional[str],
            madness_path: Optional[List[Tuple[int, int]]],
    ) -> Optional[dict]:
        """
        Append a `type: "madness"` item to `items` when available. Returns a dict with
        derived madness meta fields (madnessFound, madnessWord, madnessPath, madnessScore)
        that you may merge into `meta` if desired; otherwise returns None.

        Notes:
        - Scoring (base_score/bonus/score) is not set here; it will be filled by the
          existing scoring loop (generator keeps bonus = 0).
        - Uses provided `madness_path` when valid; otherwise tries to find a path for
          "margana" or "anagram" locally.
        """
        try:
            if not madness_available:
                return {"madnessFound": False}

            word = (madness_word or "").strip().lower() or None
            coords: Optional[List[Tuple[int, int]]] = None

            # Prefer provided path when valid
            if madness_path and isinstance(madness_path, list) and word:
                try:
                    # Ensure elements are tuples (r, c)
                    coords = [(int(r), int(c)) for (r, c) in madness_path]
                except Exception:
                    coords = None

            # Fallback: try to discover locally if path/word missing
            if not coords or not word:
                for candidate in ("margana", "anagram"):
                    path = _find_madness_path_local(grid_rows, candidate)
                    if path:
                        coords = path
                        word = candidate
                        break

            if not coords or not word:
                return {"madnessFound": False}

            # Don’t duplicate if already present
            already = any((str(it.get("type") or "").lower() == "madness") for it in items)
            # Precompute letters and letter_sum for convenience
            letters_seq = [grid_rows[r][c] for (r, c) in coords]
            letter_scores_seq = [int(letter_scores.get(ch.lower(), 0)) for ch in letters_seq]
            letter_sum = int(sum(letter_scores_seq))

            if not already:
                item = {
                    "word": str(word).lower(),
                    "type": "madness",
                    "index": 0,
                    "direction": "path",
                    "coords": [{"r": int(r), "c": int(c)} for (r, c) in coords],
                    "start_index": {"r": int(coords[0][0]), "c": int(coords[0][1])} if coords else None,
                    "end_index": {"r": int(coords[-1][0]), "c": int(coords[-1][1])} if coords else None,
                    # Pre-fill breakdowns (scoring loop will still recompute aggregates uniformly)
                    "letters": [ch.lower() for ch in letters_seq],
                    "letter_scores": letter_scores_seq,
                    "letter_sum": letter_sum,
                }
                items.append(item)

            # Return meta fields for optional merge
            return {
                "madnessFound": True,
                "madnessWord": str(word).lower(),
                "madnessPath": [[int(r), int(c)] for (r, c) in coords],
                # Keep this aligned with generator semantics: sum of path letter scores only
                "madnessScore": int(letter_sum),
            }
        except Exception:
            # Never break the pipeline because of madness integration
            return {"madnessFound": False}

    attempt = 0
    while True:
        attempt += 1
        dbg(f"outer attempt {attempt}/{args.max_usage_tries}")
        if attempt > args.max_usage_tries:
            raise RuntimeError("Exceeded --max-usage-tries while searching for a non-repeated column puzzle.")

        # Decide whether to generate a Madness puzzle for this attempt
        today_iso_for_policy = _today_iso_from_args(args)
        force_mad = bool(getattr(args, 'require_madness', False))
        if force_mad and not build_puzzle_with_path:
            raise RuntimeError("--require-madness was provided but the madness builder is unavailable.")
        use_madness = bool(build_puzzle_with_path) and (force_mad or _should_emit_madness(today_iso_for_policy))
        dbg(f"policy: require_madness={force_mad} policy={getattr(args, 'madness_policy', 'auto')} window_days={getattr(args, 'madness_window_days', 14)} min_per={getattr(args, 'madness_min_per_window', 1)} -> use_madness={use_madness}")

        # Optional per-run timeout watchdog (Unix): enforce args.per_run_seconds if provided
        _timer_set = False
        _timeout_seconds = float(getattr(args, 'per_run_seconds', 0) or 0)

        class _RunTimeout(Exception):
            pass

        def _timeout_handler(signum, frame):
            raise _RunTimeout()

        _timed_out_attempt = False
        try:
            if _timeout_seconds > 0:
                import signal  # local import to avoid Windows issues if imported globally
                try:
                    signal.signal(signal.SIGALRM, _timeout_handler)
                    signal.setitimer(signal.ITIMER_REAL, _timeout_seconds)
                    _timer_set = True
                except Exception:
                    _timer_set = False

            if use_madness:
                # Prefer fixed diag if provided; otherwise use arg diag-direction
                diag_pref = args.diag_direction if getattr(args, 'diag_direction', None) else 'main'
                mw = str(getattr(args, 'madness_word', 'margana'))
                ttries = int(getattr(args, 'max_target_tries', 500))
                ctries = int(getattr(args, 'max_column_tries', 5))
                dtries = int(getattr(args, 'max_diag_tries', 200))
                ptries = int(getattr(args, 'max_path_tries', 400))
                # Optional cap for row backtracking visits if the builder supports it
                max_row_visits = int(getattr(args, 'max_row_backtrack_visits', 0) or 0)
                kwargs = dict(
                    words5=words5,
                    rng=rng,
                    max_path_tries=ptries,
                    madness_word_mode=mw,
                    diag_direction_pref=diag_pref,
                    max_target_tries=ttries,
                    max_column_tries=ctries,
                    max_diag_tries=dtries,
                    horizontal_exclude_set=horizontal_exclude_set,
                )
                # Thread-through if the builder signature supports it
                try:
                    if max_row_visits > 0:
                        kwargs["max_row_backtrack_visits"] = max_row_visits
                except Exception:
                    pass
                dbg(f"builder: invoking madness builder (mw={mw}, diag={diag_pref}, paths={ptries}, ttries={ttries}, dtries={dtries})")
                try:
                    target, col, diag_dir, diag, rows, _path, _mad_word = build_puzzle_with_path(**kwargs)
                except Exception as e:
                    # When madness is required or selected, do not fall back to classic; try the next attempt
                    dbg(f"madness builder failed: {e} -> retry next outer attempt")
                    continue
                _was_madness = True
                _madness_path = list(_path) if _path else None
                _madness_word = str(_mad_word) if _mad_word else None
                # Validate the path against the produced grid; if it doesn't match, retry next attempt
                if _madness_path and _madness_word:
                    if not _validate_madness_path(rows, _madness_path, _madness_word):
                        dbg("reject: madness path validation failed -> retry")
                        # reset and try another attempt
                        _was_madness = False
                        _madness_path = None
                        _madness_word = None
                        continue
            else:
                target, col, diag_dir, diag, rows = column_logic.build_puzzle(
                    words5=words5,
                    rng=rng,
                    target_forced=args.target,
                    column_forced=args.column,
                    diag_target_forced=args.diag_target,
                    diag_direction_pref=args.diag_direction,
                    max_target_tries=args.max_target_tries,
                    max_column_tries=args.max_column_tries,
                    max_diag_tries=args.max_diag_tries,
                    horizontal_exclude_set=horizontal_exclude_set,
                )
                _was_madness = False
        except _RunTimeout:
            _timed_out_attempt = True
        finally:
            if _timer_set:
                try:
                    import signal
                    signal.setitimer(signal.ITIMER_REAL, 0)
                except Exception:
                    pass

        # If this attempt timed out, skip to next outer attempt
        if _timed_out_attempt:
            dbg("attempt timeout: retrying next outer attempt")
            continue

        # ---- Precompute longest anagram (<=10) exactly once for this candidate ----
        anagram_pool_try = "".join(rows)
        longest_candidates_try = longest_constructible_words(anagram_pool_try, _all)
        rows_set_try = set(rows)
        longest_candidates_try = [
            w for w in longest_candidates_try
            if w not in rows_set_try and w not in anagram_exclude_set
        ]

        def _pick_longest_upto10_try(cands: List[str]) -> str:
            cands = [w for w in cands if len(w) <= 10]
            if not cands:
                return ""
            max_len = max(len(w) for w in cands)
            pool = [w for w in cands if len(w) == max_len]
            rng.shuffle(pool)
            return pool[0]

        longest_one_try = _pick_longest_upto10_try(longest_candidates_try)
        if not longest_one_try:
            all_constructible_try = constructible_words_min_length(anagram_pool_try, _all, 1)
            upto10_try = [
                w for w in all_constructible_try
                if len(w) <= 10 and w not in rows_set_try and w not in anagram_exclude_set
            ]
            if upto10_try:
                max_len = max(len(w) for w in upto10_try)
                pool = [w for w in upto10_try if len(w) == max_len]
                rng.shuffle(pool)
                longest_one_try = pool[0]
            else:
                longest_one_try = ""

        score_word_for_gating = make_score_word(LETTER_SCORES)
        candidate_valid_words_metadata, candidate_valid_words_map, candidate_lambda_total_score = _build_final_scored_results(
            grid_rows=rows,
            words5_local=words5,
            combined_diag_words_local=_combined_diag_words,
            longest_anagram=longest_one_try,
            target_word=target,
            column_index=col,
            diagonal_word=diag,
            diagonal_direction=diag_dir,
            letter_scores_local=LETTER_SCORES,
            score_word_local=score_word_for_gating,
            madness_available=bool(_was_madness),
            madness_word=_madness_word,
            madness_path=_madness_path,
            diagonal_lengths={2, 3, 4, 5},
        )

        # ---- Resolve difficulty thresholds (including random) ----
        min_len = int(getattr(args, 'min_anagram_len', ANAGRAM_LEN_DEFAULTS['min']))
        max_len = int(getattr(args, 'max_anagram_len', ANAGRAM_LEN_DEFAULTS['max']))
        band_min = getattr(args, 'min_total_score', None)
        band_max = getattr(args, 'max_total_score', None)
        picked_band = None
        if band_min is None and band_max is None and getattr(args, 'difficulty', 'random'):
            diff = str(args.difficulty)
            if diff == 'random':
                # Fresh random band per run using the generator RNG
                pool = ['easy', 'medium', 'hard', 'xtream']
                rng.shuffle(pool)
                picked_band = pool[0]
            elif diff in DIFFICULTY_BANDS:
                picked_band = diff
            if picked_band:
                b = DIFFICULTY_BANDS.get(picked_band, {})
                band_min = b.get('min_score')
                band_max = b.get('max_score')
        # Madness day: do not apply band selection, but enforce a safety floor
        # at least equal to the easy band minimum. Keep any explicit max.
        if _was_madness:
            picked_band = None
            # Derive the easy-band floor directly from DIFFICULTY_BANDS
            _easy_floor = DIFFICULTY_BANDS.get('easy', {}).get('min_score')
            try:
                _easy_floor = int(_easy_floor) if _easy_floor is not None else None
            except Exception:
                _easy_floor = None
            if _easy_floor is not None:
                if band_min is None:
                    band_min = _easy_floor
                else:
                    try:
                        band_min = max(int(band_min), _easy_floor)
                    except Exception:
                        band_min = _easy_floor
        if picked_band:
            dbg(f"gating: picked band={picked_band} min={band_min} max={band_max} (payload_total={candidate_lambda_total_score} ana_len={len(longest_one_try)})")

        # ---- Apply gates ----
        if len(longest_one_try) < min_len or len(longest_one_try) > max_len:
            dbg(f"reject: anagram length {len(longest_one_try)} outside [{min_len},{max_len}] -> retry")
            continue
        if band_min is not None and candidate_lambda_total_score < int(band_min):
            dbg(f"reject: payload_total {candidate_lambda_total_score} < min_total_score {band_min} -> retry")
            continue
        if band_max is not None and candidate_lambda_total_score > int(band_max):
            dbg(f"reject: payload_total {candidate_lambda_total_score} > max_total_score {band_max} -> retry")
            continue

        # ---- Cooldown check and record ----
        pid_try = _column_puzzle_id(target, col, diag_dir, diag, rows)
        if puzzle_in_cooldown(column_log, pid_try, args.cooldown_days):
            dbg("reject: pid in cooldown -> retry")
            continue
        # Record usage using the puzzle's intended date (if provided), else today's date
        date_for_usage = _today_iso_from_args(args)
        record_puzzle(column_log, pid_try, date_for_usage)
        # Also record madness usage when applicable (separate map, backwards-compatible)
        try:
            if _was_madness:
                column_log.setdefault('madness_puzzles', {})[pid_try] = date_for_usage
        except Exception:
            pass
        pid = pid_try
        # Store chosen longest for final payload to ensure match
        _precomputed_longest_one = longest_one_try
        _precomputed_valid_words_metadata = candidate_valid_words_metadata
        _precomputed_valid_words_map = candidate_valid_words_map
        _precomputed_lambda_total_score = candidate_lambda_total_score
        break

        # Defer writing payloads until after acceptance

    def _date_iso_from_args(args) -> str:
        # Simplified: always use today's date
        return datetime.now(timezone.utc).date().isoformat()

    today = _date_iso_from_args(args)

    # Resolve difficulty band if requested (including 'random' deterministic by date)
    picked_band = None
    band_min = None
    band_max = None
    # Apply explicit band mapping unless min/max provided override
    if getattr(args, 'difficulty', None):
        diff = str(args.difficulty)
        if diff == 'random':
            picked_band = _pick_difficulty_band_for_date(
                today,
                difficulty=diff,
                difficulty_random_weights=str(getattr(args, "difficulty_random_weights", "")),
                difficulty_random_salt=str(getattr(args, "difficulty_random_salt", "")),
                difficulty_random_no_repeat=bool(getattr(args, "difficulty_random_no_repeat", False)),
            )
        elif diff in DIFFICULTY_BANDS:
            picked_band = diff
        # Map to thresholds if a band was selected
        if picked_band:
            b = DIFFICULTY_BANDS.get(picked_band, {})
            band_min = b.get('min_score')
            band_max = b.get('max_score')
            dbg(f"difficulty selected: {picked_band} -> min={band_min} max={band_max}")

    layout_id = _column_layout_id(target, col, diag_dir, diag)

    # Compute longest anagram (≤10) — but if a preselected one exists from gating, reuse it
    anagram_pool = "".join(rows)
    rows_set = set(rows)

    longest_one = _precomputed_longest_one or ""
    if not longest_one:
        longest_candidates = longest_constructible_words(anagram_pool, _all)
        # Exclude any of the exact row words from the candidate list
        longest_candidates = [w for w in longest_candidates if w not in rows_set]

        def _pick_longest_upto10(cands: List[str]) -> str:
            # keep only <= 10 letters
            cands = [w for w in cands if len(w) <= 10]
            if not cands:
                return ""
            max_len = max(len(w) for w in cands)
            pool = [w for w in cands if len(w) == max_len]
            rng.shuffle(pool)  # tie-break with your RNG, keeps results reproducible with --seed
            return pool[0]

        longest_one = _pick_longest_upto10(longest_candidates)
        if not longest_one:
            # Fallback search (your previous “upto10” branch), but *don’t* pick the first alphabetically
            all_constructible = constructible_words_min_length(anagram_pool, _all, 1)
            upto10 = [w for w in all_constructible if len(w) <= 10 and w not in rows_set]
            if upto10:
                max_len = max(len(w) for w in upto10)
                pool = [w for w in upto10 if len(w) == max_len]
                rng.shuffle(pool)
                longest_one = pool[0]
            else:
                longest_one = ""

    # Set global target length for anagram bonus alignment with Lambda
    try:
        global ANAGRAM_TARGET_LEN
        ANAGRAM_TARGET_LEN = int(len(longest_one) or 0)
    except Exception:
        ANAGRAM_TARGET_LEN = 0

    if DEBUG_ENABLED:
        dbg(f"post-accept: using longest anagram='{longest_one}' len={len(longest_one)} (precomputed={'yes' if _precomputed_longest_one else 'no'})")

    letter_scores = LETTER_SCORES
    score_word = make_score_word(letter_scores)

    grid_row_scores = [score_word(w) for w in rows]
    vertical_target_score = score_word(target)
    diagonal_target_score = score_word(diag)
    longest_anagram_score = score_word(longest_one)
    total_score = compute_basic_total(
        rows=rows,
        target=target,
        diag=diag,
        longest_anagram=longest_one,
        score_word=score_word,
    )

    # Find other valid words from the 5x5 grid in all straight directions.
    # Rows/columns must be full grid length (5). Diagonals can be edge-to-edge substrings of length 2..5.
    words5_set = set(words5)

    # Rows: left-to-right and right-to-left
    valid_rows_lr = [w for w in rows if w in words5_set]
    valid_rows_rl = []
    for w in rows:
        rev = w[::-1]
        # Avoid duplicating palindromes: only include reverse if it's a different word
        if rev in words5_set and rev != w:
            valid_rows_rl.append(rev)

    # Columns: top-to-bottom and bottom-to-top
    valid_cols_tb: List[str] = []
    valid_cols_bt: List[str] = []
    for c in range(5):
        col_tb = "".join(rows[r][c] for r in range(5))
        col_bt = col_tb[::-1]
        if col_tb in words5_set:
            valid_cols_tb.append(col_tb)
        if col_bt in words5_set:
            valid_cols_bt.append(col_bt)

    # Diagonals: include edge-to-edge substrings of length 2..5 along main and anti in both directions
    # Build all diagonal paths (as (r,c) coordinate lists)
    def on_edge(r: int, c: int) -> bool:
        return r == 0 or c == 0 or r == 4 or c == 4

    main_paths: List[List[Tuple[int, int]]] = []
    anti_paths: List[List[Tuple[int, int]]] = []

    # Main (dr=+1, dc=+1) starting points: top row and left column
    for c0 in range(5):
        path = []
        r, c = 0, c0
        while r < 5 and c < 5:
            path.append((r, c))
            r += 1
            c += 1
        if len(path) >= 2:
            main_paths.append(path)
    for r0 in range(1, 5):
        path = []
        r, c = r0, 0
        while r < 5 and c < 5:
            path.append((r, c))
            r += 1
            c += 1
        if len(path) >= 2:
            main_paths.append(path)

    # Anti (dr=+1, dc=-1) starting points: top row and right column
    for c0 in range(4, -1, -1):
        path = []
        r, c = 0, c0
        while r < 5 and c >= 0:
            path.append((r, c))
            r += 1
            c -= 1
        if len(path) >= 2:
            anti_paths.append(path)
    for r0 in range(1, 5):
        path = []
        r, c = r0, 4
        while r < 5 and c >= 0:
            path.append((r, c))
            r += 1
            c -= 1
        if len(path) >= 2:
            anti_paths.append(path)

    # Collect matches using combined diag dictionary (3..5 letters)
    diag_words_set = set(_combined_diag_words)
    allowed_lengths = {3, 4, 5}

    def collect_diag_matches(paths: List[List[Tuple[int, int]]], forward_bucket: List[str], reverse_bucket: List[str]):
        for path in paths:
            letters = "".join(rows[r][c] for r, c in path)
            L = len(path)
            for i in range(L):
                for j in range(i + 1, L):
                    seg_len = j - i + 1
                    if seg_len not in allowed_lengths:
                        continue
                    (sr, sc) = path[i]
                    (er, ec) = path[j]
                    if not (on_edge(sr, sc) and on_edge(er, ec)):
                        continue
                    word_fwd = letters[i:j + 1]
                    word_rev = word_fwd[::-1]
                    if word_fwd in diag_words_set:
                        forward_bucket.append(word_fwd)
                    if word_rev in diag_words_set:
                        reverse_bucket.append(word_rev)

    valid_diag_main: List[str] = []
    valid_diag_main_rev: List[str] = []
    valid_diag_anti: List[str] = []
    valid_diag_anti_rev: List[str] = []

    collect_diag_matches(main_paths, valid_diag_main, valid_diag_main_rev)
    collect_diag_matches(anti_paths, valid_diag_anti, valid_diag_anti_rev)

    # Exclude the generated target words from valid word lists so they don't count toward total_score
    vt = (target or "").lower()
    dt = (diag or "").lower()
    # Remove vertical target from top-to-bottom columns (exact direction only)
    valid_cols_tb = [w for w in valid_cols_tb if (w or "").lower() != vt]
    # Remove diagonal target from forward diagonal lists (both main and anti, if present)
    valid_diag_main = [w for w in valid_diag_main if (w or "").lower() != dt]
    valid_diag_anti = [w for w in valid_diag_anti if (w or "").lower() != dt]

    valid_words = {
        "rows": {"lr": valid_rows_lr, "rl": valid_rows_rl},
        "columns": {"tb": valid_cols_tb, "bt": valid_cols_bt},
        "diagonals": {
            "main": valid_diag_main,
            "main_rev": valid_diag_main_rev,
            "anti": valid_diag_anti,
            "anti_rev": valid_diag_anti_rev,
        },
    }

    # Build occurrences list (include duplicates when the same word appears in reverse directions)
    occurrences: List[str] = (
            list(valid_rows_lr)
            + list(valid_rows_rl)
            + list(valid_cols_tb)
            + list(valid_cols_bt)
            + list(valid_diag_main)
            + list(valid_diag_main_rev)
            + list(valid_diag_anti)
            + list(valid_diag_anti_rev)
    )
    if longest_one:
        occurrences.append(longest_one)

    # Sorted list with duplicates preserved for display
    all_valid_words = sorted(occurrences)

    # New rule: if a word is duplicated (readable backwards), double its score when adding to all_valid_words_scores
    from collections import Counter
    occ_counts = Counter(occurrences)

    # Scores as a flat map: word -> int (doubled if duplicated)
    all_valid_words_scores = {}
    for w, cnt in occ_counts.items():
        base = score_word(w)
        all_valid_words_scores[w] = base * 2 if cnt > 1 else base

    # Update total_score to be the sum of these values
    total_score = sum(all_valid_words_scores.values())

    output = {
        "date": today,
        "id": pid,
        "chain_id": layout_id,
        "word_length": 5,
        "vertical_target_word": target,
        "column_index": col,
        "diagonal_direction": diag_dir,
        "diagonal_target_word": diag,
        "grid_rows": rows,
        "anagram_pool": anagram_pool,
        "longest_anagram": longest_one,
        # New score fields
        "word_scores": {
            "grid_rows": grid_row_scores,
            "vertical_target_word": vertical_target_score,
            "diagonal_target_word": diagonal_target_score,
            "longest_anagram": longest_anagram_score,
        },
        "total_score": total_score,
        "valid_words": valid_words,
        "all_valid_words": all_valid_words,
        "all_valid_words_scores": all_valid_words_scores,
    }

    if _precomputed_valid_words_metadata is not None and _precomputed_valid_words_map is not None and _precomputed_lambda_total_score is not None:
        valid_words_metadata = _precomputed_valid_words_metadata
        valid_words_map = _precomputed_valid_words_map
        lambda_total_score = _precomputed_lambda_total_score
    else:
        valid_words_metadata, valid_words_map, lambda_total_score = _build_final_scored_results(
            grid_rows=rows,
            words5_local=words5,
            combined_diag_words_local=_combined_diag_words,
            longest_anagram=longest_one,
            target_word=target,
            column_index=col,
            diagonal_word=diag,
            diagonal_direction=diag_dir,
            letter_scores_local=letter_scores,
            score_word_local=score_word,
            madness_available=bool(_was_madness),
            madness_word=_madness_word,
            madness_path=_madness_path,
            diagonal_lengths={2, 3, 4, 5},
        )

    # Determine saved_at: always use current UTC (puzzle-date flag removed)
    def _saved_at_now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    saved_at_str = _saved_at_now()

    # Compose madness metadata if applicable
    _madness_score: Optional[int] = None
    try:
        if _was_madness and _madness_word:
            _score_fn = getattr(_gen_mad_mod, "_score_word", None)
            if callable(_score_fn):
                _madness_score = int(_score_fn(_madness_word))
    except Exception:
        _madness_score = None

    # Determine difficulty band metadata for payloads
    try:
        _diff_type2 = str(getattr(args, 'difficulty', 'random'))
    except Exception:
        _diff_type2 = 'random'
    if bool(_was_madness):
        _diff_applied2 = 'skipped'
    else:
        if _diff_type2 == 'random':
            _diff_applied2 = picked_band if picked_band else 'random'
        else:
            _diff_applied2 = _diff_type2

    completed_doc = build_completed_payload(
        saved_at=saved_at_str,
        date=today,
        vertical_target_word=target,
        column_index=col,
        diagonal_direction=diag_dir,
        diagonal_target_word=diag,
        rows=rows,
        longest_anagram=longest_one,
        valid_words=valid_words_map,
        valid_words_metadata=valid_words_metadata,
        total_score=lambda_total_score,
        meta_extra={
            "madnessAvailable": bool(_was_madness),
            "madnessWord": (_madness_word if _was_madness else None),
            "madnessDirection": "forward" if _was_madness else None,
            "madnessPath": (_madness_path if _was_madness else None),
            "madnessScore": (_madness_score if (_was_madness and _madness_score is not None) else None),
            "difficultyBandType": _diff_type2,
            "difficultyBandApplied": _diff_applied2,
        },
    )

    # Semi-completed payload: hide scores and completed words, and mask grid_rows
    # Compute a deterministic shuffled version of the longest anagram (if any)
    longest_anagram_shuffled = shuffle_word_deterministic(
        longest_one.lower(),
        f"{layout_id}|{today}|{longest_one}",
    ) if longest_one else None

    semi_output = build_semi_completed_payload(
        date=today,
        puzzle_id=pid,
        layout_id=layout_id,
        vertical_target_word=target,
        column_index=col,
        diagonal_direction=diag_dir,
        diagonal_target_word=diag,
        rows=rows,
        longest_anagram_count=len(longest_one),
        longest_anagram_shuffled=longest_anagram_shuffled,
        extra_fields={
            "madnessAvailable": bool(_was_madness),
            "difficultyBandType": _diff_type2,
            "difficultyBandApplied": _diff_applied2,
        },
    )

    completed_path = RESOURCES_DIR / "margana-completed.json"
    semi_path = RESOURCES_DIR / "margana-semi-completed.json"
    try:
        completed_path, semi_path = write_payload_pair(
            RESOURCES_DIR,
            completed_payload=completed_doc,
            semi_completed_payload=semi_output,
        )
    except Exception as e:
        print(f"Warning: failed to write {semi_path}: {e}")

    # Save usage log locally and optionally upload back to S3
    save_usage_log_with_optional_s3_sync(
        usage_log=usage_log,
        usage_log_path=USAGE_LOG_FILE,
        bucket=s3_word_bucket,
        key=args.usage_s3_key,
        sync_to_s3=not args.no_s3_usage,
    )

    print(json.dumps(completed_doc, indent=2))
    print(f"\n💾 Full payload saved to {completed_path}\n💾 Semi payload saved to {semi_path}")


if __name__ == "__main__":
    main()
