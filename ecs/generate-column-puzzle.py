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
  python3 python/ecs/generate-column-puzzle.py --environment preprod --year 2026 --iso-week 6 --diag-direction random --madness-word both --max-path-tries 400 --max-target-tries 300 --max-diag-tries 200 --use-s3-path-layout

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
PYTHON_ROOT = SCRIPT_PATH.parents[1]
RESOURCES_DIR = PYTHON_ROOT.resolve()
WORD_LIST_DEFAULT = RESOURCES_DIR / "margana-word-list.txt"
WORDLIST_HORIZONTAL_EXCLUDE = RESOURCES_DIR / "horizontal-exclude-words.txt"
# ---------------------------

# S3 word list config (fallback if local words file missing)
WORDLIST_S3_KEY_DEFAULT = "word-lists/margana-word-list.txt"

# Usage log config (reuse Margana usage log file/key by default)
USAGE_LOG_FILE = (RESOURCES_DIR / "margana-puzzle-usage-log.json").resolve()
USAGE_S3_KEY_DEFAULT = "usage-logs/margana-puzzle-usage-log.json"

# ----- Difficulty band defaults derived from 1k-sample (see tmp_sample_scores.csv) -----
# Percentiles used: P35=167, P50=176, P75=196, P85=207
DIFFICULTY_BANDS = {
    "easy": {"min_score": 161, "max_score": 180},  # [167,176]
    "medium": {"min_score": 181, "max_score": 200},  # (176,196]
    "hard": {"min_score": 201, "max_score": 230},  # [197,206]
    "xtream": {"min_score": 231, "max_score": None},  # >=207
}
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
    p.add_argument("--output-root", type=str, default=str((PROJECT_ROOT / "tmp" / "payloads").resolve()),
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
                   choices=["easy", "medium", "hard", "xtream", "random"],
                   help="Difficulty band to enforce (default: random). Options: easy, medium, hard, xtream, or random.")
    p.add_argument("--min-total-score", type=int, default=None,
                   help="If set, require total_score >= this value (soft reject otherwise).")
    p.add_argument("--max-total-score", type=int, default=None,
                   help="If set, require total_score <= this value (soft reject otherwise).")
    p.add_argument("--min-anagram-len", type=int, default=ANAGRAM_LEN_DEFAULTS["min"],
                   help="Minimum longest-anagram length to accept (default 7).")
    p.add_argument("--max-anagram-len", type=int, default=ANAGRAM_LEN_DEFAULTS["max"],
                   help="Maximum longest-anagram length to accept (default 10).")
    p.add_argument("--difficulty-random-weights", type=str, default="easy=3,medium=4,hard=2,xtream=1",
                   help="Weights for --difficulty random, e.g. 'easy=3,medium=4,hard=2,xtream=1'.")
    p.add_argument("--difficulty-random-no-repeat", action="store_true",
                   help="When using --difficulty random, try to avoid repeating the same band as the previous day.")
    p.add_argument("--difficulty-random-salt", type=str, default="",
                   help="Optional salt string mixed into the deterministic random band picker for --difficulty random.")

    return p.parse_args()

def load_horizontal_exclude_set() -> set[str]:
    exclude_path = Path(WORDLIST_HORIZONTAL_EXCLUDE)
    horizontal_exclude_set: set[str] = set()

    if not exclude_path.exists():
        dbg(f"horizontal exclude file not found at {exclude_path}; continuing with no excludes")
        return horizontal_exclude_set

    for line in exclude_path.read_text(encoding="utf-8").splitlines():
        w = line.strip().lower()
        if not w or w.startswith("#"):
            continue
        if w.isalpha() and len(w) == 5:
            horizontal_exclude_set.add(w)

    dbg(f"loaded {len(horizontal_exclude_set)} horizontal exclude words from {exclude_path}")
    return horizontal_exclude_set

def choose_rows_for_column_and_diag(
        target_col: str,
        column: int,
        target_diag: str,
        words5: List[str],
        rng: random.Random,
        diag_direction: str,
        horizontal_exclude_set: set[str],
) -> Optional[List[str]]:
    """Choose 5 distinct row words so that for each row r:
    - row[r][column] == target_col[r]
    - row[r][diag_idx(r)] == target_diag[r], where diag_idx(r) is r (main) or 4-r (anti)
    Returns the 5 rows on success, else None.

    Change: prefer higher-scoring words based on resources/letter-scores.json
    while satisfying both column and diagonal constraints.
    """
    if len(target_col) != 5 or len(target_diag) != 5:
        dbg(f"reject: invalid target lengths col='{target_col}' diag='{target_diag}'", verbose=True)
        return None
    if diag_direction not in ("main", "anti"):
        dbg(f"reject: invalid diag_direction {diag_direction}")
        return None

    def diag_idx(r: int) -> int:
        return r if diag_direction == "main" else 4 - r

    dbg(f"try rows for target='{target_col}' column={column} diag='{target_diag}' dir={diag_direction}")

    # Quick feasibility check when constraints overlap at a cell shared by column and the selected diagonal
    # Overlap happens when column == diag_idx(r) for some r; then target_col[r] must equal target_diag[r].
    for r in range(5):
        if column == diag_idx(r):
            if target_col[r] != target_diag[r]:
                dbg(f"reject: overlap mismatch at row {r}: col='{target_col[r]}' vs diag='{target_diag[r]}'")
                return None

    # Load letter scores (minimal local load)
    scores_path = RESOURCES_DIR / "letter-scores-v3.json"
    try:
        with open(scores_path, "r", encoding="utf-8") as sf:
            _ls = json.load(sf)
    except Exception:
        _ls = {}
    letter_scores = {str(k).lower(): int(v) for k, v in _ls.items() if isinstance(v, (int, float))}

    def _score_word(w: str) -> int:
        s = 0
        for ch in w:
            s += int(letter_scores.get(ch.lower(), 0))
        return s

    word_score_cache = {w: _score_word(w) for w in words5}

    words5_set = set(words5)

    def _rev_bonus(w: str) -> int:
        rw = w[::-1]
        return 1 if rw in words5_set else 0

    # Pre-index by (index -> letter -> list of words)
    index_buckets: List[dict] = []
    for i in range(5):
        d = {}
        for w in words5:
            d.setdefault(w[i], []).append(w)
        index_buckets.append(d)

    used = set()
    rows: List[str] = []
    for r in range(5):
        ch_col = target_col[r]
        ch_diag = target_diag[r]
        d_idx = diag_idx(r)
        # Candidates must match both positions; intersect sets then pick highest score unused
        cand1 = index_buckets[column].get(ch_col, [])
        cand2 = index_buckets[d_idx].get(ch_diag, [])
        dbg(f" row {r}: need col[{column}]='{ch_col}', diag[{d_idx}]='{ch_diag}' | sizes: col={len(cand1)} diag={len(cand2)}",
            verbose=True)
        # Intersection via smaller list filter
        if len(cand1) <= len(cand2):
            base, other_idx, other_ch = cand1, d_idx, ch_diag
        else:
            base, other_idx, other_ch = cand2, column, ch_col

        # Keep only row candidates that are still unused, satisfy the second fixed-letter
        # constraint for this row, and are not in the horizontal exclude list.
        filtered = []
        for w in base:
            is_unused = w not in used
            matches_other_constraint = w[other_idx] == other_ch
            is_not_excluded = w not in horizontal_exclude_set
            if is_unused and matches_other_constraint and is_not_excluded:
                filtered.append(w)

        if not filtered:
            dbg(f"  reject row {r}: no candidates after filtering (used={len(used)})")
            return None
        # Tie-break with shuffle, then sort by score desc; prefer words whose reverse is also valid
        rng.shuffle(filtered)
        filtered.sort(key=lambda w: (_rev_bonus(w), word_score_cache.get(w, 0)), reverse=True)
        if DEBUG_ENABLED and DEBUG_VERBOSE:
            # Show up to 8 top candidates with scores
            preview = ", ".join(f"{w}:{word_score_cache.get(w, 0)}" for w in filtered[:8])
            dbg(f"  candidates[{len(filtered)}]: {preview}", verbose=True)
        chosen = filtered[0]
        rows.append(chosen)
        used.add(chosen)
        dbg(f"  chosen row {r}: '{chosen}' score={word_score_cache.get(chosen, 0)}")
    dbg(f"success: rows={rows}")
    return rows


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
    """Build and return (vertical_target_word, column_index, diagonal_direction, diagonal_target_word, grid_rows[5]).

    Tries forced values when provided; otherwise iterates with retry limits until a consistent grid is found
    that satisfies both the column target and the diagonal target.
    """
    if not words5:
        raise RuntimeError("No 5-letter words available in the word list.")
    if diag_direction_pref not in ("main", "anti", "random"):
        raise RuntimeError("--diag-direction must be one of: main, anti, random")

    words5_set = set(words5)

    def is_valid_target(t: str) -> bool:
        return len(t) == 5 and t in words5_set

    targets_pool = words5[:]
    rng.shuffle(targets_pool)
    diag_pool = words5[:]
    rng.shuffle(diag_pool)
    dbg(f"build_puzzle: targets={len(targets_pool)} diag_pool={len(diag_pool)} dir_pref={diag_direction_pref}")

    # Forced paths
    if target_forced:
        target = target_forced.lower()
        if not is_valid_target(target):
            raise RuntimeError(f"--target '{target_forced}' is not a valid 5-letter word in the list.")
        cols = [column_forced] if column_forced is not None else list(range(5))
        if column_forced is None:
            rng.shuffle(cols)
        dbg(f"forced target='{target}', columns={cols[:max_column_tries]}")
        for c in cols[:max_column_tries]:
            # Determine which diagonal directions to try
            dirs = ["main", "anti"] if diag_direction_pref == "random" else [diag_direction_pref]
            rng.shuffle(dirs)
            dbg(f" column {c}: dirs={dirs}")
            if diag_target_forced:
                diag = diag_target_forced.lower()
                if not is_valid_target(diag):
                    raise RuntimeError(
                        f"--diag-target '{diag_target_forced}' is not a valid 5-letter word in the list.")
                for ddir in dirs:
                    dbg(f"  try forced diag='{diag}' dir={ddir}")
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, horizontal_exclude_set or set())
                    if rows:
                        dbg(f"  success with column={c} dir={ddir} diag='{diag}' rows={rows}")
                        return target, c, ddir, diag, rows
                continue
            # Try multiple diagonal targets
            tries_d = 0
            for diag in diag_pool:
                tries_d += 1
                if tries_d > max_diag_tries:
                    break
                for ddir in dirs:
                    dbg(f"  try diag[{tries_d}]='{diag}' dir={ddir}", verbose=True)
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, horizontal_exclude_set or set())
                    if rows:
                        dbg(f"  success with column={c} dir={ddir} diag='{diag}' rows={rows}")
                        return target, c, ddir, diag, rows
        raise RuntimeError(
            "Unable to build a grid that satisfies both column and diagonal constraints for the forced target.")

    # Unforced path: iterate over targets and columns
    tries_t = 0
    for target in targets_pool:
        tries_t += 1
        if tries_t > max_target_tries:
            break
        cols = [column_forced] if column_forced is not None else list(range(5))
        if column_forced is None:
            rng.shuffle(cols)
        dbg(f"unforced target='{target}': try columns={cols[:max_column_tries]}")
        for c in cols[:max_column_tries]:
            # Try forced diagonal first if present
            dirs = ["main", "anti"] if diag_direction_pref == "random" else [diag_direction_pref]
            rng.shuffle(dirs)
            dbg(f" column {c}: dirs={dirs}")
            if diag_target_forced:
                diag = diag_target_forced.lower()
                if not is_valid_target(diag):
                    raise RuntimeError(
                        f"--diag-target '{diag_target_forced}' is not a valid 5-letter word in the list.")
                for ddir in dirs:
                    dbg(f"  try forced diag='{diag}' dir={ddir}")
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, horizontal_exclude_set or set())
                    if rows:
                        dbg(f"  success with column={c} dir={ddir} diag='{diag}' rows={rows}")
                        return target, c, ddir, diag, rows
                continue
            # Otherwise try a few random diagonal targets
            tries_d = 0
            for diag in diag_pool:
                tries_d += 1
                if tries_d > max_diag_tries:
                    break
                for ddir in dirs:
                    dbg(f"  try diag[{tries_d}]='{diag}' dir={ddir}", verbose=True)
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, horizontal_exclude_set or set())
                    if rows:
                        dbg(f"  success with column={c} dir={ddir} diag='{diag}' rows={rows}")
                        return target, c, ddir, diag, rows

    raise RuntimeError("Failed to construct a 5x5 column+diagonal puzzle within the try limits.")


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
    """
    Build a temporary valid_words_metadata exactly like the payload path and
    sum their scores. This keeps gating totals identical to the final payload.
    """
    # Load scores (same file the payload uses)
    try:
        with open(RESOURCES_DIR / "letter-scores-v3.json", "r", encoding="utf-8") as sf:
            letter_scores = json.load(sf)
    except Exception:
        letter_scores = {}
    letter_scores = {str(k).lower(): int(v) for k, v in letter_scores.items() if isinstance(v, (int, float))}

    def score_word(w: str) -> int:
        if not w:
            return 0
        return sum(int(letter_scores.get(ch, 0)) for ch in str(w).lower())

    # Build metadata just like _vw_items + filtering + scoring in payload path
    words5_set = set(words5)
    ws_diag = set(combined_diag_words)

    # Collect items
    items: List[dict] = []
    n = 5
    cols_n = 5

    # Rows lr and rl
    rows_lr = [r for r in rows]
    rows_rl = [r[::-1] for r in rows_lr]
    for i, w in enumerate(rows_lr):
        if w in words5_set:
            items.append({"word": w, "type": "row", "index": i, "direction": "lr"})
    for i, w in enumerate(rows_rl):
        if w in words5_set and w != rows_lr[i]:
            items.append({"word": w, "type": "row", "index": i, "direction": "rl"})

    # Columns tb/bt (full length only)
    cols_tb: List[str] = []
    cols_bt: List[str] = []
    for cidx in range(cols_n):
        col_str = "".join(rows[r][cidx] for r in range(n))
        cols_tb.append(col_str)
        cols_bt.append(col_str[::-1])
    for j, w in enumerate(cols_tb):
        if w in words5_set:
            items.append({"word": w, "type": "column", "index": j, "direction": "tb"})
    for j, w in enumerate(cols_bt):
        if w in words5_set and w != cols_tb[j]:
            items.append({"word": w, "type": "column", "index": j, "direction": "bt"})

    # Diagonals: edge-to-edge substrings length 3..5 in both dirs
    def on_edge(r: int, c: int) -> bool:
        return r == 0 or c == 0 or r == n - 1 or c == cols_n - 1

    main_paths: List[List[Tuple[int, int]]] = []
    anti_paths: List[List[Tuple[int, int]]] = []

    # main paths
    for c0 in range(cols_n):
        path = []
        r, c = 0, c0
        while r < n and c < cols_n:
            path.append((r, c))
            r += 1
            c += 1
        if len(path) >= 2:
            main_paths.append(path)
    for r0 in range(1, n):
        path = []
        r, c = r0, 0
        while r < n and c < cols_n:
            path.append((r, c))
            r += 1
            c += 1
        if len(path) >= 2:
            main_paths.append(path)

    # anti paths
    for c0 in range(cols_n - 1, -1, -1):
        path = []
        r, c = 0, c0
        while r < n and c >= 0:
            path.append((r, c))
            r += 1
            c -= 1
        if len(path) >= 2:
            anti_paths.append(path)
    for r0 in range(1, n):
        path = []
        r, c = r0, cols_n - 1
        while r < n and c >= 0:
            path.append((r, c))
            r += 1
            c -= 1
        if len(path) >= 2:
            anti_paths.append(path)

    allowed_lengths = {3, 4, 5}

    def add_diag_items(paths: List[List[Tuple[int, int]]], forward_dir: str, reverse_dir: str):
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
                    if word_fwd in ws_diag:
                        items.append({"word": word_fwd, "type": "diagonal", "index": 0, "direction": forward_dir,
                                      "start_index": {"r": sr, "c": sc}, "end_index": {"r": er, "c": ec}})
                    # Only append the reverse direction if it's not a palindrome (avoid duplicates)
                    if word_rev in ws_diag and word_rev != word_fwd:
                        items.append({"word": word_rev, "type": "diagonal", "index": 0, "direction": reverse_dir,
                                      "start_index": {"r": er, "c": ec}, "end_index": {"r": sr, "c": sc}})

    add_diag_items(main_paths, "main", "main_rev")
    add_diag_items(anti_paths, "anti", "anti_rev")

    # The below code was removed in place of using the shared lib remove_pre_loaded_words
    # This was due to the current code allowing for a vt or dt to be included in the valid_words_metadata
    # when it was read backwards.
    #
    # Remove the generated targets from metadata like payload does
    # vt = (target or "").lower()
    # dt = (diag or "").lower()
    # if vt or dt:
    #     items = [it for it in items if (str(it.get("word") or "").lower() not in {vt, dt})]

    exclusion_meta = {
        "wordLength": 5,
        "columnIndex": col,
        "diagonalDirection": diag_dir,
        "verticalTargetWord": target,
        "diagonalTargetWord": diag,
    }
    items = remove_pre_loaded_words(exclusion_meta, items)

    # Add longest anagram once
    if longest_one:
        items.append({"word": longest_one, "type": "anagram", "index": 0, "direction": "builder"})

    # Score like payload (palindromes double, anagram not special)
    total = 0
    for it in items:
        w = str(it.get("word") or "")
        typ = it.get("type")
        base = score_word(w)
        is_pal = (typ != "anagram") and (w == w[::-1] and len(w) > 0)
        score = base * 2 if is_pal else base
        total += int(score)
    return int(total)

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
    words_path = Path(args.words_file).resolve()
    if not words_path.exists():
        dbg(f"words file missing locally at {words_path}, downloading from s3://{s3_word_bucket}/{WORDLIST_S3_KEY_DEFAULT}")
        etag_path = words_path.with_suffix(".etag")
        ok = download_word_list_from_s3(
            bucket=s3_word_bucket,
            key=WORDLIST_S3_KEY_DEFAULT,
            dest_path=str(words_path),
            etag_cache_path=str(etag_path),
            use_cache=True,
        )
        dbg(f"word list download ok={ok} exists_now={words_path.exists()}")
        if not ok or not words_path.exists():
            raise FileNotFoundError(
                f"Word list not found and S3 download failed. Tried local '{words_path}' and s3://{s3_word_bucket}/{WORDLIST_S3_KEY_DEFAULT}")

    words_by_len, _all = load_words(str(words_path))
    words5 = words_by_len.get(5, [])

    horizontal_exclude_set = load_horizontal_exclude_set()

    dbg(f"loaded words: len2={len(words_by_len.get(2, []))} len3={len(words_by_len.get(3, []))} len4={len(words_by_len.get(4, []))} len5={len(words5)}")
    # Also prepare combined dictionary for diagonal edge-to-edge detection (3-5 letters)
    words3 = words_by_len.get(3, [])
    words4 = words_by_len.get(4, [])
    _combined_diag_words = list(set([w.lower() for w in (words3 + words4 + words5)]))

    # Usage log: optionally download from S3, then load
    if not args.no_s3_usage:
        dbg(f"downloading usage log from s3://{s3_word_bucket}/{args.usage_s3_key}")
        download_usage_log_from_s3(bucket=s3_word_bucket, key=args.usage_s3_key, dest_path=USAGE_LOG_FILE)
    usage_log = load_usage_log(USAGE_LOG_FILE)
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
        bands_used = {"easy": 0, "medium": 0, "hard": 0, "xtream": 0, "skipped": 0}

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
            while attempt < int(getattr(args, 'max_usage_tries', 50)) and not built:
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
                            continue
                        _was_mad = True
                        _mad_path = list(_path) if _path else None
                        _mad_word = str(_mad_word) if _mad_word else None
                    else:
                        target, col, diag_dir, diag, rows = build_puzzle(
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
                    continue

                # Gates and totals
                anagram_pool_try = "".join(rows)
                longest_candidates_try = longest_constructible_words(anagram_pool_try, _all)
                rows_set_try = set(rows)
                longest_candidates_try = [w for w in longest_candidates_try if w not in rows_set_try]

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
                    upto10_try = [w for w in all_constructible_try if len(w) <= 10 and w not in rows_set_try]
                    if upto10_try:
                        max_len = max(len(w) for w in upto10_try)
                        pool = [w for w in upto10_try if len(w) == max_len]
                        rng.shuffle(pool)
                        longest_one_try = pool[0]
                    else:
                        longest_one_try = ""

                exact_total = compute_lambda_style_total(
                    rows=rows,
                    col=col,
                    target=target,
                    diag=diag,
                    diag_dir=diag_dir,
                    words5=words5,
                    combined_diag_words=_combined_diag_words,
                    longest_one=longest_one_try,
                )

                # Apply minimal gates consistent with current args
                min_len = int(getattr(args, 'min_anagram_len', ANAGRAM_LEN_DEFAULTS['min']))
                max_len = int(getattr(args, 'max_anagram_len', ANAGRAM_LEN_DEFAULTS['max']))

                # Resolve difficulty band per DAY in batch mode
                band_min = getattr(args, 'min_total_score', None)
                band_max = getattr(args, 'max_total_score', None)
                picked_band = None
                try:
                    # Only derive from difficulty when explicit min/max not provided
                    if band_min is None and band_max is None:
                        diff = str(getattr(args, 'difficulty', 'random'))
                        if diff == 'random':
                            pool = ['easy', 'medium', 'hard', 'xtream']
                            rng.shuffle(pool)  # fresh random order per day
                            picked_band = pool[0]
                        elif diff in DIFFICULTY_BANDS:
                            picked_band = diff
                        if picked_band and not _was_mad:
                            b = DIFFICULTY_BANDS.get(picked_band, {})
                            band_min = b.get('min_score')
                            band_max = b.get('max_score')
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
                    continue
                if band_min is not None and exact_total < int(band_min):
                    continue
                if band_max is not None and exact_total > int(band_max):
                    continue

                # Cooldown and record using this day's date
                pid_try = _column_puzzle_id(target, col, diag_dir, diag, rows)
                if puzzle_in_cooldown(column_log, pid_try, args.cooldown_days):
                    continue
                record_puzzle(column_log, pid_try, day_iso)
                if _was_mad:
                    column_log.setdefault('madness_puzzles', {})[pid_try] = day_iso

                # Build payload documents with full scoring metadata (match single-day output)
                layout_id = _column_layout_id(target, col, diag_dir, diag)
                longest_one = longest_one_try

                # Load letter scores
                scores_path = RESOURCES_DIR / "letter-scores-v3.json"
                try:
                    with open(scores_path, "r", encoding="utf-8") as sf:
                        _ls_local = json.load(sf)
                except Exception:
                    _ls_local = {}
                _letter_scores_local = {str(k).lower(): int(v) for k, v in _ls_local.items() if
                                        isinstance(v, (int, float))}

                def _score_word_local(w: str) -> int:
                    if not w:
                        return 0
                    return sum(int(_letter_scores_local.get(ch, 0)) for ch in str(w).lower())

                # Helper to build Lambda-like items (rows/cols/diags) — simplified copy of _vw_items
                def _vw_items_batch(grid_rows: List[str]) -> List[dict]:
                    items: List[dict] = []
                    n = len(grid_rows)
                    cols_n = len(grid_rows[0]) if n > 0 else 0
                    ws5 = set(words5)
                    ws_diag = set(_combined_diag_words)
                    # rows lr and rl
                    rows_lr_local = [r for r in grid_rows]
                    rows_rl_local = [r[::-1] for r in rows_lr_local]
                    for i, w in enumerate(rows_lr_local):
                        if w in ws5:
                            items.append({
                                "word": w, "type": "row", "index": i, "direction": "lr",
                                "start_index": {"r": i, "c": 0},
                                "end_index": {"r": i, "c": max(0, cols_n - 1)},
                            })
                    for i, w in enumerate(rows_rl_local):
                        if w in ws5 and not (i < len(rows_lr_local) and w == rows_lr_local[i]):
                            items.append({
                                "word": w, "type": "row", "index": i, "direction": "rl",
                                "start_index": {"r": i, "c": max(0, cols_n - 1)},
                                "end_index": {"r": i, "c": 0},
                            })
                    # columns tb and bt
                    cols_tb_local: List[str] = []
                    cols_bt_local: List[str] = []
                    for cidx in range(cols_n):
                        col_str = "".join(grid_rows[r][cidx] for r in range(n))
                        cols_tb_local.append(col_str)
                        cols_bt_local.append(col_str[::-1])
                    for j, w in enumerate(cols_tb_local):
                        if w in ws5:
                            items.append({
                                "word": w, "type": "column", "index": j, "direction": "tb",
                                "start_index": {"r": 0, "c": j},
                                "end_index": {"r": max(0, n - 1), "c": j},
                            })
                    for j, w in enumerate(cols_bt_local):
                        if w in ws5 and not (j < len(cols_tb_local) and w == cols_tb_local[j]):
                            items.append({
                                "word": w, "type": "column", "index": j, "direction": "bt",
                                "start_index": {"r": max(0, n - 1), "c": j},
                                "end_index": {"r": 0, "c": j},
                            })

                    # diagonals (edge-to-edge substrings length 2..5)
                    def on_edge(r: int, c: int) -> bool:
                        return r == 0 or c == 0 or r == n - 1 or c == cols_n - 1

                    main_paths: List[List[Tuple[int, int]]] = []
                    anti_paths: List[List[Tuple[int, int]]] = []
                    for c0 in range(cols_n):
                        path = []
                        r, c = 0, c0
                        while r < n and c < cols_n:
                            path.append((r, c));
                            r += 1;
                            c += 1
                        if len(path) >= 2:
                            main_paths.append(path)
                    for r0 in range(1, n):
                        path = []
                        r, c = r0, 0
                        while r < n and c < cols_n:
                            path.append((r, c));
                            r += 1;
                            c += 1
                        if len(path) >= 2:
                            main_paths.append(path)
                    for c0 in range(cols_n - 1, -1, -1):
                        path = []
                        r, c = 0, c0
                        while r < n and c >= 0:
                            path.append((r, c));
                            r += 1;
                            c -= 1
                        if len(path) >= 2:
                            anti_paths.append(path)
                    for r0 in range(1, n):
                        path = []
                        r, c = r0, cols_n - 1
                        while r < n and c >= 0:
                            path.append((r, c));
                            r += 1;
                            c -= 1
                        if len(path) >= 2:
                            anti_paths.append(path)
                    allowed_lengths = {3, 4, 5}

                    def add_diag_items(paths: List[List[Tuple[int, int]]], forward_dir: str, reverse_dir: str):
                        for path in paths:
                            letters = "".join(grid_rows[r][c] for r, c in path)
                            L = len(path)
                            for i in range(L):
                                for j in range(i + 1, L):
                                    seg_len = j - i + 1
                                    if seg_len not in allowed_lengths:
                                        continue
                                    (sr, sc) = path[i];
                                    (er, ec) = path[j]
                                    if not (on_edge(sr, sc) and on_edge(er, ec)):
                                        continue
                                    word_fwd = letters[i:j + 1]
                                    word_rev = word_fwd[::-1]
                                    if word_fwd in ws_diag:
                                        items.append(
                                            {"word": word_fwd, "type": "diagonal", "index": 0, "direction": forward_dir,
                                             "start_index": {"r": sr, "c": sc}, "end_index": {"r": er, "c": ec}})
                                    # Only append the reverse direction if it's not a palindrome (avoid duplicates)
                                    if word_rev in ws_diag and word_rev != word_fwd:
                                        items.append(
                                            {"word": word_rev, "type": "diagonal", "index": 0, "direction": reverse_dir,
                                             "start_index": {"r": er, "c": ec}, "end_index": {"r": sr, "c": sc}})

                    add_diag_items(main_paths, "main", "main_rev")
                    add_diag_items(anti_paths, "anti", "anti_rev")
                    return items

                # Build base items
                valid_words_metadata = _vw_items_batch(rows)
                # Add longest anagram entry
                if longest_one:
                    valid_words_metadata.append({
                        "word": longest_one,
                        "type": "anagram",
                        "index": 0,
                        "direction": "builder",
                        "start_index": None,
                        "end_index": None,
                    })
                # The below code was removed in place of using the shared lib remove_pre_loaded_words
                # This was due to the current code allowing for a vt or dt to be included in the valid_words_metadata
                # when it was read backwards.
                #
                # Remove generated target words
                # vt_local = (target or "").lower(); dt_local = (diag or "").lower()
                # if vt_local or dt_local:
                #     valid_words_metadata = [it for it in valid_words_metadata if (it.get("word") or "").lower() not in {vt_local, dt_local}]

                exclusion_meta = {
                    "wordLength": 5,
                    "columnIndex": col,
                    "diagonalDirection": diag_dir,
                    "verticalTargetWord": target,
                    "diagonalTargetWord": diag,
                }
                valid_words_metadata = remove_pre_loaded_words(exclusion_meta, valid_words_metadata)

                _append_madness_item_to_metadata(
                    grid_rows=rows,
                    items=valid_words_metadata,
                    letter_scores=_letter_scores_local,  # use local scores in batch scope
                    madness_available=bool(_was_mad),
                    madness_word=_mad_word,
                    madness_path=_mad_path,
                )

                # Augment items with scoring info (letters, letter_scores, sums, palindromes, semordnilap)
                diag_words_set_local = set(_combined_diag_words)
                for it in valid_words_metadata:
                    w = (it.get("word") or "")
                    base = _score_word_local(w)
                    is_pal = (it.get("type") != "anagram") and (w == w[::-1] and len(w) > 0)
                    rev = w[::-1] if isinstance(w, str) else ""
                    is_sem = (it.get("type") != "anagram") and (rev != w and rev in diag_words_set_local)
                    it["palindrome"] = bool(is_pal)
                    it["semordnilap"] = bool(is_sem)
                    try:
                        it["letter_value"] = {ch: int(_letter_scores_local.get(ch, 0)) for ch in set(str(w).lower()) if
                                              ch}
                    except Exception:
                        it["letter_value"] = {ch: 0 for ch in set(str(w).lower()) if ch}
                    letters_seq = [ch for ch in str(w).lower() if ch]
                    scores_seq = [int(_letter_scores_local.get(ch, 0)) for ch in letters_seq]
                    it["letters"] = letters_seq
                    it["letter_scores"] = scores_seq
                    it["letter_sum"] = int(base)
                    base_score = base * 2 if is_pal else base
                    it["base_score"] = int(base_score)
                    it["bonus"] = 0
                    it["score"] = int(base_score)

                # Compute total and build valid_words map from metadata
                lambda_total_score = sum(int(it.get("score") or 0) for it in valid_words_metadata)
                words_set_for_map_local = set(_combined_diag_words)
                valid_words_map = {
                    "rows": {"lr": [], "rl": []},
                    "columns": {"tb": [], "bt": []},
                    "diagonals": {"main": [], "main_rev": [], "anti": [], "anti_rev": []},
                    "anagram": [],
                }
                _seen_map = {
                    "rows": {"lr": set(), "rl": set()},
                    "columns": {"tb": set(), "bt": set()},
                    "diagonals": {"main": set(), "main_rev": set(), "anti": set(), "anti_rev": set()},
                    "anagram": set(),
                }
                # Prevent cross-direction duplicates for diagonals (e.g., palindromes) in the exported map
                _seen_diagonals_all = set()
                for it in valid_words_metadata:
                    w = (it.get("word") or "").lower()
                    typ = it.get("type");
                    direction = it.get("direction")
                    if not w or w not in words_set_for_map_local:
                        continue
                    if typ == "row":
                        bucket = "lr" if direction == "lr" else ("rl" if direction == "rl" else None)
                        if bucket and w not in _seen_map["rows"][bucket]:
                            valid_words_map["rows"][bucket].append(w);
                            _seen_map["rows"][bucket].add(w)
                    elif typ == "column":
                        bucket = "tb" if direction == "tb" else ("bt" if direction == "bt" else None)
                        if bucket and w not in _seen_map["columns"][bucket]:
                            valid_words_map["columns"][bucket].append(w);
                            _seen_map["columns"][bucket].add(w)
                    elif typ == "diagonal":
                        if direction in ("main", "main_rev", "anti", "anti_rev"):
                            if w not in _seen_diagonals_all and w not in _seen_map["diagonals"][direction]:
                                valid_words_map["diagonals"][direction].append(w)
                                _seen_map["diagonals"][direction].add(w)
                                _seen_diagonals_all.add(w)
                    elif typ == "anagram":
                        if w not in _seen_map["anagram"]:
                            valid_words_map["anagram"].append(w);
                            _seen_map["anagram"].add(w)

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
                        _diff_applied = picked_band if picked_band else 'random'
                    else:
                        _diff_applied = _diff_type
                # Track band usage for batch summary
                try:
                    if _diff_applied in ('easy', 'medium', 'hard', 'xtream'):
                        bands_used[_diff_applied] = int(bands_used.get(_diff_applied, 0)) + 1
                    elif _diff_applied == 'skipped':
                        bands_used['skipped'] = int(bands_used.get('skipped', 0)) + 1
                except Exception:
                    pass

                completed_doc = {
                    "saved_at": datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat().replace("+00:00",
                                                                                                          "Z"),
                    "user": {"sub": "margana", "username": "margana", "email": None, "issuer": "generator",
                             "identity_provider": None},
                    "diagonal_direction": diag_dir,
                    "diagonal_target_word": diag,
                    "meta": {
                        "date": day_iso,
                        "rows": 5,
                        "cols": 5,
                        "wordLength": 5,
                        "columnIndex": col,
                        "diagonalDirection": diag_dir,
                        "verticalTargetWord": target if target else None,
                        "diagonalTargetWord": diag if diag else None,
                        "longestAnagram": (longest_one if longest_one else None),
                        "longestAnagramCount": int(len(longest_one)) if longest_one else 0,
                        "userAnagram": None,
                        "madnessAvailable": bool(_was_mad),
                        "madnessWord": (_mad_word if _was_mad else None),
                        "madnessDirection": "forward" if _was_mad else None,
                        "madnessPath": (_mad_path if _was_mad else None),
                        "madnessScore": (_madness_score if (_was_mad and _madness_score is not None) else None),
                        # Difficulty band metadata (requested fields)
                        "difficultyBandType": _diff_type,
                        "difficultyBandApplied": _diff_applied,
                    },
                    "grid_rows": rows,
                    "valid_words": valid_words_map,
                    "valid_words_metadata": valid_words_metadata,
                    "total_score": lambda_total_score,
                }

                # Semi payload
                def _mask_rows(rows_in: List[str], column_index: int, diag_direction: str) -> List[str]:
                    masked: List[str] = []
                    for rr in range(5):
                        row_chars = list(rows_in[rr])
                        for cc in range(5):
                            on_col = (cc == column_index)
                            on_diag = (diag_direction == "main" and cc == rr) or (
                                        diag_direction == "anti" and cc == 4 - rr)
                            row_chars[cc] = row_chars[cc] if (on_col or on_diag) else '*'
                        masked.append("".join(row_chars))
                    return masked

                longest_anagram_shuffled = None
                if longest_one:
                    rnd = random.Random(f"{layout_id}|{day_iso}|{longest_one}")
                    arr = list(longest_one.lower())
                    for i in range(len(arr) - 1, 0, -1):
                        j = rnd.randrange(0, i + 1)
                        arr[i], arr[j] = arr[j], arr[i]
                    shuffled = "".join(arr)
                    if shuffled == longest_one and len(longest_one) > 1:
                        shuffled = longest_one[1:].lower() + longest_one[0].lower()
                    longest_anagram_shuffled = shuffled

                semi_output = {
                    "date": day_iso,
                    "id": pid_try,
                    "chain_id": layout_id,
                    "word_length": 5,
                    "vertical_target_word": target,
                    "column_index": col,
                    "diagonal_direction": diag_dir,
                    "diagonal_target_word": diag,
                    "grid_rows": _mask_rows(rows, col, diag_dir),
                    "longest_anagram_count": len(longest_one),
                    "longestAnagramShuffled": longest_anagram_shuffled,
                    "madnessAvailable": bool(_was_mad),
                    # Difficulty band metadata (requested fields)
                    "difficultyBandType": _diff_type,
                    "difficultyBandApplied": _diff_applied,
                }

                # Write files for the day
                with open(day_dir / "margana-completed.json", "w", encoding="utf-8") as cf:
                    json.dump(completed_doc, cf, indent=2)
                with open(day_dir / "margana-semi-completed.json", "w", encoding="utf-8") as sf:
                    json.dump(semi_output, sf, indent=2)

                written += 1
                built = True
                # End while attempts

        # End for each day
        # Save usage once and optionally upload
        save_usage_log(usage_log, USAGE_LOG_FILE)
        if not args.no_s3_usage:
            upload_usage_log_to_s3(bucket=s3_word_bucket, key=args.usage_s3_key, src_path=USAGE_LOG_FILE)

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
                target, col, diag_dir, diag, rows = build_puzzle(
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
        longest_candidates_try = [w for w in longest_candidates_try if w not in rows_set_try]

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
            upto10_try = [w for w in all_constructible_try if len(w) <= 10 and w not in rows_set_try]
            if upto10_try:
                max_len = max(len(w) for w in upto10_try)
                pool = [w for w in upto10_try if len(w) == max_len]
                rng.shuffle(pool)
                longest_one_try = pool[0]
            else:
                longest_one_try = ""

        # ---- Compute the exact total using payload rules and this chosen anagram ----
        exact_total = compute_lambda_style_total(
            rows=rows,
            col=col,
            target=target,
            diag=diag,
            diag_dir=diag_dir,
            words5=words5,
            combined_diag_words=_combined_diag_words,
            longest_one=longest_one_try,
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
            dbg(f"gating: picked band={picked_band} min={band_min} max={band_max} (payload_total={exact_total} ana_len={len(longest_one_try)})")

        # ---- Apply gates ----
        if len(longest_one_try) < min_len or len(longest_one_try) > max_len:
            dbg(f"reject: anagram length {len(longest_one_try)} outside [{min_len},{max_len}] -> retry")
            continue
        if band_min is not None and exact_total < int(band_min):
            dbg(f"reject: payload_total {exact_total} < min_total_score {band_min} -> retry")
            continue
        if band_max is not None and exact_total > int(band_max):
            dbg(f"reject: payload_total {exact_total} > max_total_score {band_max} -> retry")
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
        break

        # Defer writing payloads until after acceptance

    def _date_iso_from_args(args) -> str:
        # Simplified: always use today's date
        return datetime.now(timezone.utc).date().isoformat()

    today = _date_iso_from_args(args)

    # Resolve difficulty band if requested (including 'random' deterministic by date)
    def _parse_weights(s: str) -> dict:
        d = {"easy": 3, "medium": 4, "hard": 2, "xtream": 1}
        try:
            for part in str(s or "").split(','):
                if not part.strip():
                    continue
                k, v = part.split('=')
                k = k.strip();
                v = int(v.strip())
                if k in d and v >= 0:
                    d[k] = v
        except Exception:
            pass
        return d

    picked_band = None
    band_min = None
    band_max = None
    # Apply explicit band mapping unless min/max provided override
    if getattr(args, 'difficulty', None):
        diff = str(args.difficulty)
        if diff == 'random':
            # Deterministic RNG based on date + optional salt
            seed_key = f"{today}|{getattr(args, 'difficulty_random_salt', '')}"
            rnd = random.Random(seed_key)
            w = _parse_weights(getattr(args, 'difficulty_random_weights', ''))
            # Build weighted list
            pool = ["easy"] * int(w.get("easy", 0)) + ["medium"] * int(w.get("medium", 0)) + \
                   ["hard"] * int(w.get("hard", 0)) + ["xtream"] * int(w.get("xtream", 0))
            if not pool:
                pool = ["easy", "medium", "hard", "xtream"]
            pick = rnd.choice(pool)
            # no-repeat guard: try a second draw if same as yesterday and option enabled
            if bool(getattr(args, 'difficulty_random_no_repeat', False)):
                try:
                    # derive yesterday band deterministically
                    y = (date.fromisoformat(today) - timedelta(days=1)).isoformat()
                    rnd_y = random.Random(f"{y}|{getattr(args, 'difficulty_random_salt', '')}")
                    py = rnd_y.choice(pool)
                    if py == pick and len(set(pool)) > 1:
                        # redraw once
                        alt = pick
                        tries = 0
                        while alt == pick and tries < 5:
                            alt = rnd.choice(pool)
                            tries += 1
                        pick = alt
                except Exception:
                    pass
            picked_band = pick
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

    # Load letter scores and compute per-word and total scores
    scores_path = RESOURCES_DIR / "letter-scores-v3.json"
    try:
        with open(scores_path, "r", encoding="utf-8") as sf:
            letter_scores = json.load(sf)
    except FileNotFoundError:
        letter_scores = {}

    # Normalize keys to lowercase for safety
    letter_scores = {str(k).lower(): int(v) for k, v in letter_scores.items() if isinstance(v, (int, float))}

    def score_word(w: str) -> int:
        if not w:
            return 0
        s = 0
        for ch in w.lower():
            s += int(letter_scores.get(ch, 0))
        return s

    grid_row_scores = [score_word(w) for w in rows]
    vertical_target_score = score_word(target)
    diagonal_target_score = score_word(diag)
    longest_anagram_score = score_word(longest_one)

    total_score = sum(grid_row_scores) + vertical_target_score + diagonal_target_score + longest_anagram_score

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

    # Build Lambda-like valid_words_metadata with start/end indices and per-word score
    def _vw_items(grid_rows: List[str]) -> List[dict]:
        items: List[dict] = []
        n = len(grid_rows)
        cols_n = len(grid_rows[0]) if n > 0 else 0
        # Use 5-letter dict for rows/columns, combined 2-5 for diagonals
        ws5 = set(words5)
        ws_diag = set(_combined_diag_words)
        # rows lr and rl (full length only)
        rows_lr_local = [r for r in grid_rows]
        rows_rl_local = [r[::-1] for r in rows_lr_local]
        for i, w in enumerate(rows_lr_local):
            if w in ws5:
                items.append({
                    "word": w, "type": "row", "index": i, "direction": "lr",
                    "start_index": {"r": i, "c": 0},
                    "end_index": {"r": i, "c": max(0, cols_n - 1)},
                })
        for i, w in enumerate(rows_rl_local):
            if w in ws5:
                # Avoid duplicating palindromic rows: if reverse equals forward, skip the reverse entry
                if i < len(rows_lr_local) and w == rows_lr_local[i]:
                    continue
                items.append({
                    "word": w, "type": "row", "index": i, "direction": "rl",
                    "start_index": {"r": i, "c": max(0, cols_n - 1)},
                    "end_index": {"r": i, "c": 0},
                })
        # columns tb and bt (full length only)
        cols_tb_local: List[str] = []
        cols_bt_local: List[str] = []
        for cidx in range(cols_n):
            col_str = "".join(grid_rows[r][cidx] for r in range(n))
            cols_tb_local.append(col_str)
            cols_bt_local.append(col_str[::-1])
        for j, w in enumerate(cols_tb_local):
            if w in ws5:
                items.append({
                    "word": w, "type": "column", "index": j, "direction": "tb",
                    "start_index": {"r": 0, "c": j},
                    "end_index": {"r": max(0, n - 1), "c": j},
                })
        for j, w in enumerate(cols_bt_local):
            if w in ws5:
                # Avoid duplicating palindromic columns: if reverse equals forward at the same index, skip
                if j < len(cols_tb_local) and w == cols_tb_local[j]:
                    continue
                items.append({
                    "word": w, "type": "column", "index": j, "direction": "bt",
                    "start_index": {"r": max(0, n - 1), "c": j},
                    "end_index": {"r": 0, "c": j},
                })

        # diagonals: edge-to-edge substrings length 2..5 along main and anti, both directions
        def on_edge(r: int, c: int) -> bool:
            return r == 0 or c == 0 or r == n - 1 or c == cols_n - 1

        main_paths: List[List[Tuple[int, int]]] = []
        anti_paths: List[List[Tuple[int, int]]] = []
        # main paths
        for c0 in range(cols_n):
            path = []
            r, c = 0, c0
            while r < n and c < cols_n:
                path.append((r, c))
                r += 1
                c += 1
            if len(path) >= 2:
                main_paths.append(path)
        for r0 in range(1, n):
            path = []
            r, c = r0, 0
            while r < n and c < cols_n:
                path.append((r, c))
                r += 1
                c += 1
            if len(path) >= 2:
                main_paths.append(path)
        # anti paths
        for c0 in range(cols_n - 1, -1, -1):
            path = []
            r, c = 0, c0
            while r < n and c >= 0:
                path.append((r, c))
                r += 1
                c -= 1
            if len(path) >= 2:
                anti_paths.append(path)
        for r0 in range(1, n):
            path = []
            r, c = r0, cols_n - 1
            while r < n and c >= 0:
                path.append((r, c))
                r += 1
                c -= 1
            if len(path) >= 2:
                anti_paths.append(path)
        allowed_lengths = {2, 3, 4, 5}

        def add_diag_items(paths: List[List[Tuple[int, int]]], forward_dir: str, reverse_dir: str):
            for path in paths:
                letters = "".join(grid_rows[r][c] for r, c in path)
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
                        if word_fwd in ws_diag:
                            items.append({
                                "word": word_fwd, "type": "diagonal", "index": 0, "direction": forward_dir,
                                "start_index": {"r": sr, "c": sc},
                                "end_index": {"r": er, "c": ec},
                            })
                        # Only append the reverse direction if it's not a palindrome (avoid duplicates)
                        if word_rev in ws_diag and word_rev != word_fwd:
                            items.append({
                                "word": word_rev, "type": "diagonal", "index": 0, "direction": reverse_dir,
                                "start_index": {"r": er, "c": ec},
                                "end_index": {"r": sr, "c": sc},
                            })

        add_diag_items(main_paths, "main", "main_rev")
        add_diag_items(anti_paths, "anti", "anti_rev")
        return items

    valid_words_metadata = _vw_items(rows)
    # Ensure the top anagram also appears in the metadata to mirror Lambda output
    if longest_one:
        valid_words_metadata.append({
            "word": longest_one,
            "type": "anagram",
            "index": 0,
            "direction": "builder",
            "start_index": None,
            "end_index": None,
        })

    # The below code was removed in place of using the shared lib remove_pre_loaded_words
    # This was due to the current code allowing for a vt or dt to be included in the valid_words_metadata
    # when it was read backwards.
    #
    # # Remove the generated target words from metadata (not part of the user's scored findings)
    # vt = (target or "").lower()
    # dt = (diag or "").lower()
    # if vt or dt:
    #     valid_words_metadata = [it for it in valid_words_metadata if (it.get("word") or "").lower() not in {vt, dt}]

    exclusion_meta = {
        "wordLength": 5,
        "columnIndex": col,
        "diagonalDirection": diag_dir,
        "verticalTargetWord": target,
        "diagonalTargetWord": diag,
    }
    valid_words_metadata = remove_pre_loaded_words(exclusion_meta, valid_words_metadata)

    # Inject Margana Madness metadata item (if applicable) before scoring loop
    _append_madness_item_to_metadata(
        grid_rows=rows,
        items=valid_words_metadata,
        letter_scores=letter_scores,
        madness_available=bool(_was_madness),
        madness_word=_madness_word,
        madness_path=_madness_path,
    )

    # Build a lookup set of words (2..5 letters) to evaluate semordnilap across all items consistently
    diag_words_set = set(_combined_diag_words)
    for it in valid_words_metadata:
        w = (it.get("word") or "")
        base = score_word(w)
        is_pal = (it.get("type") != "anagram") and (w == w[::-1] and len(w) > 0)
        rev = w[::-1] if isinstance(w, str) else ""
        is_sem = (it.get("type") != "anagram") and (rev != w and rev in diag_words_set)
        it["palindrome"] = bool(is_pal)
        it["semordnilap"] = bool(is_sem)
        # Align with Lambda: 'letter_value' maps unique letters only; also include duplicate-aware sequences
        try:
            # unique-letter map (legacy shape)
            it["letter_value"] = {ch: int(letter_scores.get(ch, 0)) for ch in set(str(w).lower()) if ch}
        except Exception:
            it["letter_value"] = {ch: 0 for ch in set(str(w).lower()) if ch}
        try:
            letters_seq = [ch for ch in str(w).lower() if ch]
            scores_seq = [int(letter_scores.get(ch, 0)) for ch in letters_seq]
            it["letters"] = letters_seq
            it["letter_scores"] = scores_seq
        except Exception:
            it["letters"] = [ch for ch in str(w).lower() if ch]
            it["letter_scores"] = [0 for _ in it["letters"]]
        # Simple aggregate
        it["letter_sum"] = int(base)
        # base score (palindrome doubles base)
        base_score = base * 2 if is_pal else base
        it["base_score"] = int(base_score)
        # NEW: compute and add a bonus consistent with Lambda lambda_margana_results.py
        # Margana's payload never gets a bonus, this is left in place so we are in sync with lambda_margana_results.py
        # it["bonus"] = int(bonus_for_valid_word(it))
        it["bonus"] = 0  # This should always be 0 as Margana never gets a bonus
        # it["score"] = int(it["base_score"]) + int(it["bonus"])
        it["score"] = int(it["base_score"])

    lambda_total_score = sum(int(it.get("score") or 0) for it in valid_words_metadata)

    # Determine saved_at: always use current UTC (puzzle-date flag removed)
    def _saved_at_now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    saved_at_str = _saved_at_now()

    # Compose Lambda-mirrored completed document
    # Build Lambda-like valid_words map from metadata (validated only, de-duplicated)
    words_set_for_map = set(_combined_diag_words)
    valid_words_map = {
        "rows": {"lr": [], "rl": []},
        "columns": {"tb": [], "bt": []},
        "diagonals": {"main": [], "main_rev": [], "anti": [], "anti_rev": []},
        "anagram": [],
    }
    seen_map = {
        "rows": {"lr": set(), "rl": set()},
        "columns": {"tb": set(), "bt": set()},
        "diagonals": {"main": set(), "main_rev": set(), "anti": set(), "anti_rev": set()},
        "anagram": set(),
    }
    # Prevent cross-direction duplicates for diagonals in the final exported map
    seen_diagonals_all = set()
    for it in valid_words_metadata:
        w = (it.get("word") or "").lower()
        typ = it.get("type")
        direction = it.get("direction")
        if not w or w not in words_set_for_map:
            continue
        if typ == "row":
            bucket = "lr" if direction == "lr" else ("rl" if direction == "rl" else None)
            if bucket and w not in seen_map["rows"][bucket]:
                valid_words_map["rows"][bucket].append(w);
                seen_map["rows"][bucket].add(w)
        elif typ == "column":
            bucket = "tb" if direction == "tb" else ("bt" if direction == "bt" else None)
            if bucket and w not in seen_map["columns"][bucket]:
                valid_words_map["columns"][bucket].append(w);
                seen_map["columns"][bucket].add(w)
        elif typ == "diagonal":
            if direction in ("main", "main_rev", "anti", "anti_rev"):
                if w not in seen_diagonals_all and w not in seen_map["diagonals"][direction]:
                    valid_words_map["diagonals"][direction].append(w)
                    seen_map["diagonals"][direction].add(w)
                    seen_diagonals_all.add(w)
        elif typ == "anagram":
            if w not in seen_map["anagram"]:
                valid_words_map["anagram"].append(w);
                seen_map["anagram"].add(w)

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

    completed_doc = {
        "saved_at": saved_at_str,
        "user": {
            "sub": "margana",
            "username": "margana",
            "email": None,
            "issuer": "generator",
            "identity_provider": None,
        },
        # Root-level fields mirroring semi payload for easier client consumption
        "diagonal_direction": diag_dir,
        "diagonal_target_word": diag,
        "meta": {
            "date": today,
            "rows": 5,
            "cols": 5,
            "wordLength": 5,
            "columnIndex": col,
            "diagonalDirection": diag_dir,
            "verticalTargetWord": target if target else None,
            "diagonalTargetWord": diag if diag else None,
            "longestAnagram": (longest_one if longest_one else None),
            "longestAnagramCount": int(len(longest_one)) if longest_one else 0,
            "userAnagram": None,
            # Madness metadata (camelCase to match existing style)
            "madnessAvailable": bool(_was_madness),
            "madnessWord": (_madness_word if _was_madness else None),
            "madnessDirection": "forward" if _was_madness else None,
            "madnessPath": (_madness_path if _was_madness else None),
            "madnessScore": (_madness_score if (_was_madness and _madness_score is not None) else None),
            # Difficulty band metadata (requested fields)
            "difficultyBandType": _diff_type2,
            "difficultyBandApplied": _diff_applied2,
        },
        "grid_rows": rows,
        "valid_words": valid_words_map,
        "valid_words_metadata": valid_words_metadata,
        "total_score": lambda_total_score,
    }

    # Save the required paired payloads under resources
    # Full payload (as-is)
    completed_path = RESOURCES_DIR / "margana-completed.json"
    try:
        with open(completed_path, "w", encoding="utf-8") as cf:
            json.dump(completed_doc, cf, indent=2)
    except Exception as e:
        print(f"Warning: failed to write {completed_path}: {e}")

    # Semi-completed payload: hide scores and completed words, and mask grid_rows
    def _mask_rows(rows_in: List[str], column_index: int, diag_direction: str) -> List[str]:
        masked: List[str] = []
        for r in range(5):
            row_chars = list(rows_in[r])
            for c in range(5):
                on_col = (c == column_index)
                on_diag = (diag_direction == "main" and c == r) or (diag_direction == "anti" and c == 4 - r)
                row_chars[c] = row_chars[c] if (on_col or on_diag) else '*'
            masked.append("".join(row_chars))
        return masked

    # Compose semi-completed view without revealing full solutions
    # Compute a deterministic shuffled version of the longest anagram (if any)
    def _shuffle_word_deterministic(word: str, seed_key: str) -> str:
        if not word:
            return ""
        # Deterministic RNG seeded by a stable key (layout + date + word)
        rnd = random.Random(seed_key)
        arr = list(word)
        for i in range(len(arr) - 1, 0, -1):
            j = rnd.randrange(0, i + 1)
            arr[i], arr[j] = arr[j], arr[i]
        shuffled = "".join(arr)
        if shuffled == word and len(word) > 1:
            # Ensure it is not equal to the original: rotate by 1
            shuffled = word[1:] + word[0]
        return shuffled

    longest_anagram_shuffled = _shuffle_word_deterministic(longest_one.lower(),
                                                           f"{layout_id}|{today}|{longest_one}") if longest_one else None

    semi_output = {
        "date": today,
        "id": pid,
        "chain_id": layout_id,
        "word_length": 5,
        "vertical_target_word": target,
        "column_index": col,
        "diagonal_direction": diag_dir,
        "diagonal_target_word": diag,
        "grid_rows": _mask_rows(rows, col, diag_dir),
        # Only expose the length of the longest anagram
        "longest_anagram_count": len(longest_one),
        # Provide a shuffled version of the longest anagram (camelCase as requested)
        "longestAnagramShuffled": longest_anagram_shuffled,
        # Minimal flag for UI: true if this puzzle was generated in Madness mode (no details leaked)
        "madnessAvailable": bool(_was_madness),
        # Difficulty band metadata (requested fields)
        "difficultyBandType": _diff_type2,
        "difficultyBandApplied": _diff_applied2,
    }

    semi_path = RESOURCES_DIR / "margana-semi-completed.json"
    try:
        with open(semi_path, "w", encoding="utf-8") as sf:
            json.dump(semi_output, sf, indent=2)
    except Exception as e:
        print(f"Warning: failed to write {semi_path}: {e}")

    # Save usage log locally and optionally upload back to S3
    save_usage_log(usage_log, USAGE_LOG_FILE)
    if not args.no_s3_usage:
        upload_usage_log_to_s3(bucket=s3_word_bucket, key=args.usage_s3_key, src_path=USAGE_LOG_FILE)

    print(json.dumps(completed_doc, indent=2))
    print(f"\n💾 Full payload saved to {completed_path}\n💾 Semi payload saved to {semi_path}")


if __name__ == "__main__":
    main()
