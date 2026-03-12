#!/usr/bin/env python3
"""
Generator (Madness edition): adds detection of an adjacent-path hidden word "margana" (or reverse "anagram").
- Copies the behavior of generate-column-puzzle.py for building puzzles, scoring, outputs, uploads.
- Adds optional gating to require the hidden path and payload flags to announce it.
- Bonus value for the user (not Margana) is computed from letter-scores-v3.json for the detected word,
  so if scores change we don't need to modify this script.

Notes:
- This file is intentionally separate to avoid disrupting current runs of the main generator.
- Default behavior: detection enabled (non-blocking). Use --require-madness to only accept puzzles that contain the path.
- Use --reserve-with-puzzle-date to record usage cooldown using the puzzle date (so future scheduling aligns).
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
import hashlib
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple, Set

# Reuse modules from the project
from margana_gen.word_graph import load_words, longest_constructible_words, constructible_words_min_length
from margana_gen.usage_log import load_usage_log, save_usage_log, puzzle_in_cooldown, record_puzzle
from margana_gen.s3_utils import (
    download_usage_log_from_s3,
    upload_usage_log_to_s3,
    upload_margana_completed_to_s3,
    upload_margan_semi_completed_to_s3,
    download_word_list_from_s3,
)
from margana_score import remove_pre_loaded_words

SCRIPT_PATH = Path(__file__).resolve()
PYTHON_ROOT = SCRIPT_PATH.parents[1]
RESOURCES_DIR = PYTHON_ROOT.resolve()
WORD_LIST_DEFAULT = RESOURCES_DIR / "margana-word-list.txt"
WORDLIST_HORIZONTAL_EXCLUDE = RESOURCES_DIR / "horizontal-exclude-words.txt"
USAGE_LOG_FILE = (RESOURCES_DIR / "margana-puzzle-usage-log2.json").resolve()
WORDLIST_S3_KEY_DEFAULT = "word-lists/margana-word-list.txt"
USAGE_S3_KEY_DEFAULT = "usage-logs/margana-puzzle-usage-log2.json"

# Difficulty thresholds (copied from main generator)
DIFFICULTY_BANDS = {
    "easy":   {"min_score": 167, "max_score": 176},
    "medium": {"min_score": 177, "max_score": 196},
    "hard":   {"min_score": 197, "max_score": 206},
    "xtream": {"min_score": 207, "max_score": None},
}
ANAGRAM_LEN_DEFAULTS = {"min": 7, "max": 10}

# Debug
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
        pass

# 8-neighbor directions
DIRS8 = [
    (-1,-1),(-1,0),(-1,1),
    (0,-1),        (0,1),
    (1,-1),(1,0), (1,1)
]

# Random 8-neighbor simple path sampler (no cell reuse)
# Returns a list of (r,c) of given length within 5x5, or None if not found within internal tries

def _sample_random_path(length: int, rng: random.Random) -> Optional[List[Tuple[int,int]]]:
    R = C = 5
    if length <= 0:
        return []
    def in_bounds(r: int, c: int) -> bool:
        return 0 <= r < R and 0 <= c < C
    def neighbors(r: int, c: int) -> List[Tuple[int,int]]:
        nbrs = []
        for dr, dc in DIRS8:
            nr, nc = r + dr, c + dc
            if in_bounds(nr, nc):
                nbrs.append((nr, nc))
        rng.shuffle(nbrs)
        return nbrs
    # Try multiple random starts
    for _ in range(200):  # internal cap per sample request
        start = (rng.randrange(0, R), rng.randrange(0, C))
        used = {start}
        path = [start]
        # DFS with backtracking
        def dfs() -> bool:
            if len(path) >= length:
                return True
            r, c = path[-1]
            for nr, nc in neighbors(r, c):
                if (nr, nc) in used:
                    continue
                used.add((nr, nc))
                path.append((nr, nc))
                if dfs():
                    return True
                path.pop()
                used.remove((nr, nc))
            return False
        if dfs():
            return path[:]
    return None

def _find_adjacent_word_path(grid_rows: List[str], word: str) -> Optional[List[Tuple[int,int]]]:
    """Return a path of (r,c) for the word using 8-connected adjacency with no cell reuse; else None."""
    R = C = 5
    W = word.lower()
    G = [list(r.lower()) for r in grid_rows]

    def dfs(r: int, c: int, i: int, used: set[Tuple[int,int]]):
        if i == len(W) - 1:
            return [(r, c)]
        for dr, dc in DIRS8:
            nr, nc = r + dr, c + dc
            if 0 <= nr < R and 0 <= nc < C and (nr, nc) not in used and G[nr][nc] == W[i + 1]:
                p = dfs(nr, nc, i + 1, used | {(nr, nc)})
                if p is not None:
                    return [(r, c)] + p
        return None

    for r in range(R):
        for c in range(C):
            if G[r][c] == W[0]:
                p = dfs(r, c, 0, {(r, c)})
                if p is not None:
                    return p
    return None

# ---- Utilities largely mirrored from the main generator ----

def _load_letter_scores() -> dict:
    try:
        with open(RESOURCES_DIR / "letter-scores-v3.json", "r", encoding="utf-8") as sf:
            ls = json.load(sf)
    except Exception:
        ls = {}
    return {str(k).lower(): int(v) for k, v in ls.items() if isinstance(v, (int, float))}

LETTER_SCORES = _load_letter_scores()

def _score_word(w: str) -> int:
    if not w:
        return 0
    return sum(int(LETTER_SCORES.get(ch, 0)) for ch in str(w).lower())

# import build functions from the main file by reading it would be complex; we re-implement the essentials
# For simplicity and to keep this file self-contained, we import the whole original generator as a module would be nicer,
# but here we replicate the needed parts adapted from the displayed latest version.

# We reuse choose_rows_for_column_and_diag and build_puzzle logic by minimal inline versions (adapted) ---------

def choose_rows_for_column_and_diag(
    target_col: str,
    column: int,
    target_diag: str,
    words5: List[str],
    rng: random.Random,
    diag_direction: str,
    horizontal_exclude_set: set[str],
) -> Optional[List[str]]:
    if len(target_col) != 5 or len(target_diag) != 5:
        return None
    if diag_direction not in ("main", "anti"):
        return None

    def diag_idx(r: int) -> int:
        return r if diag_direction == "main" else 4 - r

    # Overlap check
    for r in range(5):
        if column == diag_idx(r) and target_col[r] != target_diag[r]:
            return None

    words5_set = set(words5)
    word_score_cache = {w: _score_word(w) for w in words5}

    # Pre-index: position -> letter -> words
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
        cand1 = index_buckets[column].get(ch_col, [])
        cand2 = index_buckets[d_idx].get(ch_diag, [])
        base = cand1 if len(cand1) <= len(cand2) else cand2
        other_idx, other_ch = (d_idx, ch_diag) if base is cand1 else (column, ch_col)
        filtered = []
        for w in base:
            is_unused = w not in used
            matches_other_constraint = w[other_idx] == other_ch
            is_not_excluded = w not in horizontal_exclude_set
            if is_unused and matches_other_constraint and is_not_excluded:
                filtered.append(w)
        if not filtered:
            return None
        rng.shuffle(filtered)
        filtered.sort(key=lambda w: (1 if w[::-1] in words5_set else 0, word_score_cache.get(w, 0)), reverse=True)
        chosen = filtered[0]
        rows.append(chosen)
        used.add(chosen)
    return rows

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
    if not words5:
        raise RuntimeError("No 5-letter words available in the word list.")
    if diag_direction_pref not in ("main", "anti", "random"):
        raise RuntimeError("--diag-direction must be one of: main, anti, random")

    words5_set = set(words5)
    def is_valid_target(t: str) -> bool:
        return len(t) == 5 and t in words5_set

    targets_pool = words5[:]; rng.shuffle(targets_pool)
    diag_pool = words5[:]; rng.shuffle(diag_pool)

    exclude_set = horizontal_exclude_set or set()

    # Forced path
    if target_forced:
        target = target_forced.lower()
        if not is_valid_target(target):
            raise RuntimeError(f"--target '{target_forced}' is not a valid 5-letter word in the list.")
        cols = [column_forced] if column_forced is not None else list(range(5))
        if column_forced is None:
            rng.shuffle(cols)
        for c in cols[:max_column_tries]:
            dirs = ["main", "anti"] if diag_direction_pref == "random" else [diag_direction_pref]
            rng.shuffle(dirs)
            if diag_target_forced:
                diag = diag_target_forced.lower()
                if not is_valid_target(diag):
                    raise RuntimeError(f"--diag-target '{diag_target_forced}' is not a valid 5-letter word in the list.")
                for ddir in dirs:
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, exclude_set)
                    if rows:
                        return target, c, ddir, diag, rows
                continue
            tries_d = 0
            for diag in diag_pool:
                tries_d += 1
                if tries_d > max_diag_tries:
                    break
                for ddir in dirs:
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, exclude_set)
                    if rows:
                        return target, c, ddir, diag, rows
        raise RuntimeError("Unable to build a grid that satisfies both column and diagonal constraints for the forced target.")

    # Unforced
    tries_t = 0
    for target in targets_pool:
        tries_t += 1
        if tries_t > max_target_tries:
            break
        cols = [column_forced] if column_forced is not None else list(range(5))
        if column_forced is None:
            rng.shuffle(cols)
        for c in cols[:max_column_tries]:
            dirs = ["main", "anti"] if diag_direction_pref == "random" else [diag_direction_pref]
            rng.shuffle(dirs)
            if diag_target_forced:
                diag = diag_target_forced.lower()
                if not is_valid_target(diag):
                    raise RuntimeError(f"--diag-target '{diag_target_forced}' is not a valid 5-letter word in the list.")
                for ddir in dirs:
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, exclude_set)
                    if rows:
                        return target, c, ddir, diag, rows
                continue
            tries_d = 0
            for diag in diag_pool:
                tries_d += 1
                if tries_d > max_diag_tries:
                    break
                for ddir in dirs:
                    rows = choose_rows_for_column_and_diag(target, c, diag, words5, rng, ddir, exclude_set)
                    if rows:
                        return target, c, ddir, diag, rows
    raise RuntimeError("Failed to construct a 5x5 column+diagonal puzzle within the try limits.")


def build_puzzle_with_path(
    words5: List[str],
    rng: random.Random,
    max_path_tries: int,
    madness_word_mode: str,  # 'margana' | 'anagram' | 'both'
    diag_direction_pref: str,
    max_target_tries: int,
    max_column_tries: int,
    max_diag_tries: int,
    max_row_backtrack_visits: int = 50000,
    horizontal_exclude_set: Optional[set[str]] = None,
) -> Tuple[str, int, str, str, List[str], List[Tuple[int,int]], str]:
    """
    Place a 7-letter 8-neighbor path for 'margana' or 'anagram' first, then choose
    (target, column, diag, dir) compatible with the fixed path letters, and finally
    pick 5 distinct row words that satisfy all fixed-position constraints.

    Returns: (target, column, diag_dir, diag_word, rows[5], path_coords, madness_word)
    """
    words5_set = set(words5)
    exclude_set = horizontal_exclude_set or set()

    def generate_path_candidates() -> List[Tuple[List[Tuple[int,int]], str]]:
        order: List[str] = []
        if madness_word_mode == "margana":
            order = ["margana"]
        elif madness_word_mode == "anagram":
            order = ["anagram"]
        else:
            order = ["margana", "anagram"]
        cands: List[Tuple[List[Tuple[int,int]], str]] = []
        tries = 0
        while tries < max_path_tries:
            for w in order:
                path = _sample_random_path(length=len(w), rng=rng)
                if path is not None:
                    cands.append((path, w))
                tries += 1
                if tries >= max_path_tries:
                    break
        rng.shuffle(cands)
        return cands

    # Helper: choose rows under multi-position constraints with backtracking
    word_score = {w: _score_word(w) for w in words5}
    buckets: List[dict] = []
    for i in range(5):
        d = {}
        for w in words5:
            d.setdefault(w[i], []).append(w)
        buckets.append(d)

    def choose_rows_with_constraints(constraints_by_row: List[dict]) -> Optional[List[str]]:
        """
        Pick 5 distinct row words under the fixed-position constraints.
        Improvements over the naive version:
        - MRV: choose next row with the fewest feasible candidates.
        - Forward-checking: after assigning a word, prune remaining domains and fail fast if any empties.
        - Visit cap: respect max_row_backtrack_visits to avoid long tails.
        """
        rows = [None] * 5  # type: ignore
        used: Set[str] = set()

        visits = 0
        visit_cap = int(max_row_backtrack_visits)

        # Precompute base candidate pools per row from letter-position constraints only (ignores `used`).
        def base_pool_for_row(r: int) -> List[str]:
            cons = constraints_by_row[r]
            if not cons:
                return [w for w in words5 if w not in exclude_set]
            pools: List[List[str]] = []
            for pos, ch in cons.items():
                pools.append(buckets[pos].get(ch, [])[:])
            if not pools:
                return [w for w in words5 if w not in exclude_set]
            pools.sort(key=len)
            pool = list(pools[0])
            for extra in pools[1:]:
                s = set(extra)
                pool = [w for w in pool if w in s]
            return [w for w in pool if w not in exclude_set]

        base_pools: List[List[str]] = [base_pool_for_row(r) for r in range(5)]

        # Order heuristic functions
        def rev_bonus(w: str) -> int:
            return 1 if w[::-1] in words5_set else 0

        def rank_pool(pool: List[str]) -> List[str]:
            # Shuffle a bit to avoid pathological ties, then sort by (reverse bonus, score)
            rng.shuffle(pool)
            pool.sort(key=lambda w: (rev_bonus(w), word_score.get(w, 0)), reverse=True)
            return pool

        # Current dynamic domains (filtered by `used`)
        def current_domain(r: int) -> List[str]:
            if rows[r] is not None:
                return [rows[r]]
            return [w for w in base_pools[r] if w not in used]

        def pick_next_row() -> Optional[int]:
            # Minimum Remaining Values (MRV): pick unassigned row with smallest domain size.
            best_r = None
            best_size = None
            for r in range(5):
                if rows[r] is not None:
                    continue
                dom_size = 0
                # Early peek size without materializing full ranking list
                for w in base_pools[r]:
                    if w not in used:
                        dom_size += 1
                        if dom_size > 1_000_000:  # never hit, just defensive
                            break
                if best_size is None or dom_size < best_size:
                    best_size = dom_size
                    best_r = r
            return best_r

        def backtrack() -> bool:
            nonlocal visits
            if visits >= visit_cap:
                return False
            # Check completion
            if all(rows[r] is not None for r in range(5)):
                return True
            # MRV choose row
            r = pick_next_row()
            if r is None:
                return True
            # Forward-checking: if any unassigned row already has empty domain, fail now
            for rr in range(5):
                if rows[rr] is None:
                    if not any(w not in used for w in base_pools[rr]):
                        return False

            cand = rank_pool(current_domain(r))
            for w in cand:
                # Place
                rows[r] = w
                used.add(w)
                visits += 1
                if visits >= visit_cap:
                    # Undo and fail
                    used.remove(w)
                    rows[r] = None
                    return False
                # Forward-checking: ensure all remaining rows still have at least one candidate (excluding `w` which is now used)
                feasible = True
                for rr in range(5):
                    if rows[rr] is None and not any(x not in used for x in base_pools[rr]):
                        feasible = False
                        break
                if feasible and backtrack():
                    return True
                # Undo
                used.remove(w)
                rows[r] = None
            return False

        ok = backtrack()
        if not ok:
            return None
        return [str(rows[i]) for i in range(5)]

    # Main search over paths and compatible (V,c,D,dir)
    path_candidates = generate_path_candidates()
    total_paths = len(path_candidates)
    dbg(f"path-first: generated {total_paths} path candidates (max_path_tries={max_path_tries})")

    # Pools for targets
    targets_pool = words5[:]; rng.shuffle(targets_pool)
    diag_pool = words5[:]; rng.shuffle(diag_pool)

    for idx, (path, madness_word) in enumerate(path_candidates, start=1):
        if idx == 1 or (idx % 10 == 0):
            dbg(f"path-first: trying path {idx}/{total_paths} for word='{madness_word}'")
        # Build a fast lookup for fixed letters by cell (path constraints)
        fixed = {(r, c): madness_word[i] for i, (r, c) in enumerate(path)}

        # Precompute 5-letter position index (reuse buckets built earlier)
        pos_index5 = buckets

        # Helper to compute diag index for a given direction
        def diag_idx_for(dir_name: str, rr: int) -> int:
            return rr if dir_name == "main" else 4 - rr

        # Directions to consider
        dirs_pref = ["main", "anti"] if diag_direction_pref == "random" else [diag_direction_pref]
        rng.shuffle(dirs_pref)

        # Early prune: for each direction, compute required diagonal letters from the PATH ONLY.
        # If no diagonal matches these fixed positions, skip that direction entirely.
        viable_dirs: List[str] = []
        base_diag_candidates_by_dir: dict[str, List[str]] = {}
        for ddir in dirs_pref:
            required_from_path: dict[int, str] = {}
            ok = True
            for (r_fixed, c_fixed), ch_fixed in fixed.items():
                if diag_idx_for(ddir, r_fixed) == c_fixed:
                    if r_fixed in required_from_path and required_from_path[r_fixed] != ch_fixed:
                        ok = False; break
                    required_from_path[r_fixed] = ch_fixed
            if not ok:
                continue
            # Intersect buckets for required positions
            if not required_from_path:
                base_diags = words5[:]  # any 5-letter word is a diagonal candidate at this stage
            else:
                pools = [pos_index5[rr].get(ch, []) for rr, ch in required_from_path.items()]
                if not pools:
                    base_diags = words5[:]
                else:
                    pools.sort(key=len)
                    base = list(pools[0])
                    if len(pools) > 1:
                        s_rest = [set(p) for p in pools[1:]]
                        for i2 in range(len(base) - 1, -1, -1):
                            w = base[i2]
                            if any(w not in s for s in s_rest):
                                base.pop(i2)
                    base_diags = base
            if base_diags:
                rng.shuffle(base_diags)
                viable_dirs.append(ddir)
                base_diag_candidates_by_dir[ddir] = base_diags
        if not viable_dirs:
            # No direction compatible with the path alone → skip path
            continue

        # columns to try
        cols = list(range(5)); rng.shuffle(cols)
        cols = cols[:max_column_tries]

        # Try counts per path
        tries_t = 0  # target tries (across all columns/dirs/diags for this path)
        target_cap_hit = False
        target_cap_logged = False
        TARGET_SAMPLE_CAP = 100  # soft cap per (path,col,dir,diag) to avoid exhausting the budget on one diagonal

        for c in cols:
            if target_cap_hit:
                break
            # Build column letter requirements from PATH for this column
            col_req_from_path: dict[int, str] = {r: ch for (r, cc), ch in fixed.items() if cc == c}

            for ddir in viable_dirs:
                if target_cap_hit:
                    break
                def diag_idx(rr: int) -> int:
                    return diag_idx_for(ddir, rr)

                # Start from base diag candidates that satisfy path-only constraints for this direction
                base_diag_candidates = base_diag_candidates_by_dir.get(ddir, [])
                if dbg and DEBUG_ENABLED:
                    # We'll print candidates after adding column/target overlap when tries_t==0 later
                    pass

                # For this column, diagonals also must agree with the vertical target at overlap rows (unknown yet).
                # We'll iterate each diagonal and derive minimal target constraints to pre-filter targets quickly.
                tries_d = 0

                # Optional progress: on first evaluation for this (path,c,dir), show base diag count
                if DEBUG_ENABLED:
                    dbg(f"path-first: path {idx}/{total_paths} col {c} dir={ddir} → base diag candidates = {len(base_diag_candidates)}")

                for diag in base_diag_candidates:
                    if target_cap_hit:
                        break
                    tries_d += 1
                    if tries_d % 50 == 0:
                        dbg(f"path-first: path {idx}/{total_paths} → diag try {tries_d}/{max_diag_tries} (col {c} dir={ddir})")
                    if tries_d > max_diag_tries:
                        dbg(f"path-first: reached max_diag_tries={max_diag_tries} for path {idx}/{total_paths} at col {c} dir={ddir}")
                        break

                    # Compute required target letters induced by this diag at overlap rows
                    target_requirements: dict[int, str] = dict(col_req_from_path)  # start with path column constraints
                    for rr in range(5):
                        if c == diag_idx(rr):
                            # vertical target at row rr must equal diagonal[rr]
                            if rr in target_requirements and target_requirements[rr] != diag[rr]:
                                target_requirements = None  # conflict
                                break
                            target_requirements[rr] = diag[rr]
                    if target_requirements is None:
                        continue

                    # Pre-filter target candidates by intersecting column buckets at constrained rows
                    if not target_requirements:
                        target_candidates = words5[:]
                    else:
                        t_pools = []
                        for rr, ch in target_requirements.items():
                            # For a column word, character at row rr is word[rr]
                            t_pools.append(pos_index5[rr].get(ch, []))
                        if not t_pools:
                            target_candidates = words5[:]
                        else:
                            t_pools.sort(key=len)
                            t_base = list(t_pools[0])
                            if len(t_pools) > 1:
                                s_rest = [set(p) for p in t_pools[1:]]
                                for i2 in range(len(t_base) - 1, -1, -1):
                                    w = t_base[i2]
                                    if any(w not in s for s in s_rest):
                                        t_base.pop(i2)
                            target_candidates = t_base

                    if tries_d == 1 and DEBUG_ENABLED:
                        dbg(f"path-first: path {idx}/{total_paths} col {c} dir={ddir} → diag='{diag}' yields target candidates = {len(target_candidates)}")

                    if not target_candidates:
                        continue

                    # If the target set is huge, sample a slice to avoid burning the entire path budget on one diag
                    if len(target_candidates) > TARGET_SAMPLE_CAP:
                        rng.shuffle(target_candidates)
                        target_candidates = target_candidates[:TARGET_SAMPLE_CAP]
                    else:
                        rng.shuffle(target_candidates)

                    for target in target_candidates:
                        # Guard: stop this path as soon as cap is hit (before incrementing)
                        if tries_t >= max_target_tries:
                            target_cap_hit = True
                            if DEBUG_ENABLED and not target_cap_logged:
                                dbg(f"path-first: path {idx}/{total_paths} hit max_target_tries={max_target_tries} -> advancing to next path")
                                target_cap_logged = True
                            break
                        tries_t += 1
                        if tries_t == 1 or (tries_t % 50 == 0):
                            dbg(f"path-first: path {idx}/{total_paths} → target try {tries_t}/{max_target_tries}")

                        # Sanity: ensure target agrees with target_requirements
                        ok_t = True
                        for rr, ch in target_requirements.items():
                            if target[rr] != ch:
                                ok_t = False; break
                        if not ok_t:
                            continue

                        # Build per-row fixed letter constraints and attempt rows
                        constraints_by_row: List[dict] = [dict() for _ in range(5)]
                        for rr in range(5):
                            constraints_by_row[rr][c] = target[rr]
                            constraints_by_row[rr][diag_idx(rr)] = diag[rr]
                        for (r_fixed, c_fixed), ch_fixed in fixed.items():
                            constraints_by_row[r_fixed][c_fixed] = ch_fixed

                        rows = choose_rows_with_constraints(constraints_by_row)
                        if rows:
                            dbg(f"path-first: SUCCESS at path {idx}/{total_paths}, target_try {tries_t}, diag_try {tries_d}, dir={ddir}, col={c}")
                            return target, c, ddir, diag, rows, path, madness_word
                    # End targets loop
                # End diag loop
        # End columns loop
    raise RuntimeError("Failed to construct a path-first madness puzzle within the try limits.")

# ---- Hash helpers ----

def _column_puzzle_id(vertical_target: str, column: int, diag_dir: str, diag_target: str, rows: List[str]) -> str:
    base = f"V:{vertical_target}|C:{column}|D:{diag_dir}|T:{diag_target}|R:{'>'.join(rows)}"
    return hashlib.sha1(base.encode('utf-8')).hexdigest()[:10]

def _column_layout_id(vertical_target: str, column: int, diag_dir: str, diag_target: str) -> str:
    base = f"V:{vertical_target}|C:{column}|D:{diag_dir}|T:{diag_target}"
    return hashlib.sha1(base.encode('utf-8')).hexdigest()[:10]

# ---- Main ----

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a 5x5 column-word puzzle JSON (Madness edition).")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--environment", type=str, default="dev", choices=["dev", "preprod", "prod"])
    p.add_argument("--words-file", type=str, default=str(WORD_LIST_DEFAULT))
    p.add_argument("--target", type=str, default=None)
    p.add_argument("--column", type=int, default=None, choices=[0,1,2,3,4])
    p.add_argument("--diag-target", type=str, default=None)
    p.add_argument("--diag-direction", type=str, choices=["main","anti","random"], default="random")

    # Usage/cooldown
    p.add_argument("--cooldown-days", type=int, default=365)
    p.add_argument("--max-usage-tries", type=int, default=50)
    p.add_argument("--no-s3-usage", action="store_true")
    p.add_argument("--s3-bucket", type=str, default="margana-word-game")
    p.add_argument("--usage-s3-key", type=str, default=USAGE_S3_KEY_DEFAULT)

    # Try limits
    p.add_argument("--max-target-tries", type=int, default=500)
    p.add_argument("--max-column-tries", type=int, default=5)
    p.add_argument("--max-diag-tries", type=int, default=200)

    # Debug
    p.add_argument("--debug", action="store_true")
    p.add_argument("--debug-verbose", action="store_true")

    # Difficulty gating (same as main)
    p.add_argument("--difficulty", type=str, default=None, choices=["easy","medium","hard","xtream","random"])
    p.add_argument("--min-total-score", type=int, default=None)
    p.add_argument("--max-total-score", type=int, default=None)
    p.add_argument("--min-anagram-len", type=int, default=ANAGRAM_LEN_DEFAULTS["min"])
    p.add_argument("--max-anagram-len", type=int, default=ANAGRAM_LEN_DEFAULTS["max"])
    p.add_argument("--difficulty-random-weights", type=str, default="easy=3,medium=4,hard=2,xtream=1")
    p.add_argument("--difficulty-random-no-repeat", action="store_true")
    p.add_argument("--difficulty-random-salt", type=str, default="")

    # Upload / date
    p.add_argument("--upload-puzzle", action="store_true")
    p.add_argument("--puzzle-date", type=str, default=None, help="DD/MM/YYYY")
    p.add_argument("--puzzle-s3-prefix", type=str, default="public/daily-puzzles")

    # DDB (pass-through; same as main)
    p.add_argument("--write-ddb", action="store_true")
    p.add_argument("--ddb-table", type=str, default="MarganaUserResults")
    p.add_argument("--ddb-region", type=str, default=None)
    p.add_argument("--ddb-sub", type=str, default=None)
    p.add_argument("--ddb-create-table", action="store_true")

    # NEW: Madness controls
    p.add_argument("--margana-madness", dest="margana_madness", action="store_true", help="Enable adjacent-path detection and payload flags.")
    p.add_argument("--no-margana-madness", dest="margana_madness", action="store_false", help="Disable adjacent-path detection.")
    p.set_defaults(margana_madness=True)  # default ON in this copy
    p.add_argument("--require-madness", action="store_true", help="Only accept puzzles that contain the madness path.")
    p.add_argument("--madness-bonus", type=int, default=None, help="Override bonus value; if omitted, compute from letter-scores-v3.json for the detected word.")
    p.add_argument("--reserve-with-puzzle-date", action="store_true", help="Record cooldown using puzzle date instead of today.")

    # NEW: Path-first Madness controls
    p.add_argument("--max-path-tries", type=int, default=200, help="Max different 8-neighbor paths to try for the madness word (default 200)")
    p.add_argument("--madness-word", type=str, default="both", choices=["margana","anagram","both"], help="Which madness word to place when path-first building (default: both)")

    return p.parse_args()

# Reuse compute_lambda_style_total from main behavior (simplified to use metadata scoring)
# Instead of duplicating all, we will rely on the main generator's payload scoring approach by reconstructing
# the same items. To keep time reasonable, we approximate by calling the same logic as in main would: rows + reverses +
# columns + reverses + diagonal substrings + longest anagram with palindrome doubling (anagram not doubled).

# For brevity and safety in this copy, we will compute acceptance using the final payload's total assembled below,
# so gating and payload always match.


def main():
    args = parse_args()

    global DEBUG_ENABLED, DEBUG_VERBOSE
    DEBUG_ENABLED = bool(args.debug)
    DEBUG_VERBOSE = bool(args.debug_verbose)
    if DEBUG_ENABLED:
        dbg("Debugging enabled" + (" (verbose)" if DEBUG_VERBOSE else ""))

    rng = random.Random(args.seed)
    s3_word_bucket = f"margana-word-game-{args.environment}"

    # Ensure words file
    words_path = Path(args.words_file).resolve()
    if not words_path.exists():
        if not args.no_s3_usage:
            dbg(f"words file missing; downloading from s3://{s3_word_bucket}/{WORDLIST_S3_KEY_DEFAULT}")
            etag_path = words_path.with_suffix(".etag")
            ok = download_word_list_from_s3(
                bucket=s3_word_bucket,
                key=WORDLIST_S3_KEY_DEFAULT,
                dest_path=str(words_path),
                etag_cache_path=str(etag_path),
                use_cache=True,
            )
            if not ok or not words_path.exists():
                raise FileNotFoundError("Word list not found and S3 download failed")
        else:
            raise FileNotFoundError(f"Word list not found at {words_path}")

    words_by_len, _all = load_words(str(words_path))
    words5 = words_by_len.get(5, [])
    horizontal_exclude_set = load_horizontal_exclude_set()
    words2 = words_by_len.get(2, [])
    words3 = words_by_len.get(3, [])
    words4 = words_by_len.get(4, [])
    combined_diag_words = list(set([w.lower() for w in (words2 + words3 + words4 + words5)]))
    diag_words_set = set(combined_diag_words)

    # Usage log
    if not args.no_s3_usage:
        download_usage_log_from_s3(bucket=s3_word_bucket, key=args.usage_s3_key, dest_path=USAGE_LOG_FILE)
    usage_log = load_usage_log(USAGE_LOG_FILE)
    column_log = usage_log.setdefault("column_puzzle", {"puzzles": {}})

    # Helper: date from args
    def _date_iso_from_args() -> str:
        if args.puzzle_date:
            try:
                dd, mm, yyyy = str(args.puzzle_date).strip().split('/')
                y, m, d = int(yyyy), int(mm), int(dd)
                return f"{y:04d}-{m:02d}-{d:02d}"
            except Exception:
                pass
        return datetime.now(timezone.utc).date().isoformat()

    today_iso = _date_iso_from_args()

    # Outer attempts for cooldown/retry
    attempt = 0
    while True:
        attempt += 1
        if attempt > args.max_usage_tries:
            raise RuntimeError("Exceeded --max-usage-tries while searching for a non-repeated column puzzle.")

        # Build puzzle: if madness is required, use path-first builder; otherwise use classic builder
        madness_path = None
        madness_word_used = None
        if bool(args.require_madness):
            target, col, diag_dir, diag, rows, madness_path, madness_word_used = build_puzzle_with_path(
                words5=words5,
                rng=rng,
                max_path_tries=int(getattr(args, 'max_path_tries', 200)),
                madness_word_mode=str(getattr(args, 'madness_word', 'both')),
                diag_direction_pref=str(args.diag_direction),
                max_target_tries=int(args.max_target_tries),
                max_column_tries=int(args.max_column_tries),
                max_diag_tries=int(args.max_diag_tries),
                horizontal_exclude_set=horizontal_exclude_set,
            )
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

        # Precompute longest anagram (<=10) reusing main logic approach
        anagram_pool_try = "".join(rows)
        rows_set_try = set(rows)
        longest_candidates_try = longest_constructible_words(anagram_pool_try, _all)
        longest_candidates_try = [w for w in longest_candidates_try if w not in rows_set_try and len(w) <= 10]
        if longest_candidates_try:
            max_len = max(len(w) for w in longest_candidates_try)
            pool = [w for w in longest_candidates_try if len(w) == max_len]
            rng.shuffle(pool)
            longest_one = pool[0]
        else:
            all_constructible_try = constructible_words_min_length(anagram_pool_try, _all, 1)
            upto10 = [w for w in all_constructible_try if len(w) <= 10 and w not in rows_set_try]
            if upto10:
                max_len = max(len(w) for w in upto10)
                pool = [w for w in upto10 if len(w) == max_len]
                rng.shuffle(pool)
                longest_one = pool[0]
            else:
                longest_one = ""

        # Difficulty gating: compute exact payload-style total (mirror main behavior)
        # Build metadata items similar to the main generator
        ws5 = set(words5)
        items: List[dict] = []
        # rows lr & rl
        rows_lr = [r for r in rows]
        rows_rl = [r[::-1] for r in rows_lr]
        for i, w in enumerate(rows_lr):
            if w in ws5:
                items.append({"word": w, "type": "row", "index": i, "direction": "lr"})
        for i, w in enumerate(rows_rl):
            if w in ws5 and w != rows_lr[i]:
                items.append({"word": w, "type": "row", "index": i, "direction": "rl"})
        # columns tb/bt
        cols_tb: List[str] = []
        cols_bt: List[str] = []
        for cidx in range(5):
            col_str = "".join(rows[r][cidx] for r in range(5))
            cols_tb.append(col_str)
            cols_bt.append(col_str[::-1])
        for j, w in enumerate(cols_tb):
            if w in ws5:
                items.append({"word": w, "type": "column", "index": j, "direction": "tb"})
        for j, w in enumerate(cols_bt):
            if w in ws5 and w != cols_tb[j]:
                items.append({"word": w, "type": "column", "index": j, "direction": "bt"})
        # diagonals edge-to-edge substrings 2..5, both dirs
        def on_edge(r: int, c: int) -> bool:
            return r == 0 or c == 0 or r == 4 or c == 4
        # build paths
        main_paths: List[List[Tuple[int,int]]] = []
        anti_paths: List[List[Tuple[int,int]]] = []
        # main
        for c0 in range(5):
            path=[]; r=0; c=c0
            while r<5 and c<5:
                path.append((r,c)); r+=1; c+=1
            if len(path)>=2: main_paths.append(path)
        for r0 in range(1,5):
            path=[]; r=r0; c=0
            while r<5 and c<5:
                path.append((r,c)); r+=1; c+=1
            if len(path)>=2: main_paths.append(path)
        # anti
        for c0 in range(4,-1,-1):
            path=[]; r=0; c=c0
            while r<5 and c>=0:
                path.append((r,c)); r+=1; c-=1
            if len(path)>=2: anti_paths.append(path)
        for r0 in range(1,5):
            path=[]; r=r0; c=4
            while r<5 and c>=0:
                path.append((r,c)); r+=1; c-=1
            if len(path)>=2: anti_paths.append(path)
        allowed_lengths = {2,3,4,5}
        def add_diag(paths: List[List[Tuple[int,int]]], fdir: str, rdir: str):
            for path in paths:
                letters = "".join(rows[r][c] for r,c in path)
                L = len(path)
                for i in range(L):
                    for j in range(i+1, L):
                        seg_len = j - i + 1
                        if seg_len not in allowed_lengths:
                            continue
                        (sr,sc) = path[i]; (er,ec) = path[j]
                        if not (on_edge(sr,sc) and on_edge(er,ec)):
                            continue
                        wf = letters[i:j+1]
                        wr = wf[::-1]
                        if wf in diag_words_set:
                            items.append({"word": wf, "type": "diagonal", "index": 0, "direction": fdir,
                                           "start_index": {"r": sr, "c": sc}, "end_index": {"r": er, "c": ec}})
                        if wr in diag_words_set:
                            items.append({"word": wr, "type": "diagonal", "index": 0, "direction": rdir,
                                           "start_index": {"r": er, "c": ec}, "end_index": {"r": sr, "c": sc}})
        add_diag(main_paths, "main", "main_rev")
        add_diag(anti_paths, "anti", "anti_rev")

        # The below code was removed in place of using the shared lib remove_pre_loaded_words
        # This was due to the current code allowing for a vt or dt to be included in the valid_words_metadata
        # when it was read backwards.
        #
        # remove generated targets
        # vt = (target or "").lower(); dt = (diag or "").lower()
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

        # add top anagram once
        if longest_one:
            items.append({"word": longest_one, "type": "anagram", "index": 0, "direction": "builder"})
        # compute total like main (palindromes double; anagram not doubled)
        total_exact = 0
        for it in items:
            w = str(it.get("word") or "")
            typ = it.get("type")
            base = _score_word(w)
            is_pal = (typ != "anagram") and (w == w[::-1] and len(w) > 0)
            total_exact += base * 2 if is_pal else base

        # Resolve difficulty band thresholds
        band_min = args.min_total_score
        band_max = args.max_total_score
        picked_band = None
        if bool(args.require_madness):
            # Madness day: bypass difficulty bands; only explicit min/max apply
            dbg("require-madness → bypassing difficulty bands; enforcing anagram_len and explicit min/max only")
        elif band_min is None and band_max is None and args.difficulty:
            diff = str(args.difficulty)
            if diff == 'random':
                # deterministic by date
                seed_key = f"{today_iso}|{args.difficulty_random_salt}"
                rnd = random.Random(seed_key)
                # parse weights
                weights = {"easy":3,"medium":4,"hard":2,"xtream":1}
                try:
                    for part in str(args.difficulty_random_weights or '').split(','):
                        if not part.strip():
                            continue
                        k,v = part.split('='); weights[k.strip()] = int(v.strip())
                except Exception:
                    pass
                pool = ["easy"]*int(weights.get("easy",0)) + ["medium"]*int(weights.get("medium",0)) + ["hard"]*int(weights.get("hard",0)) + ["xtream"]*int(weights.get("xtream",0))
                if not pool:
                    pool = ["easy","medium","hard","xtream"]
                pick = rnd.choice(pool)
                if bool(args.difficulty_random_no_repeat):
                    try:
                        y = (date.fromisoformat(today_iso) - timedelta(days=1)).isoformat()
                        rnd_y = random.Random(f"{y}|{args.difficulty_random_salt}")
                        py = rnd_y.choice(pool)
                        if py == pick and len(set(pool)) > 1:
                            alt = pick; tries=0
                            while alt == pick and tries < 5:
                                alt = rnd.choice(pool); tries += 1
                            pick = alt
                    except Exception:
                        pass
                picked_band = pick
            elif diff in DIFFICULTY_BANDS:
                picked_band = diff
            if picked_band:
                b = DIFFICULTY_BANDS[picked_band]
                band_min = b.get('min_score'); band_max = b.get('max_score')
                dbg(f"difficulty selected: {picked_band} -> min={band_min} max={band_max}")

        # Anagram len gate
        if len(longest_one) < int(args.min_anagram_len) or len(longest_one) > int(args.max_anagram_len):
            dbg(f"reject: anagram length {len(longest_one)} outside [{args.min_anagram_len},{args.max_anagram_len}] -> retry")
            continue
        # Score band gate
        if band_min is not None and total_exact < int(band_min):
            dbg(f"reject: payload_total {total_exact} < min_total_score {band_min} -> retry")
            continue
        if band_max is not None and total_exact > int(band_max):
            dbg(f"reject: payload_total {total_exact} > max_total_score {band_max} -> retry")
            continue

        # Madness detection (surprise day): when path-first builder is used, we already have the path
        madnessAvailable = False
        madnessPath: Optional[List[Tuple[int,int]]] = None
        madnessWord = None
        madnessDirection = None
        if bool(args.margana_madness):
            if bool(args.require_madness) and madness_path is not None:
                madnessAvailable = True
                madnessPath = madness_path
                madnessWord = madness_word_used or "margana"
                madnessDirection = "forward" if (madnessWord == "margana") else "reverse"
            else:
                # Fallback detection when not required/path-first
                p = _find_adjacent_word_path(rows, "margana")
                if p is None:
                    p = _find_adjacent_word_path(rows, "anagram")
                    if p is not None:
                        madnessWord = "anagram"; madnessDirection = "reverse"
                else:
                    madnessWord = "margana"; madnessDirection = "forward"
                if p is not None:
                    madnessAvailable = True
                    madnessPath = p
            if args.require_madness and not madnessAvailable:
                dbg("reject: madness required but not present -> retry")
                continue

        # Cooldown and record
        pid_try = _column_puzzle_id(target, col, diag_dir, diag, rows)
        if puzzle_in_cooldown(column_log, pid_try, args.cooldown_days):
            dbg("reject: pid in cooldown -> retry")
            continue
        # Record with puzzle date if requested (for scheduling correctness)
        record_date = today_iso if bool(args.reserve_with_puzzle_date) else datetime.today().date().isoformat()
        record_puzzle(column_log, pid_try, record_date)
        pid = pid_try
        layout_id = _column_layout_id(target, col, diag_dir, diag)

        # Build completed and semi payloads (mirror main generator where possible)
        # Build valid_words map from items (dedup by bucket)
        words_set_for_map = set(combined_diag_words)
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
        for it in items:
            w = (it.get("word") or "").lower(); typ = it.get("type"); direction = it.get("direction")
            if not w or w not in words_set_for_map:
                continue
            if typ == "row":
                bucket = "lr" if direction == "lr" else ("rl" if direction == "rl" else None)
                if bucket and w not in seen_map["rows"][bucket]:
                    valid_words_map["rows"][bucket].append(w); seen_map["rows"][bucket].add(w)
            elif typ == "column":
                bucket = "tb" if direction == "tb" else ("bt" if direction == "bt" else None)
                if bucket and w not in seen_map["columns"][bucket]:
                    valid_words_map["columns"][bucket].append(w); seen_map["columns"][bucket].add(w)
            elif typ == "diagonal":
                if direction in ("main","main_rev","anti","anti_rev") and w not in seen_map["diagonals"][direction]:
                    valid_words_map["diagonals"][direction].append(w); seen_map["diagonals"][direction].add(w)
            elif typ == "anagram":
                if w not in seen_map["anagram"]:
                    valid_words_map["anagram"].append(w); seen_map["anagram"].add(w)

        # Build valid_words_metadata including per-word score info
        valid_words_metadata: List[dict] = []
        letter_scores = LETTER_SCORES
        def add_item_meta(word: str, typ: str, direction: str, start=None, end=None):
            base = _score_word(word)
            is_pal = (typ != "anagram") and (word == word[::-1] and len(word) > 0)
            meta = {
                "word": word,
                "type": typ,
                "index": 0,
                "direction": direction,
                "start_index": start,
                "end_index": end,
                "palindrome": bool(is_pal),
                "semordnilap": False,
                "letter_value": {ch: int(letter_scores.get(ch,0)) for ch in set(word.lower()) if ch},
                "letters": [ch for ch in word.lower()],
                "letter_scores": [int(letter_scores.get(ch,0)) for ch in word.lower()],
                "letter_sum": int(base),
                "base_score": int(base * 2 if is_pal else base),
                "bonus": 0,
                "score": int(base * 2 if is_pal else base),
            }
            valid_words_metadata.append(meta)

        # Populate metadata from items
        for it in items:
            w = str(it.get("word") or ""); typ = it.get("type"); direction = it.get("direction")
            start = it.get("start_index"); end = it.get("end_index")
            add_item_meta(w, typ, direction, start, end)

        # The below code was removed in place of using the shared lib remove_pre_loaded_words
        # This was due to the current code allowing for a vt or dt to be included in the valid_words_metadata
        # when it was read backwards.
        #
        # Remove target words
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

    # Append an explicit madness item (score=0) in completed payload only
        madnessBonus = None
        madnessPath_list = None
        if madnessAvailable and madnessPath is not None:
            madnessPath_list = [[int(r), int(c)] for (r,c) in madnessPath]
            # compute bonus from letter scores unless overridden
            if args.madness_bonus is not None:
                madnessBonus = int(args.madness_bonus)
            else:
                # sum of letter scores for the detected word
                madnessBonus = _score_word(madnessWord or "margana")

        # Compute total score from metadata (like main)
        lambda_total_score = sum(int(it.get("score") or 0) for it in valid_words_metadata)

        # saved_at
        def _saved_at_from_args() -> str:
            if args.puzzle_date:
                try:
                    dd, mm, yyyy = str(args.puzzle_date).strip().split('/')
                    y, m, d = int(yyyy), int(mm), int(dd)
                    return datetime(y, m, d, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                except Exception:
                    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        saved_at_str = _saved_at_from_args()

        # Compose completed_doc
        completed_doc = {
            "saved_at": saved_at_str,
            "user": {"sub": "margana", "username": "margana", "email": None, "issuer": "generator", "identity_provider": None},
            "diagonal_direction": diag_dir,
            "diagonal_target_word": diag,
            "meta": {
                "date": today_iso,
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
                # Madness flags for completed payload
                "madnessAvailable": bool(madnessAvailable),
                "madnessBonus": int(madnessBonus) if madnessAvailable and madnessBonus is not None else 0,
                "madnessWord": madnessWord if madnessAvailable else None,
                "madnessDirection": madnessDirection if madnessAvailable else None,
            },
            "grid_rows": rows,
            "valid_words": valid_words_map,
            "valid_words_metadata": valid_words_metadata,
            "total_score": lambda_total_score,
        }
        if madnessAvailable and madnessPath_list is not None:
            completed_doc["meta"]["madnessPath"] = madnessPath_list

        # Save completed
        completed_path = RESOURCES_DIR / "margana-completed.json"
        try:
            with open(completed_path, "w", encoding="utf-8") as cf:
                json.dump(completed_doc, cf, indent=2)
        except Exception as e:
            print(f"Warning: failed to write {completed_path}: {e}")

        # Semi payload (no spoilers: include flags and bonus but not the path/word)
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

        semi_output = {
            "date": today_iso,
            "id": pid,
            "chain_id": layout_id,
            "word_length": 5,
            "vertical_target_word": target,
            "column_index": col,
            "diagonal_direction": diag_dir,
            "diagonal_target_word": diag,
            "grid_rows": _mask_rows(rows, col, diag_dir),
            "longest_anagram_count": len(longest_one),
            "longestAnagramShuffled": None,  # optional: could add same deterministic shuffle if desired
            # Madness flags for semi payload (announcement only)
            "madnessAvailable": bool(madnessAvailable),
            "madnessBonus": int(madnessBonus) if madnessAvailable and madnessBonus is not None else 0,
        }
        semi_path = RESOURCES_DIR / "margana-semi-completed.json"
        try:
            with open(semi_path, "w", encoding="utf-8") as sf:
                json.dump(semi_output, sf, indent=2)
        except Exception as e:
            print(f"Warning: failed to write {semi_path}: {e}")

        # Save usage log
        save_usage_log(usage_log, USAGE_LOG_FILE)
        if not args.no_s3_usage:
            upload_usage_log_to_s3(bucket=s3_word_bucket, key=args.usage_s3_key, src_path=USAGE_LOG_FILE)

        # Optional upload paired payloads
        if args.upload_puzzle:
            if not args.puzzle_date:
                raise ValueError("--upload-puzzle requires --puzzle-date in DD/MM/YYYY format.")
            key_completed = upload_margana_completed_to_s3(bucket=s3_word_bucket, prefix_root=args.puzzle_s3_prefix, puzzle_date_ddmmyyyy=args.puzzle_date, src_path=str(completed_path))
            print(f"✅ Uploaded completed payload to s3://{s3_word_bucket}/{key_completed}")
            key_semi = upload_margan_semi_completed_to_s3(bucket=s3_word_bucket, prefix_root=args.puzzle_s3_prefix, puzzle_date_ddmmyyyy=args.puzzle_date, src_path=str(semi_path))
            print(f"✅ Uploaded semi payload to s3://{s3_word_bucket}/{key_semi}")

        # Optional DDB write (mirrors main)
        if args.write_ddb:
            try:
                import boto3
                saved_at = completed_doc.get("saved_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                user = completed_doc.get("user") or {}
                user_sub = args.ddb_sub or user.get("sub") or ("gen-" + uuid.uuid4().hex[:24])
                username = (user.get("username") or "generator")
                try:
                    dt = datetime.fromisoformat(str(saved_at).replace("Z", "+00:00")).astimezone(timezone.utc)
                    date_str = dt.strftime("%Y-%m-%d")
                except Exception:
                    date_str = str(saved_at)[:10]
                day_key = date_str.replace('-', '')
                # Simple breakout
                vw_meta = completed_doc.get("valid_words_metadata") or []
                highest_word = ""; highest_score = 0
                for e in vw_meta:
                    w = str((e or {}).get("word") or "")
                    sc = int((e or {}).get("score") or 0)
                    if sc > highest_score:
                        highest_score = sc; highest_word = w.lower()
                item = {
                    "PK": f"USER#{user_sub}",
                    "SK": f"DATE#{day_key}",
                    "user_sub": str(user_sub),
                    "username": str(username),
                    "date": date_str,
                    "saved_at": str(saved_at),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "total_score": int(completed_doc.get("total_score") or 0),
                    "highest_scoring_word": highest_word,
                    "highest_scoring_word_score": highest_score,
                    "result_payload": completed_doc,
                }
                if args.ddb_create_table:
                    # Best-effort: create table if not exists (dev only)
                    client = boto3.client("dynamodb", region_name=args.ddb_region) if args.ddb_region else boto3.client("dynamodb")
                    try:
                        client.describe_table(TableName=f"{args.ddb_table}-{args.environment}")
                    except Exception:
                        client.create_table(
                            TableName=f"{args.ddb_table}-{args.environment}",
                            AttributeDefinitions=[{"AttributeName":"PK","AttributeType":"S"},{"AttributeName":"SK","AttributeType":"S"}],
                            KeySchema=[{"AttributeName":"PK","KeyType":"HASH"},{"AttributeName":"SK","KeyType":"RANGE"}],
                            BillingMode="PAY_PER_REQUEST",
                        )
                        client.get_waiter('table_exists').wait(TableName=f"{args.ddb_table}-{args.environment}")
                ddb = boto3.resource("dynamodb", region_name=args.ddb_region) if args.ddb_region else boto3.resource("dynamodb")
                tbl = ddb.Table(f"{args.ddb_table}-{args.environment}")
                tbl.put_item(Item=item)
                print(f"✅ Wrote result to DynamoDB table '{args.ddb_table}-{args.environment}' for user_sub={user_sub} date={date_str}")
            except Exception as e:
                print(f"⚠️  Skipped DynamoDB write: {e}")

        # Print completed to stdout
        print(json.dumps(completed_doc, indent=2))
        print(f"\n💾 Full payload saved to {completed_path}\n💾 Semi payload saved to {semi_path}")
        break

if __name__ == "__main__":
    main()
