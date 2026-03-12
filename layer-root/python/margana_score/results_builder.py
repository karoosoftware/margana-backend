"""
Utilities to compute Margana results payloads shared between lambdas.

This module mirrors the business logic from `lambda_margana_results.py` so that
multiple lambdas (e.g., the final submission lambda and the live-score lambda)
can build the same payload and response without duplicating code.

Functions are written with clear docstrings and type hints to simplify future
unit testing.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import os
import logging

from .wordlist import load_wordlist

# Helper utilities that can be re-used by tests
def _sign(x: int) -> int:
    return 0 if x == 0 else (1 if x > 0 else -1)


def iter_path_cells(item: Dict[str, Any]):
    """Yield dicts {"r":int, "c":int} for each cell along the item's path (inclusive).

    Accepts row/column/diagonal/anagram items. For non-grid items (e.g., anagram), yields nothing.
    Safe against malformed indices.
    """
    try:
        typ = str(item.get("type") or "").lower()
        if typ not in {"row", "column", "diagonal"}:
            return
        s = item.get("start_index") or {}
        e = item.get("end_index") or {}
        rs = int(s.get("r")); cs = int(s.get("c"))
        re = int(e.get("r")); ce = int(e.get("c"))
        dr = _sign(re - rs)
        dc = _sign(ce - cs)
        r, c = rs, cs
        yield {"r": r, "c": c}
        while (r, c) != (re, ce):
            r += dr
            c += dc
            yield {"r": r, "c": c}
    except Exception:
        return


def exclude_items_touching_invalid_rows(items: List[Dict[str, Any]], row_summaries: List[Dict[str, Any]]):
    """Return a new list excluding any item whose path touches a row marked invalid.

    - Uses `row_summaries[*].row` and `.valid` to build the invalid rows set.
    - Non-grid items (e.g., anagram) are never excluded by this rule.
    """
    try:
        bad_rows = set()
        for rs in row_summaries or []:
            try:
                is_invalid = not bool(rs.get("valid"))
                is_skipped = bool(rs.get("skipped"))
                if is_invalid or is_skipped:
                    bad_rows.add(int(rs.get("row")))
            except Exception:
                continue
        if not bad_rows:
            return list(items or [])
        kept: List[Dict[str, Any]] = []
        for it in items or []:
            typ = str(it.get("type") or "").lower()
            if typ not in {"row", "column", "diagonal"}:
                kept.append(it)
                continue
            touches_invalid = False
            for cell in iter_path_cells(it) or []:
                try:
                    if int(cell.get("r")) in bad_rows:
                        touches_invalid = True
                        break
                except Exception:
                    continue
            if not touches_invalid:
                kept.append(it)
        return kept
    except Exception:
        return list(items or [])


# v3 letter scores (must stay in sync with lambda_margana_results.py)
LETTER_SCORES: Dict[str, int] = {
    "a": 3, "b": 7, "c": 4, "d": 4, "e": 2, "f": 7, "g": 5, "h": 6, "i": 2, "j": 12, "k": 8, "l": 4,
    "m": 6, "n": 3, "o": 4, "p": 5, "q": 13, "r": 3, "s": 2, "t": 3, "u": 5, "v": 8, "w": 8, "x": 12,
    "y": 7, "z": 12
}

# Configurable bonuses (kept here so shared logic can use consistent defaults)
try:
    ANAGRAM_BONUS_POINTS = int(os.getenv("MARGANA_ANAGRAM_BONUS", "20"))
except Exception:
    ANAGRAM_BONUS_POINTS = 20

try:
    MADNESS_BONUS_DEFAULT = int(os.getenv("MARGANA_MADNESS_BONUS", "30"))
except Exception:
    MADNESS_BONUS_DEFAULT = 30


def safe_upper_letter(ch: Any) -> str:
    """Return a single A–Z letter for a cell value, or empty string if invalid."""
    if not isinstance(ch, str) or not ch:
        return ""
    ch = ch.strip()[:1]
    up = ch.upper()
    return up if "A" <= up <= "Z" else ""


def rebuild_grid(meta: Dict[str, Any], cells: List[Dict[str, Any]]) -> List[str]:
    """
    Rebuild a rectangular lowercase grid (list of row strings) from `meta` and `cells`.
    - Preserves fixed width using spaces for blanks.
    - Returns rows as lowercase strings of equal length.
    """
    rows = int(meta.get("rows") or 0)
    cols = int(meta.get("cols") or 0)
    if rows <= 0 or cols <= 0:
        max_r = max((int(c.get("r") or 0) for c in cells), default=-1)
        max_c = max((int(c.get("c") or 0) for c in cells), default=-1)
        rows = max_r + 1
        cols = max_c + 1
    grid: List[List[str]] = [["" for _ in range(cols)] for _ in range(rows)]
    for cell in cells:
        r = int(cell.get("r") or 0)
        c = int(cell.get("c") or 0)
        ch = safe_upper_letter(cell.get("letter"))
        if 0 <= r < rows and 0 <= c < cols:
            grid[r][c] = ch or ''
    # Preserve rectangular shape with spaces for blanks
    return ["".join((ch if ch else ' ') for ch in row).lower() for row in grid]


def _score_word(word: str) -> int:
    """Compute the per-letter score of a word (lowercase a–z only)."""
    total = 0
    for ch in (word or "").lower():
        if 'a' <= ch <= 'z':
            total += int(LETTER_SCORES.get(ch, 0))
    return total


def compute_valid_words(grid_rows: List[str], words_list: List[str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Find all valid words in the grid across rows, columns, and diagonals (both directions).
    Returns:
      - valid_words_items: list of dicts with fields {word, type, index, direction, start_index, end_index}
      - breakdown: dictionary with found words per orientation (for debugging/inspection)
    """
    valid_words_items: List[Dict[str, Any]] = []
    words_set = set(w.lower() for w in words_list)

    n = len(grid_rows)
    cols = len(grid_rows[0]) if n > 0 else 0

    rows_lr = [r.lower() for r in grid_rows]
    rows_rl = [r[::-1] for r in rows_lr]
    for i, w in enumerate(rows_lr):
        if w in words_set:
            valid_words_items.append({
                "word": w, "type": "row", "index": i, "direction": "lr",
                "start_index": {"r": i, "c": 0},
                "end_index": {"r": i, "c": max(0, cols - 1)},
            })
    for i, w in enumerate(rows_rl):
        if w in words_set:
            if i < len(rows_lr) and w == rows_lr[i]:
                continue
            valid_words_items.append({
                "word": w, "type": "row", "index": i, "direction": "rl",
                "start_index": {"r": i, "c": max(0, cols - 1)},
                "end_index": {"r": i, "c": 0},
            })

    cols_tb: List[str] = []
    cols_bt: List[str] = []
    for c in range(cols):
        col = "".join(grid_rows[r][c] for r in range(n))
        cols_tb.append(col)
        cols_bt.append(col[::-1])
    for j, w in enumerate(cols_tb):
        if w in words_set:
            valid_words_items.append({
                "word": w, "type": "column", "index": j, "direction": "tb",
                "start_index": {"r": 0, "c": j},
                "end_index": {"r": max(0, n - 1), "c": j},
            })
    for j, w in enumerate(cols_bt):
        if w in words_set:
            if j < len(cols_tb) and w == cols_tb[j]:
                continue
            valid_words_items.append({
                "word": w, "type": "column", "index": j, "direction": "bt",
                "start_index": {"r": max(0, n - 1), "c": j},
                "end_index": {"r": 0, "c": j},
            })

    def add_diag_matches(paths: List[List[Tuple[int, int]]], forward_dir: str, reverse_dir: str):
        for path in paths:
            if len(path) < 2:
                continue
            s = ''.join(grid_rows[r][c] for (r, c) in path)
            if s in words_set:
                valid_words_items.append({
                    "word": s, "type": "diagonal", "index": 0, "direction": forward_dir,
                    "start_index": {"r": path[0][0], "c": path[0][1]},
                    "end_index": {"r": path[-1][0], "c": path[-1][1]},
                })
            rs = s[::-1]
            if rs in words_set and rs != s:
                valid_words_items.append({
                    "word": rs, "type": "diagonal", "index": 0, "direction": reverse_dir,
                    "start_index": {"r": path[-1][0], "c": path[-1][1]},
                    "end_index": {"r": path[0][0], "c": path[0][1]},
                })

    main_diagonals: List[List[Tuple[int, int]]] = []
    anti_diagonals: List[List[Tuple[int, int]]] = []

    # main diagonals
    for c0 in range(cols):
        path = []
        r, c = 0, c0
        while r < n and c < cols:
            path.append((r, c))
            r += 1
            c += 1
        if len(path) >= 2:
            main_diagonals.append(path)
    for r0 in range(1, n):
        path = []
        r, c = r0, 0
        while r < n and c < cols:
            path.append((r, c))
            r += 1
            c += 1
        if len(path) >= 2:
            main_diagonals.append(path)

    # anti diagonals
    for c0 in range(cols - 1, -1, -1):
        path = []
        r, c = 0, c0
        while r < n and c >= 0:
            path.append((r, c))
            r += 1
            c -= 1
        if len(path) >= 2:
            anti_diagonals.append(path)
    for r0 in range(1, n):
        path = []
        r, c = r0, cols - 1
        while r < n and c >= 0:
            path.append((r, c))
            r += 1
            c -= 1
        if len(path) >= 2:
            anti_diagonals.append(path)

    add_diag_matches(main_diagonals, "main", "main_rev")
    add_diag_matches(anti_diagonals, "anti", "anti_rev")

    main_lr = ''.join(grid_rows[i][i] for i in range(min(n, cols)))
    main_rl = main_lr[::-1]
    anti_lr = ''.join(grid_rows[i][cols - 1 - i] for i in range(min(n, cols)))
    anti_rl = anti_lr[::-1]

    breakdown = {
        "rows": {"lr": rows_lr, "rl": rows_rl},
        "columns": {"tb": cols_tb, "bt": cols_bt},
        "diagonals": {
            "main": [main_lr] if main_lr else [],
            "main_rev": [main_rl] if main_rl else [],
            "anti": [anti_lr] if anti_lr else [],
            "anti_rev": [anti_rl] if anti_rl else [],
        },
    }
    return valid_words_items, breakdown


def bonus_for_valid_word(item: dict, meta: dict) -> int:
    """
    Anagram bonus policy (no word reveal):
    - Award `ANAGRAM_BONUS_POINTS` if and only if the submitted anagram uses exactly
      `meta.longestAnagramCount` letters (or if derivable via meta fields).
    - Otherwise, 0. Never reveals the anagram.
    """
    try:
        w = str(item.get("word") or "").strip().lower()
        t = str(item.get("type") or "").strip().lower()
        if t != "anagram":
            return 0

        count_val = meta.get("longestAnagramCount")
        if count_val is None:
            count_val = meta.get("longest_anagram_count")
        if count_val is None:
            try:
                shuffled = str(meta.get("longestAnagramShuffled") or meta.get("longest_anagram_shuffled") or "")
                if shuffled:
                    count_val = sum(1 for ch in shuffled if ch.isalpha()) or None
            except Exception:
                count_val = None

        target_len = int(count_val) if count_val is not None else None
        if target_len is None or target_len <= 0:
            return 0
        return int(ANAGRAM_BONUS_POINTS) if len(w) == int(target_len) else 0
    except Exception:
        return 0


def _normalize_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize meta fields to the canonical shape used by the results payload."""
    try:
        vt = str(meta.get("verticalTargetWord") or meta.get("vertical_target_word") or "").strip().lower()
        dt = str(meta.get("diagonalTargetWord") or meta.get("diagonal_target_word") or "").strip().lower()
        la = str(meta.get("longestAnagram") or meta.get("longest_anagram") or "").strip().lower()
        madness_av = meta.get("madnessAvailable")
        if madness_av is None:
            madness_av = meta.get("madness_available")
        madness_av_bool = bool(madness_av)
        meta_norm = dict(meta)
        if vt:
            meta_norm["verticalTargetWord"] = vt
        if dt:
            meta_norm["diagonalTargetWord"] = dt
        if la:
            meta_norm["longestAnagram"] = la
        meta_norm["madnessAvailable"] = madness_av_bool
        return meta_norm
    except Exception:
        return meta


def build_results_response(
    body_in: Dict[str, Any],
    path,
    *,
    commit: bool = False,
    user_sub: str | None = None,
    date_str: str | None = None,
) -> Dict[str, Any]:
    """
    Build the same response shape produced by lambda_margana_results.handler,
    without performing any S3 uploads or DynamoDB lookups.

    Input body must match the request that results lambda expects: at minimum
    `{ meta: {...}, cells: [...] }`.

    Returns the response payload dict containing keys:
      - meta, valid_words_metadata, total_score, skippedRows, 
        row_summaries, invoice, saved
        :param path: 
    """
    meta_in = (body_in.get("meta") or {}) if isinstance(body_in, dict) else {}
    cells = (body_in.get("cells") or []) if isinstance(body_in, dict) else []

    meta = _normalize_meta(meta_in)

    # Build grid
    grid_rows = rebuild_grid(meta, cells)
    if not grid_rows or not all(len(r) == len(grid_rows[0]) for r in grid_rows):
        # Minimal guard; the caller/lambda should convert to HTTP 400
        raise ValueError("Invalid grid provided.")

    # Load word list from provided path (caller resolves path)
    logger = logging.getLogger(__name__)
    logger.info("Results builder using provided word list path: %s", path)
    words_by_len, _ = load_wordlist(str(path))
    # For grid validation we only consider 3–5 letter words (grid rows/cols are 3–5),
    # but for anagram validation we allow 3–10 (per product rules and word list).
    words3 = words_by_len.get(3, [])
    words4 = words_by_len.get(4, [])
    words5 = words_by_len.get(5, [])
    grid_words_set = {w.lower() for w in (words3 + words4 + words5)}

    # Build an anagram set separately: 3–10 letters only.
    # Note: the bundled word list already contains only 3–10 length words,
    # but we enforce the bound explicitly for clarity.
    anagram_lengths = [L for L in range(3, 11)]
    anagram_words_set = {
        w.lower()
        for L, ws in words_by_len.items()
        if L in anagram_lengths
        for w in ws
    }

    # Union set used for downstream checks (e.g., semordnilap and valid_words map)
    combined_set = set(grid_words_set) | set(anagram_words_set)
    combined_words = list(combined_set)

    # Compute valid words and include user anagram if present (validate against S3 word list)
    # Compute grid valid words using only 3–5 letter list
    valid_items, _breakdown = compute_valid_words(grid_rows, list(grid_words_set))
    # --- Explicit anagram_result verdict ---
    submitted = None
    accepted = False
    reason = None

    user_word = None
    for k in ("userAnagram", "builderWord", "anagram", "user_word"):
        if isinstance(meta.get(k), str) and meta.get(k):
            user_word = meta.get(k)
            break
    if isinstance(user_word, str):
        w = user_word.strip().lower()
        submitted = w
        if not w or len(w) < 3:
            reason = "too_short"
        elif w not in anagram_words_set:
            reason = "not_in_wordlist"
        else:
            # Word is acceptable
            accepted = True
            # Only append if not already present from grid search
            if not any((it.get("type") == "anagram" and (it.get("word") or "").lower() == w) for it in valid_items):
                valid_items.append({
                    "word": w,
                    "type": "anagram",
                    "index": 0,
                    "direction": "builder",
                    "start_index": None,
                    "end_index": None,
                })

    # Parse optional skipped rows
    try:
        R = len(grid_rows)
        raw_skipped = meta.get("skippedRows") if isinstance(meta, dict) else None
        if raw_skipped is None:
            raw_skipped = meta.get("skipped_rows") if isinstance(meta, dict) else None
        raw_skipped = raw_skipped if isinstance(raw_skipped, list) else []
        skipped_rows = sorted({
            int(x) for x in raw_skipped
            if (isinstance(x, int) or (isinstance(x, str) and x.isdigit())) and 0 <= int(x) < R
        })
    except Exception:
        skipped_rows = []

    # Build per-row summaries
    try:
        R = len(grid_rows)
        valid_by_row_lr = {i: set() for i in range(R)}
        for it in valid_items:
            if it.get("type") == "row":
                idx = it.get("index")
                if isinstance(idx, int) and 0 <= idx < R:
                    w = (it.get("word") or "").lower()
                    if w:
                        valid_by_row_lr[idx].add(w)

        row_summaries = []
        for i, row_str in enumerate(grid_rows):
            skipped = i in skipped_rows
            word = (row_str or "").lower()
            if skipped:
                valid_flag = True
                score_val = 0
            else:
                valid_flag = word in valid_by_row_lr.get(i, set())
                score_val = _score_word(word) if valid_flag else 0
            row_summaries.append({
                "row": i,
                "skipped": skipped,
                "word": word,
                "valid": bool(valid_flag),
                "score": int(score_val),
            })
    except Exception:
        row_summaries = []

    valid_items = remove_pre_loaded_words(meta, valid_items)

    # Exclude any items whose path touches an invalid row (per row_summaries)
    try:
        valid_items = exclude_items_touching_invalid_rows(valid_items, row_summaries)
    except Exception:
        pass

    # Score items (palindrome, semordnilap, bonuses)
    for it in valid_items:
        w = (it.get("word") or "")
        base = _score_word(w)
        is_pal = (it.get("type") != "anagram") and (w == w[::-1] and len(w) > 0)
        rev = w[::-1] if isinstance(w, str) else ""
        is_sem = (it.get("type") != "anagram") and (rev != w and rev in combined_set)
        it["palindrome"] = bool(is_pal)
        it["semordnilap"] = bool(is_sem)

        # Duplicate-aware breakdowns
        letters_seq = [ch for ch in str(w).lower() if ch]
        scores_seq = [int(LETTER_SCORES.get(ch, 0)) for ch in letters_seq]
        it["letters"] = letters_seq
        it["letter_scores"] = scores_seq
        it["letter_value"] = {ch: int(LETTER_SCORES.get(ch, 0)) for ch in set(str(w).lower()) if ch}
        it["letter_sum"] = int(base)

        base_score = base * 2 if is_pal else base
        bonus_int = 0
        if str(it.get("type") or "").lower() == "madness":
            bonus_int = int(MADNESS_BONUS_DEFAULT)
            # Ensure coords present for UI (if any)
            coords = it.get("coords")
            if not coords or not isinstance(coords, list):
                it["coords"] = []
        else:
            bonus_int = int(bonus_for_valid_word(it, meta) or 0)

        it["base_score"] = int(base_score)
        it["bonus"] = int(bonus_int)
        it["score"] = it["base_score"] + it["bonus"]

    total_score = sum(int(it.get("score") or 0) for it in valid_items)

    # Build validated-only map (dedup across directions)
    words_set = set(x.lower() for x in combined_words)
    valid_words_map = {
        "rows": {"lr": [], "rl": []},
        "columns": {"tb": [], "bt": []},
        "diagonals": {"main": [], "main_rev": [], "anti": [], "anti_rev": []},
        "anagram": [],
    }
    seen = {"rows": {"lr": set(), "rl": set()}, "columns": {"tb": set(), "bt": set()}, "diagonals": {"main": set(), "main_rev": set(), "anti": set(), "anti_rev": set()}, "anagram": set()}
    seen_diagonals_all = set()
    for it in valid_items:
        w = (it.get("word") or "").lower()
        typ = it.get("type")
        direction = it.get("direction")
        if not w or w not in words_set:
            continue
        if typ == "row":
            bucket = "lr" if direction == "lr" else ("rl" if direction == "rl" else None)
            if bucket and w not in seen["rows"][bucket]:
                valid_words_map["rows"][bucket].append(w); seen["rows"][bucket].add(w)
        elif typ == "column":
            bucket = "tb" if direction == "tb" else ("bt" if direction == "bt" else None)
            if bucket and w not in seen["columns"][bucket]:
                valid_words_map["columns"][bucket].append(w); seen["columns"][bucket].add(w)
        elif typ == "diagonal":
            if direction in ("main", "main_rev", "anti", "anti_rev"):
                if w not in seen_diagonals_all and w not in seen["diagonals"][direction]:
                    valid_words_map["diagonals"][direction].append(w)
                    seen["diagonals"][direction].add(w)
                    seen_diagonals_all.add(w)
        elif typ == "anagram":
            if w not in seen["anagram"]:
                valid_words_map["anagram"].append(w); seen["anagram"].add(w)

    # Live mode never saves to S3; provide a consistent shape for `saved`
    saved = {"bucket": None, "key": None, "uploaded": False}

    resp = {
        "meta": meta,
        "valid_words_metadata": valid_items,
        "total_score": int(total_score),
        "skippedRows": skipped_rows,
        "row_summaries": row_summaries,
        "saved": saved,
        "valid_words": valid_words_map,
    }

    # Attach explicit anagram_result verdict for clients
    resp["anagram_result"] = {
        "submitted": submitted,
        "accepted": bool(accepted),
        **({"reason": reason} if (submitted and not accepted and reason) else {}),
    }

    return resp


def remove_pre_loaded_words(meta: dict[str, Any], valid_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Remove ONLY the pre-placed target words from scoring — scoped by their exact locations
    # (vertical at columnIndex; diagonal along diagonalDirection) and include their reverse forms.
    try:
        vt = str(meta.get("verticalTargetWord") or meta.get("vertical_target_word") or "").strip().lower()
        dt = str(meta.get("diagonalTargetWord") or meta.get("diagonal_target_word") or "").strip().lower()
        # indices and lengths
        try:
            col_idx = int(meta.get("columnIndex") if meta.get("columnIndex") is not None else meta.get("column_index"))
        except Exception:
            col_idx = None
        try:
            word_len = int(meta.get("wordLength") if meta.get("wordLength") is not None else meta.get("word_length"))
        except Exception:
            word_len = None
        diag_dir = str(meta.get("diagonalDirection") or meta.get("diagonal_direction") or "").strip().lower()

        def _is_preplaced_target_item(it: Dict[str, Any]) -> bool:
            try:
                w = str(it.get("word") or "").lower()
                typ = str(it.get("type") or "").lower()
                direction = str(it.get("direction") or "").lower()
                if not w:
                    return False
                # Only exclude exact pre-placed spans (full word length) when word_len is known
                if isinstance(word_len, int) and word_len > 0 and len(w) != word_len:
                    return False
                # Vertical column target exclusion (and its reverse)
                if vt and typ == "column":
                    if w == vt or w == vt[::-1]:
                        # Both readings at the vertical target location should not count.
                        # If col_idx is missing, we still exclude these words from ALL columns 
                        # to be safe, as fixed words are not meant to score.
                        if col_idx is None or it.get("index") == col_idx:
                            return True

                # Diagonal target exclusion (and its reverse)
                if dt and typ == "diagonal":
                    if w == dt or w == dt[::-1]:
                        # Both readings along the target diagonal should not count.
                        # If diag_dir is unknown, we exclude from all diagonals.
                        if not diag_dir or diag_dir not in ("main", "anti") or direction in {diag_dir, f"{diag_dir}_rev"}:
                            return True
                return False
            except Exception:
                return False

        if vt or dt:
            valid_items = [it for it in valid_items if not _is_preplaced_target_item(it)]
    except Exception:
        pass
    return valid_items
