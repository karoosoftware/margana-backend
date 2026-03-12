from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .results_builder import safe_upper_letter, LETTER_SCORES, rebuild_grid, MADNESS_BONUS_DEFAULT

def find_madness_path(grid_rows: List[str], word: str) -> List[Tuple[int, int]]:
    """Find an 8-neighbor contiguous path that spells `word` (lowercase) over `grid_rows`.
    Returns a list of (r,c) coordinates if found, otherwise an empty list.
    """
    try:
        target = (word or "").strip().lower()
        if not target:
            return []
        R = len(grid_rows)
        if R == 0:
            return []
        C = len(grid_rows[0])
        dirs = [(-1,-1),(-1,0),(-1,1),(0,-1),(0,1),(1,-1),(1,0),(1,1)]
        visited = [[False]*C for _ in range(R)]
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
                if dfs(r+dr, c+dc, i+1):
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

def detect_madness(meta: Dict[str, Any], cells: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute Margana Madness metadata for a given grid.

    Returns a dict with keys:
      - madnessFound: bool
      - madnessWord: str | None
      - madnessPath: List[List[int]] | None (list of [r,c])
      - madnessScore: int | None (sum of letter scores along path)
      - coords_rc: internal coordinates list (List[Tuple[int,int]])
    """
    meta_in = meta or {}
    cells_in = cells or []
    grid_rows = rebuild_grid(meta_in, cells_in)

    # Prefer provided madness path/word if present in input meta
    coords_rc: List[Tuple[int, int]] | None = None
    madness_word: str | None = None
    raw_path = meta_in.get("madnessPath") or meta_in.get("madness_path")
    if isinstance(raw_path, list) and raw_path:
        coords_rc = []
        for p in raw_path:
            if isinstance(p, (list, tuple)) and len(p) == 2:
                r, c = int(p[0]), int(p[1])
                coords_rc.append((r, c))
            elif isinstance(p, dict) and "r" in p and "c" in p:
                coords_rc.append((int(p["r"]), int(p["c"])) )
    mw = meta_in.get("madnessWord") or meta_in.get("madness_word")
    if isinstance(mw, str) and mw.strip():
        madness_word = mw.strip().lower()

    if not coords_rc or not madness_word:
        for candidate in ("margana", "anagram"):
            path = find_madness_path(grid_rows, candidate)
            if path:
                coords_rc = path
                madness_word = candidate
                break

    if coords_rc and madness_word:
        letters_seq = [grid_rows[r][c] for (r, c) in coords_rc]
        letter_scores = [int(LETTER_SCORES.get(ch.lower(), 0)) for ch in letters_seq]
        letter_sum = int(sum(letter_scores))
        return {
            "madnessFound": True,
            "madnessWord": str(madness_word).lower(),
            "madnessPath": [[int(r), int(c)] for (r, c) in coords_rc],
            "madnessScore": int(letter_sum),
            "coords_rc": coords_rc,
        }
    else:
        return {
            "madnessFound": False,
            "madnessWord": None,
            "madnessPath": None,
            "madnessScore": None,
            "coords_rc": [],
        }


def integrate_madness(payload: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich a base results payload with Margana Madness info when applicable.

    - Reads body.meta.madnessAvailable to decide whether to run detection.
    - If a path is found, appends a 'madness' item to valid_words_metadata when missing.
    - Updates payload.meta.madnessFound/madnessWord/madnessPath/madnessScore accordingly.
    Returns the modified payload (also modifies in place).
    """
    try:
        if not isinstance(body, dict):
            return payload
        meta_in = (body.get("meta") or {}) if isinstance(body, dict) else {}
        try:
            madness_flag = bool(meta_in.get("madnessAvailable") or meta_in.get("madness_available"))
        except Exception:
            madness_flag = False
        if not madness_flag:
            # Ensure flag exists in response meta
            pm = payload.get("meta") if isinstance(payload, dict) else {}
            if not isinstance(pm, dict):
                pm = {}
            pm["madnessFound"] = False
            pm["madnessAvailable"] = False
            payload["meta"] = pm
            return payload

        # Compute detection, but mask invalid rows so Madness cannot use them.
        # Determine invalid rows from payload.row_summaries and exclude their cells from detection input.
        row_summaries = []
        try:
            row_summaries = payload.get("row_summaries") or []
        except Exception:
            row_summaries = []

        bad_rows = set()
        try:
            for rs in row_summaries or []:
                try:
                    if not bool(rs.get("valid")):
                        bad_rows.add(int(rs.get("row")))
                except Exception:
                    continue
        except Exception:
            bad_rows = set()

        cells_in = (body.get("cells") or []) if isinstance(body, dict) else []
        if bad_rows:
            masked_cells = []
            for cell in cells_in:
                try:
                    r = int(cell.get("r"))
                except Exception:
                    # Exclude unparseable cells conservatively
                    continue
                if r in bad_rows:
                    # Skip cells on invalid rows
                    continue
                masked_cells.append(cell)
            det = detect_madness(meta_in, masked_cells)
        else:
            det = detect_madness(meta_in, cells_in)

        pm = payload.get("meta") if isinstance(payload, dict) else {}
        if not isinstance(pm, dict):
            pm = {}
        pm["madnessAvailable"] = True
        pm["madnessFound"] = bool(det.get("madnessFound"))
        if det.get("madnessFound"):
            pm["madnessWord"] = det.get("madnessWord")
            pm["madnessPath"] = det.get("madnessPath")
            pm["madnessScore"] = int(det.get("madnessScore") or 0)
            payload["meta"] = pm

            # Append a metadata item if not already present
            coords_rc = det.get("coords_rc") or []
            vwm = payload.get("valid_words_metadata")
            if not isinstance(vwm, list):
                vwm = []
            already = any((str(it.get("type") or "").lower() == "madness") for it in vwm)
            if not already and coords_rc:
                # We need grid_rows to build letter breakdown
                # Use the same masking policy so metadata letters/scores match detection
                effective_cells = (body.get("cells") or [])
                if bad_rows:
                    eff = []
                    for cell in effective_cells:
                        try:
                            r = int(cell.get("r"))
                            if r in bad_rows:
                                continue
                            eff.append(cell)
                        except Exception:
                            continue
                    effective_cells = eff
                grid_rows = rebuild_grid(meta_in, effective_cells)
                letters_seq = [grid_rows[r][c] for (r, c) in coords_rc]
                letter_scores = [int(LETTER_SCORES.get(ch.lower(), 0)) for ch in letters_seq]
                letter_sum = int(sum(letter_scores))
                # Build letter_value mapping (unique letters -> score)
                letter_value = {}
                try:
                    for ch in {ch.lower() for ch in letters_seq}:
                        letter_value[ch] = int(LETTER_SCORES.get(ch, 0))
                except Exception:
                    letter_value = {}
                base_score = int(letter_sum)
                bonus = int(MADNESS_BONUS_DEFAULT)
                total_score = int(base_score + bonus)
                item = {
                    "word": str(det.get("madnessWord") or "margana"),
                    "type": "madness",
                    "index": 0,
                    "direction": "path",
                    "coords": [{"r": int(r), "c": int(c)} for (r, c) in coords_rc],
                    "start_index": {"r": int(coords_rc[0][0]), "c": int(coords_rc[0][1])} if coords_rc else None,
                    "end_index": {"r": int(coords_rc[-1][0]), "c": int(coords_rc[-1][1])} if coords_rc else None,
                    "letters": [ch.lower() for ch in letters_seq],
                    "letter_scores": letter_scores,
                    "letter_sum": letter_sum,
                    # Legacy/compat fields aligned with other valid_words_metadata items
                    "palindrome": False,
                    "semordnilap": False,
                    "letter_value": letter_value,
                    "base_score": base_score,
                    "bonus": bonus,
                    "score": total_score,
                }
                vwm.append(item)
                payload["valid_words_metadata"] = vwm
        else:
            payload["meta"] = pm
        # Recompute total score to include the newly added Madness word
        try:
            vwm = payload.get("valid_words_metadata") or []
            if isinstance(vwm, list):
                # Simply sum every word score in the metadata list
                total = sum(int(it.get("score") or 0) for it in vwm if isinstance(it, dict))
                payload["total_score"] = int(total)
        except Exception:
            pass
        return payload
    except Exception:
        # Keep payload unchanged on error
        return payload