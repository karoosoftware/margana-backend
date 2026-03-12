import json
import hashlib
from datetime import datetime
from pathlib import Path


def _today_iso():
    return datetime.today().date().isoformat()


def load_usage_log(path):
    """
    Structure (historical examples; actual keys are added lazily by callers):
    {
      "easy":   {"pairs": {...}, "last_used_start": {...}, "last_used_end": {...}, "chains": {}, "puzzles": {}},
      "medium": {...},
      "hard":   {...}
    }

    This loader is intentionally robust:
    - Missing file -> returns {}.
    - Empty/whitespace-only file -> returns {}.
    - Invalid/corrupted JSON -> returns {}.
    """
    p = Path(path)
    try:
        if not p.exists():
            return {}
        text = p.read_text(encoding="utf-8")
        if not text.strip():
            # Empty file; treat as an empty usage log
            return {}
        return json.loads(text)
    except Exception:
        # Any parse/read error -> treat as empty usage log
        return {}


def save_usage_log(log, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2)


def _is_older_than(date_iso: str, days: int) -> bool:
    try:
        d = datetime.strptime(date_iso, "%Y-%m-%d").date()
    except Exception:
        return True
    return (datetime.today().date() - d).days >= days


def in_cooldown(date_iso: str, cooldown_days: int) -> bool:
    return not _is_older_than(date_iso, cooldown_days)


def start_or_end_in_cooldown(level_log, word: str, is_start=True, cooldown_days=365):
    last_map = level_log["last_used_start"] if is_start else level_log["last_used_end"]
    last = last_map.get(word)
    return in_cooldown(last, cooldown_days) if last else False


def pair_in_cooldown(level_log, start: str, end: str, cooldown_days: int) -> bool:
    key = f"{start}-{end}"
    last = level_log["pairs"].get(key)
    return in_cooldown(last, cooldown_days) if last else False


def record_pair(level_log, start: str, end: str, date_iso: str):
    key = f"{start}-{end}"
    level_log["pairs"][key] = date_iso
    level_log["last_used_start"][start] = date_iso
    level_log["last_used_end"][end] = date_iso


def select_fresh_pair_for_chain(level_name: str, usage_log: dict, chain: list[str], cooldown_days: int = 365):
    """
    Pick (start, end) from the given chain for the specified level if they are not in cooldown
    individually and as a pair. If eligible, record the usage and return (start, end). Otherwise return None.
    """
    if not chain:
        return None
    level_log = usage_log.get(level_name)
    if level_log is None:
        # Initialize level slot if missing (defensive)
        usage_log[level_name] = {"pairs": {}, "last_used_start": {}, "last_used_end": {}, "chains": {}, "puzzles": {}}
        level_log = usage_log[level_name]

    start = chain[0]
    end = chain[-1]

    if start_or_end_in_cooldown(level_log, start, is_start=True, cooldown_days=cooldown_days):
        return None
    if start_or_end_in_cooldown(level_log, end, is_start=False, cooldown_days=cooldown_days):
        return None
    if pair_in_cooldown(level_log, start, end, cooldown_days):
        return None

    record_pair(level_log, start, end, _today_iso())
    return (start, end)


# --- Chain & Puzzle IDs (fixed mode) ---

def chain_hash(chain: list[str]) -> str:
    """Short, stable ID for a chain."""
    h = hashlib.sha1(">".join(chain).encode("utf-8")).hexdigest()
    return h[:10]


def puzzle_hash(chain: list[str], anagram=None) -> str:
    """
    Composite ID = hash(chain + '||' + anagram). If anagram is None, it's just the chain.
    """
    base = ">".join(chain)
    if anagram:
        base = base + "||" + anagram
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:10]


def chain_in_cooldown(level_log, chain_id: str, cooldown_days: int) -> bool:
    last = level_log.get("chains", {}).get(chain_id)
    return in_cooldown(last, cooldown_days) if last else False


def record_chain(level_log, chain_id: str, date_iso: str):
    level_log.setdefault("chains", {})
    level_log["chains"][chain_id] = date_iso


def puzzle_in_cooldown(level_log, puzzle_id: str, cooldown_days: int) -> bool:
    last = level_log.get("puzzles", {}).get(puzzle_id)
    return in_cooldown(last, cooldown_days) if last else False


def record_puzzle(level_log, puzzle_id: str, date_iso: str):
    level_log.setdefault("puzzles", {})
    level_log["puzzles"][puzzle_id] = date_iso
