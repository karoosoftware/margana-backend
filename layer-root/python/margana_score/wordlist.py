# wordlist.py (or whatever module this lives in)

from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

# ---- in-memory cache (per Lambda container) ----
_CACHED_PATH: str | None = None
_CACHED_BY_LEN: Dict[int, List[str]] | None = None
_CACHED_ALL_WORDS: List[str] | None = None


def _fallback_load_words(path: str) -> Tuple[Dict[int, List[str]], List[str]]:
    try:
        p = Path(path)
        words: List[str] = []
        for line in p.read_text(encoding="utf-8").splitlines():
            w = "".join(ch for ch in line.strip() if ch.isalpha()).lower()
            if w:
                words.append(w)
        by_len: Dict[int, List[str]] = {}
        for w in words:
            by_len.setdefault(len(w), []).append(w)
        return by_len, words
    except Exception:
        logger.exception("Failed to load words from %s", path)
        return {}, []


def load_wordlist(path: str) -> Tuple[Dict[int, List[str]], List[str]]:
    """
    Load words from a file robustly, with in-memory caching.
    If called multiple times with the same path in the same Lambda container,
    the file is parsed only once.
    """
    global _CACHED_PATH, _CACHED_BY_LEN, _CACHED_ALL_WORDS

    # Fast path: already loaded this file in this container
    if (
        _CACHED_PATH == path
        and _CACHED_BY_LEN is not None
        and _CACHED_ALL_WORDS is not None
    ):
        return _CACHED_BY_LEN, _CACHED_ALL_WORDS

    try:
        by_len, all_words = _fallback_load_words(path)
    except Exception:
        # keep previous behaviour: never raise, just return empty
        logger.exception("Unexpected error in load_wordlist(%s)", path)
        return {}, []

    _CACHED_PATH = path
    _CACHED_BY_LEN = by_len
    _CACHED_ALL_WORDS = all_words

    logger.info(
        "Loaded %d words from %s into memory (distinct lengths: %s)",
        len(all_words),
        path,
        sorted(by_len.keys()),
    )

    return by_len, all_words
