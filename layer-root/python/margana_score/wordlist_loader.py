# margana_score/wordlist_loader.py
from __future__ import annotations

from importlib import resources
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def get_bundled_wordlist_path() -> Path:
    """
    Return the on-disk path to the bundled word list file.

    This lives inside the deployed Lambda package under margana_score/data/.
    """
    data_dir = resources.files("margana_score.data")
    wordlist_path = data_dir.joinpath("margana-word-list.txt")
    logger.info("Using bundled word list at %s", wordlist_path)
    return Path(wordlist_path)
