"""
Shared scoring/validation package for Margana Lambdas.

This package now expects callers to resolve and pass the word list path
explicitly to the builder functions. The word list utilities live in
`wordlist.py`, focused solely on loading words from a provided path.
"""
from .wordlist import (
    load_wordlist,
)
from .results_builder import (
    build_results_response,
    remove_pre_loaded_words,
    rebuild_grid,
)

from .s3_utils import (
    download_word_list_from_s3
)

from .gen_utilis import (
    require_env
)

from .wordlist_loader import (
    get_bundled_wordlist_path
)

from .margana_madness import (
    integrate_madness,
    detect_madness,
    find_madness_path,
)
__all__ = [
    "load_wordlist",
    "build_results_response",
    "rebuild_grid",
    "download_word_list_from_s3",
    "require_env",
    "get_bundled_wordlist_path",
    "remove_pre_loaded_words",
    "integrate_madness",
    "detect_madness",
    "find_madness_path",
]
