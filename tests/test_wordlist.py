# test_wordlist.py
from pathlib import Path

import pytest

from margana_score.wordlist import _fallback_load_words, load_wordlist


def test_fallback_load_words_happy_path(tmp_path: Path):
    word_file = tmp_path / "words.txt"
    word_file.write_text(
        "Apple\n"
        "banana \n"
        "  CARROT!!!\n"
        "1234\n"              # no letters -> ignored
        "\n"                  # empty -> ignored
        "Mixed-Case Word\n",  # non-alpha removed, lowercased
        encoding="utf-8",
    )

    by_len, all_words = _fallback_load_words(str(word_file))

    # All words cleaned + lowercased, only alpha chars kept
    assert all_words == ["apple", "banana", "carrot", "mixedcaseword"]

    # Grouped by length correctly and in order of appearance
    assert by_len == {
        5: ["apple"],
        6: ["banana", "carrot"],
        13: ["mixedcaseword"],
    }


def test_fallback_load_words_nonexistent_file_returns_empty(tmp_path: Path):
    non_existent = tmp_path / "no_such_file.txt"
    by_len, all_words = _fallback_load_words(str(non_existent))

    assert by_len == {}
    assert all_words == []


def test_fallback_load_words_directory_instead_of_file_returns_empty(tmp_path: Path):
    # Passing a directory should cause read_text to fail -> handled as empty
    dir_path = tmp_path / "some_dir"
    dir_path.mkdir()

    by_len, all_words = _fallback_load_words(str(dir_path))

    assert by_len == {}
    assert all_words == []


def test_load_wordlist_delegates_to_fallback(tmp_path: Path):
    word_file = tmp_path / "words.txt"
    word_file.write_text("One\nTwo\nThree\n", encoding="utf-8")

    by_len_fallback, all_words_fallback = _fallback_load_words(str(word_file))
    by_len, all_words = load_wordlist(str(word_file))

    # load_wordlist should just return whatever _fallback_load_words does
    assert (by_len, all_words) == (by_len_fallback, all_words_fallback)


def test_load_wordlist_never_raises_for_missing_file(tmp_path: Path):
    non_existent = tmp_path / "no_such_file.txt"

    # Should not raise; should return empty structures
    by_len, all_words = load_wordlist(str(non_existent))

    assert by_len == {}
    assert all_words == []
