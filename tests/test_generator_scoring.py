from __future__ import annotations

from margana_gen.generator_scoring import compute_basic_total, make_score_word


def test_make_score_word_normalizes_keys_and_handles_empty_input():
    score_word = make_score_word({"A": 1, "b": 3, "C": 5, "skip": "x"})

    assert score_word("abc") == 9
    assert score_word("") == 0


def test_compute_basic_total_sums_rows_targets_and_anagram():
    score_word = make_score_word({chr(ord("a") + idx): idx + 1 for idx in range(26)})

    total = compute_basic_total(
        rows=["abcde", "fghij"],
        target="klmno",
        diag="pqrst",
        longest_anagram="uvwxy",
        score_word=score_word,
    )

    assert total == 15 + 40 + 65 + 90 + 115
