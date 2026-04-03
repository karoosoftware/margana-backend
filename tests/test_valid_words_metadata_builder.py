from __future__ import annotations

from margana_gen.valid_words_metadata_builder import (
    build_valid_words_metadata,
    enrich_valid_words_metadata,
)


def test_build_valid_words_metadata_normalizes_items_and_appends_anagram():
    items = [
        {
            "word": "views",
            "type": "row",
            "index": 3,
            "direction": "lr",
            "start_index": {"r": 0, "c": 0},
            "end_index": {"r": 0, "c": 4},
            "extra": "ignored",
        }
    ]

    result = build_valid_words_metadata(items, longest_anagram="stapler")

    assert result == [
        {
            "word": "views",
            "type": "row",
            "index": 3,
            "direction": "lr",
            "start_index": {"r": 0, "c": 0},
            "end_index": {"r": 0, "c": 4},
        },
        {
            "word": "stapler",
            "type": "anagram",
            "index": 0,
            "direction": "builder",
            "start_index": None,
            "end_index": None,
        },
    ]


def test_enrich_valid_words_metadata_adds_scoring_letter_and_word_flags():
    items = [
        {"word": "level", "type": "row", "direction": "lr"},
        {"word": "drawer", "type": "diagonal", "direction": "main"},
        {"word": "reward", "type": "diagonal", "direction": "main_rev"},
        {"word": "stapler", "type": "anagram", "direction": "builder"},
    ]
    letter_scores = {chr(ord("a") + idx): idx + 1 for idx in range(26)}

    enrich_valid_words_metadata(
        items,
        letter_scores=letter_scores,
        score_word=lambda word: sum(letter_scores.get(ch, 0) for ch in word.lower()),
        semordnilap_words=["drawer", "reward"],
    )

    assert items[0]["palindrome"] is True
    assert items[0]["semordnilap"] is False
    assert items[0]["letters"] == ["l", "e", "v", "e", "l"]
    assert items[0]["bonus"] == 0
    assert items[0]["score"] == items[0]["base_score"] == 112

    assert items[1]["palindrome"] is False
    assert items[1]["semordnilap"] is True
    assert items[1]["letter_sum"] == 69
    assert items[1]["score"] == 69

    assert items[3]["palindrome"] is False
    assert items[3]["semordnilap"] is False
    assert items[3]["letter_scores"] == [19, 20, 1, 16, 12, 5, 18]


def test_diagonal_items_contribute_to_total_with_palindrome_and_semordnilap_rules():
    items = [
        {"word": "level", "type": "diagonal", "direction": "main"},
        {"word": "drawer", "type": "diagonal", "direction": "anti"},
        {"word": "reward", "type": "diagonal", "direction": "anti_rev"},
    ]
    letter_scores = {chr(ord("a") + idx): idx + 1 for idx in range(26)}
    score_word = lambda word: sum(letter_scores.get(ch, 0) for ch in word.lower())

    enrich_valid_words_metadata(
        items,
        letter_scores=letter_scores,
        score_word=score_word,
        semordnilap_words=["drawer", "reward", "level"],
    )

    total = sum(int(item["score"]) for item in items)

    assert items[0]["type"] == "diagonal"
    assert items[0]["palindrome"] is True
    assert items[0]["score"] == score_word("level") * 2

    assert items[1]["semordnilap"] is True
    assert items[2]["semordnilap"] is True
    assert items[1]["score"] == score_word("drawer")
    assert items[2]["score"] == score_word("reward")

    assert total == (score_word("level") * 2) + score_word("drawer") + score_word("reward")
