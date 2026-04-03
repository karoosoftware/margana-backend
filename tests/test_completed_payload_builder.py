from __future__ import annotations

from margana_gen.completed_payload_builder import build_completed_payload


def test_build_completed_payload_preserves_expected_shape():
    payload = build_completed_payload(
        saved_at="2026-04-01T12:00:00Z",
        date="2026-04-02",
        vertical_target_word="aloes",
        column_index=4,
        diagonal_direction="main",
        diagonal_target_word="mocks",
        rows=["mamba", "vowel", "wacko", "quake", "draws"],
        longest_anagram="boardwalks",
        valid_words={"rows": {"lr": ["mamba"]}},
        valid_words_metadata=[{"word": "mamba", "type": "row", "direction": "lr"}],
        total_score=321,
        meta_extra={"madnessAvailable": False, "difficultyBandApplied": "hard"},
    )

    assert payload == {
        "saved_at": "2026-04-01T12:00:00Z",
        "user": {
            "sub": "margana",
            "username": "margana",
            "email": None,
            "issuer": "generator",
            "identity_provider": None,
        },
        "diagonal_direction": "main",
        "diagonal_target_word": "mocks",
        "meta": {
            "date": "2026-04-02",
            "rows": 5,
            "cols": 5,
            "wordLength": 5,
            "columnIndex": 4,
            "diagonalDirection": "main",
            "verticalTargetWord": "aloes",
            "diagonalTargetWord": "mocks",
            "longestAnagram": "boardwalks",
            "longestAnagramCount": 10,
            "userAnagram": None,
            "madnessAvailable": False,
            "difficultyBandApplied": "hard",
        },
        "grid_rows": ["mamba", "vowel", "wacko", "quake", "draws"],
        "valid_words": {"rows": {"lr": ["mamba"]}},
        "valid_words_metadata": [{"word": "mamba", "type": "row", "direction": "lr"}],
        "total_score": 321,
    }
