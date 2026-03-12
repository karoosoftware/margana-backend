import pytest

from margana_score.results_builder import exclude_items_touching_invalid_rows


def cell(r, c):
    return {"r": r, "c": c}


def item(word, typ, s, e, direction="lr", index=0):
    return {
        "word": word,
        "type": typ,
        "index": index,
        "direction": direction,
        "start_index": s,
        "end_index": e,
    }


def test_exclude_items_that_touch_invalid_rows():
    # Build three items:
    # 1) A diagonal crossing rows 1..3 (touches invalid row 2) → should be removed
    diag = item(
        "had",
        "diagonal",
        cell(1, 1),  # start
        cell(3, 3),  # end (down-right)
        direction="main",
        index=0,
    )

    # 2) A row item on row 2 (invalid) → should be removed
    row_bad = item(
        "drgdn",
        "row",
        cell(2, 0),
        cell(2, 4),
        direction="lr",
        index=2,
    )

    # 3) A row item fully on a valid row (row 0) → should be kept
    row_good = item(
        "skulk",
        "row",
        cell(0, 0),
        cell(0, 4),
        direction="lr",
        index=0,
    )

    items = [diag, row_bad, row_good]

    row_summaries = [
        {"row": 0, "valid": True},
        {"row": 1, "valid": True},
        {"row": 2, "valid": False},  # invalid row should cause filtering
        {"row": 3, "valid": True},
    ]

    kept = exclude_items_touching_invalid_rows(items, row_summaries)

    # Only the good row should remain
    words = sorted([it["word"] for it in kept])
    assert words == ["skulk"]


def test_exclude_items_that_touch_skipped_rows_even_if_row_marked_valid():
    # A wildcard-skipped row should exclude any crossing column/diagonal from scoring.
    diag = item(
        "had",
        "diagonal",
        cell(1, 1),
        cell(3, 3),
        direction="main",
        index=0,
    )
    col = item(
        "goads",
        "column",
        cell(0, 2),
        cell(4, 2),
        direction="tb",
        index=2,
    )
    row_good = item(
        "skulk",
        "row",
        cell(0, 0),
        cell(0, 4),
        direction="lr",
        index=0,
    )
    items = [diag, col, row_good]

    # Row 2 is skipped via wildcard bypass: valid=true for submit flow, but should
    # still exclude crossing words from score computation.
    row_summaries = [
        {"row": 0, "valid": True, "skipped": False},
        {"row": 1, "valid": True, "skipped": False},
        {"row": 2, "valid": True, "skipped": True},
        {"row": 3, "valid": True, "skipped": False},
        {"row": 4, "valid": True, "skipped": False},
    ]

    kept = exclude_items_touching_invalid_rows(items, row_summaries)
    words = sorted([it["word"] for it in kept])
    assert words == ["skulk"]
