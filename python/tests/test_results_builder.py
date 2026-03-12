import json
import pathlib
import sys
from typing import List, Tuple


# Ensure the package path (project/python) is importable
THIS_FILE = pathlib.Path(__file__).resolve()
PYTHON_DIR = THIS_FILE.parents[1]  # .../project/python
TEST_DIR = PYTHON_DIR / "tests"

if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

from margana_score import build_results_response

# Explicit test word list path (tests drive the path; validation no longer resolves it)
WORDLIST_TEST_PATH = TEST_DIR / "resources" / "margana-word-list.txt"


def load_event_body(resource_rel_path: str) -> dict:
    """
    Load an API Gateway-style event JSON from resources and return the parsed body dict.
    resource_rel_path is relative to the project root, e.g. "resources/events/live_score_event.json".
    """
    resource_path = TEST_DIR / resource_rel_path
    raw = json.loads(resource_path.read_text())
    return raw.get("body")


def expected_row_words_from_cells(body: dict) -> List[str]:
    """Reconstruct row strings mirroring results_builder.rebuild_grid behavior.

    - Returns fixed-width lowercase strings per row.
    - Uses a space character for any missing/blank cell to preserve width.
    """
    meta = body.get("meta") or {}
    cells = body.get("cells") or []
    rows = int(meta.get("rows") or 0)
    cols = int(meta.get("cols") or 0)
    if rows <= 0 or cols <= 0:
        for cell in cells:
            r = int(cell.get("r") or 0)
            c = int(cell.get("c") or 0)
            rows = max(rows, r + 1)
            cols = max(cols, c + 1)
    grid = [[" " for _ in range(cols)] for _ in range(rows)]
    for cell in cells:
        r = int(cell.get("r") or 0)
        c = int(cell.get("c") or 0)
        ch = str(cell.get("letter") or "").strip().lower()[:1]
        grid[r][c] = ch if ch.isalpha() else " "
    return ["".join(ch if ch else " " for ch in row) for row in grid]


# Parameterized list of resource event files to test. Add more entries easily.
EVENT_CASES: List[Tuple[str, str]] = [
    (
        "live_score_event",
        "resources/live_score_event.json",
    ),
    (
        "live_score_event_1word",
        "resources/live_score_event_1word.json",
    ),
    (
        "live_score_event_2words",
        "resources/live_score_event_2words.json",
    ),
    (
        "live_score_event_3words",
        "resources/live_score_event_3words.json",
    ),
    (
        "live_score_event_4words",
        "resources/live_score_event_4words.json",
    ),
]

# Optional: mapping of case name to expected full response JSON (for exact comparison)
EXPECTED_RESPONSE_BY_CASE = {
    "live_score_event": TEST_DIR / "resources/expected_live_score_response.json",
    "live_score_event_1word": TEST_DIR / "resources/expected_live_score_response_1word.json",
    "live_score_event_2words": TEST_DIR / "resources/expected_live_score_response_2words.json",
    "live_score_event_3words": TEST_DIR / "resources/expected_live_score_response_3words.json",
    "live_score_event_4words": TEST_DIR / "resources/expected_live_score_response_4words.json",
}


def test_build_results_response_smoke_and_shape():
    for case_name, rel_path in EVENT_CASES:
        print(f"\n--- Running case: {case_name} ({rel_path}) ---")
        body = load_event_body(rel_path)
        resp = build_results_response(body, WORDLIST_TEST_PATH)

        # Basic shape assertions
        assert isinstance(resp, dict), f"{case_name}: response should be a dict"
        for key in (
            "meta",
            "valid_words_metadata",
            "total_score",
            "skippedRows",
            "row_summaries",
            "saved",
            "valid_words",
        ):
            assert key in resp, f"{case_name}: missing key {key}"

        # Totals are non-negative integers
        assert isinstance(resp["total_score"], int) and resp["total_score"] >= 0

        # Row summaries match the grid rows count and words
        expected_rows = expected_row_words_from_cells(body)
        row_summaries = resp.get("row_summaries") or []
        assert len(row_summaries) == len(expected_rows), (
            f"{case_name}: row_summaries length mismatch; expected {len(expected_rows)}"
        )
        for i, row_summary in enumerate(row_summaries):
            assert row_summary.get("row") == i, f"{case_name}: row index mismatch at {i}"
            assert row_summary.get("skipped") in (True, False)
            assert row_summary.get("word") == expected_rows[i]
            # valid is a boolean; score is int >= 0
            assert isinstance(row_summary.get("valid"), bool)
            score_val = row_summary.get("score")
            assert isinstance(score_val, int) and score_val >= 0

        # Saved shape for live mode
        saved = resp.get("saved") or {}
        assert set(saved.keys()) == {"bucket", "key", "uploaded"}
        assert saved.get("uploaded") is False


def test_build_results_response_matches_expected():
    """Compare the full generated response to an expected JSON payload for readability."""
    for case_name, rel_path in EVENT_CASES:
        body = load_event_body(rel_path)
        resp = build_results_response(body, WORDLIST_TEST_PATH)

        expected_path = EXPECTED_RESPONSE_BY_CASE.get(case_name)
        if not expected_path or not expected_path.exists():
            # If no expected file is provided for a case, skip strict comparison.
            continue
        expected = json.loads(expected_path.read_text())

        # Some fixtures may intentionally omit `valid_words` to mirror lean HTTP/S3 schemas.
        # The offline builder still returns `valid_words` for debugging. If the expected
        # payload does not include it, ignore this field for the purpose of strict equality.
        resp_to_check = dict(resp)
        if "valid_words" not in expected and "valid_words" in resp_to_check:
            resp_to_check.pop("valid_words", None)

        # Direct equality is intended (after normalization); keep expected file updated if schema changes.
        assert resp_to_check == expected, (
            f"Response for {case_name} does not match expected.\n"
            f"Expected:\n{json.dumps(expected, indent=2, sort_keys=True)}\n\n"
            f"Actual:\n{json.dumps(resp_to_check, indent=2, sort_keys=True)}"
        )
