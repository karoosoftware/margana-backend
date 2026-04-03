from __future__ import annotations

from datetime import date, timedelta

from margana_gen import usage_log


def test_record_puzzle_and_puzzle_in_cooldown():
    level_log: dict = {"puzzles": {}}
    today = date.today().isoformat()

    usage_log.record_puzzle(level_log, "puzzle-123", today)

    assert level_log["puzzles"]["puzzle-123"] == today
    assert usage_log.puzzle_in_cooldown(level_log, "puzzle-123", cooldown_days=365) is True


def test_puzzle_in_cooldown_is_false_for_old_entry():
    old_date = (date.today() - timedelta(days=366)).isoformat()
    level_log = {"puzzles": {"puzzle-123": old_date}}

    assert usage_log.puzzle_in_cooldown(level_log, "puzzle-123", cooldown_days=365) is False


def test_select_fresh_pair_for_chain_honors_existing_usage_entries(monkeypatch):
    usage = {
        "hard": {
            "pairs": {"alpha-omega": date.today().isoformat()},
            "last_used_start": {},
            "last_used_end": {},
            "chains": {},
            "puzzles": {},
        }
    }

    monkeypatch.setattr(usage_log, "_today_iso", lambda: "2026-04-03")

    result = usage_log.select_fresh_pair_for_chain(
        "hard",
        usage,
        ["alpha", "mid", "omega"],
        cooldown_days=365,
    )

    assert result is None


def test_select_fresh_pair_for_chain_records_when_not_in_cooldown(monkeypatch):
    usage = {"hard": {"pairs": {}, "last_used_start": {}, "last_used_end": {}, "chains": {}, "puzzles": {}}}
    monkeypatch.setattr(usage_log, "_today_iso", lambda: "2026-04-03")

    result = usage_log.select_fresh_pair_for_chain(
        "hard",
        usage,
        ["alpha", "mid", "omega"],
        cooldown_days=365,
    )

    assert result == ("alpha", "omega")
    assert usage["hard"]["pairs"]["alpha-omega"] == "2026-04-03"
    assert usage["hard"]["last_used_start"]["alpha"] == "2026-04-03"
    assert usage["hard"]["last_used_end"]["omega"] == "2026-04-03"
