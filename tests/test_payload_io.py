from __future__ import annotations

import json

from margana_gen.payload_io import write_payload_pair


def test_write_payload_pair_writes_both_json_files(tmp_path):
    completed = {"foo": "bar", "kind": "completed"}
    semi = {"foo": "baz", "kind": "semi"}

    completed_path, semi_path = write_payload_pair(
        tmp_path,
        completed_payload=completed,
        semi_completed_payload=semi,
    )

    assert completed_path.name == "margana-completed.json"
    assert semi_path.name == "margana-semi-completed.json"
    assert json.loads(completed_path.read_text(encoding="utf-8")) == completed
    assert json.loads(semi_path.read_text(encoding="utf-8")) == semi
