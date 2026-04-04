from __future__ import annotations

import json
import types
from pathlib import Path
import subprocess

from ecs import main as ecs_main


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    def __init__(self, payloads: dict[str, bytes]):
        self.payloads = payloads

    def get_object(self, Bucket: str, Key: str):
        return {"Body": _FakeBody(self.payloads[Key])}


def _write_completed_payloads(root: Path, count: int) -> None:
    start_day = 30
    for offset in range(count):
        day = start_day + offset
        month = "03" if day <= 31 else "04"
        dom = day if day <= 31 else day - 31
        payload_root = root / "public" / "daily-puzzles" / "2026" / month / f"{dom:02d}"
        payload_root.mkdir(parents=True, exist_ok=True)
        payload_root.joinpath("margana-completed.json").write_text(
            json.dumps(
                {
                    "total_score": 174 + offset,
                    "meta": {
                        "date": f"2026-{month}-{dom:02d}",
                        "difficultyBandApplied": "easy",
                        "madnessAvailable": False,
                    },
                }
            ),
            encoding="utf-8",
        )


def test_download_static_assets_writes_expected_files(tmp_path, monkeypatch):
    payloads = {
        "margana-word-list.txt": b"alpha\nbravo\n",
        "horizontal-exclude-words.txt": b"avoid\n",
        "letter-scores-v3.json": b"{\"a\": 1}\n",
    }

    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: _FakeS3Client(payloads))
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    downloaded = ecs_main.download_static_assets(
        bucket_name="margana-static-assets-preprod",
        region_name="eu-west-1",
        assets_dir=str(tmp_path),
        word_list_key="margana-word-list.txt",
        horizontal_exclude_key="horizontal-exclude-words.txt",
        letter_scores_key="letter-scores-v3.json",
    )

    assert Path(downloaded["word_list"]).read_text(encoding="utf-8") == "alpha\nbravo\n"
    assert Path(downloaded["horizontal_exclude"]).read_text(encoding="utf-8") == "avoid\n"
    assert Path(downloaded["letter_scores"]).read_text(encoding="utf-8") == "{\"a\": 1}\n"


def test_main_download_static_assets_mode_downloads_and_exits(tmp_path, monkeypatch, capsys):
    called = {}

    def fake_download_static_assets(**kwargs):
        called.update(kwargs)
        return {"word_list": str(tmp_path / "margana-word-list.txt")}

    monkeypatch.setattr(ecs_main, "download_static_assets", fake_download_static_assets)

    ecs_main.main(
        [
            "--download-static-assets",
            "--assets-dir",
            str(tmp_path),
            "--static-assets-bucket",
            "margana-static-assets-preprod",
        ]
    )

    out = capsys.readouterr().out
    assert called["bucket_name"] == "margana-static-assets-preprod"
    assert called["assets_dir"] == str(tmp_path)
    assert "Static assets downloaded successfully." in out


def test_stage_assets_for_generator_copies_to_target_root(tmp_path):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    (assets_dir / "margana-word-list.txt").write_text("alpha\n", encoding="utf-8")
    (assets_dir / "horizontal-exclude-words.txt").write_text("avoid\n", encoding="utf-8")
    (assets_dir / "letter-scores-v3.json").write_text("{\"a\":1}\n", encoding="utf-8")

    staged = ecs_main.stage_assets_for_generator(
        {
            "word_list": str(assets_dir / "margana-word-list.txt"),
            "horizontal_exclude": str(assets_dir / "horizontal-exclude-words.txt"),
            "letter_scores": str(assets_dir / "letter-scores-v3.json"),
        },
        str(tmp_path / "target"),
    )

    assert Path(staged["word_list"]).read_text(encoding="utf-8") == "alpha\n"
    assert Path(staged["horizontal_exclude"]).read_text(encoding="utf-8") == "avoid\n"
    assert Path(staged["letter_scores"]).read_text(encoding="utf-8") == "{\"a\":1}\n"


def test_main_runs_generation_pipeline_for_target_week(tmp_path, monkeypatch, capsys):
    _write_completed_payloads(tmp_path / "payloads", 7)

    monkeypatch.setattr(
        ecs_main,
        "download_static_assets",
        lambda **kwargs: {
            "word_list": str(tmp_path / "downloaded-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "downloaded-horizontal.txt"),
            "letter_scores": str(tmp_path / "downloaded-scores.json"),
        },
    )
    monkeypatch.setattr(
        ecs_main,
        "stage_assets_for_generator",
        lambda downloaded_assets, target_root: {
            "word_list": str(tmp_path / "staged-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "staged-horizontal.txt"),
            "letter_scores": str(tmp_path / "staged-scores.json"),
        },
    )
    usage_downloads = []
    usage_uploads = []

    monkeypatch.setattr(
        ecs_main,
        "download_s3_object_to_file",
        lambda bucket_name, object_key, destination_path, region_name: usage_downloads.append(
            (bucket_name, object_key, destination_path, region_name)
        ),
    )
    monkeypatch.setattr(ecs_main, "existing_week_payload_keys_in_s3", lambda **kwargs: [])
    monkeypatch.setattr(
        ecs_main,
        "upload_file_to_s3",
        lambda source_path, bucket_name, object_key, region_name, content_type: usage_uploads.append(
            (source_path, bucket_name, object_key, region_name, content_type)
        ),
    )

    captured = {}

    def fake_run_generation_pipeline(config, *, cwd=None):
        captured["config"] = config
        captured["cwd"] = cwd
        return (
            subprocess.CompletedProcess(["generator"], 0),
            subprocess.CompletedProcess(["validator"], 0),
        )

    monkeypatch.setattr(ecs_main, "run_generation_pipeline", fake_run_generation_pipeline)

    ecs_main.main(["--target-week", "2026-14", "--output-root", str(tmp_path / "payloads")])

    out = capsys.readouterr().out
    config = captured["config"]
    assert config.payload_dir == tmp_path / "payloads"
    assert "--year" in config.generator_args
    assert "2026" in config.generator_args
    assert "--iso-week" in config.generator_args
    assert "14" in config.generator_args
    assert "--words-file" in config.generator_args
    assert str(tmp_path / "staged-word-list.txt") in config.generator_args
    assert "--max-usage-tries" in config.generator_args
    assert "200" in config.generator_args
    assert "--cooldown-days" in config.generator_args
    assert "1826" in config.generator_args
    assert "--no-s3-usage" in config.generator_args
    assert config.validator_args == [
        "--summary-only",
        "--horizontal-exclude-file",
        str(tmp_path / "staged-horizontal.txt"),
    ]
    assert usage_downloads[0][0] == "margana-word-game-preprod"
    assert usage_downloads[0][1] == "usage-logs/margana-puzzle-usage-log.json"
    assert usage_uploads[0][1] == "margana-word-game-preprod"
    payload_uploads = [call for call in usage_uploads if call[1] == "margana-word-game-preprod" and str(call[2]).startswith("public/daily-puzzles/")]
    assert len(payload_uploads) == 7
    assert "Task completed successfully." in out


def test_main_print_payloads_outputs_generated_json(tmp_path, monkeypatch, capsys):
    _write_completed_payloads(tmp_path / "payloads", 7)
    payload_root = tmp_path / "payloads" / "public" / "daily-puzzles" / "2026" / "03" / "30"
    (payload_root / "margana-semi-completed.json").write_text('{"kind":"semi"}', encoding="utf-8")

    monkeypatch.setattr(
        ecs_main,
        "download_static_assets",
        lambda **kwargs: {
            "word_list": str(tmp_path / "downloaded-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "downloaded-horizontal.txt"),
            "letter_scores": str(tmp_path / "downloaded-scores.json"),
        },
    )
    monkeypatch.setattr(
        ecs_main,
        "stage_assets_for_generator",
        lambda downloaded_assets, target_root: {
            "word_list": str(tmp_path / "staged-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "staged-horizontal.txt"),
            "letter_scores": str(tmp_path / "staged-scores.json"),
        },
    )
    monkeypatch.setattr(ecs_main, "download_s3_object_to_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(ecs_main, "upload_file_to_s3", lambda *args, **kwargs: None)
    monkeypatch.setattr(ecs_main, "existing_week_payload_keys_in_s3", lambda **kwargs: [])
    monkeypatch.setattr(
        ecs_main,
        "run_generation_pipeline",
        lambda config, *, cwd=None: (
            subprocess.CompletedProcess(["generator"], 0),
            subprocess.CompletedProcess(["validator"], 0),
        ),
    )

    ecs_main.main(
        [
            "--target-week",
            "2026-14",
            "--output-root",
            str(tmp_path / "payloads"),
            "--print-payloads",
        ]
    )

    out = capsys.readouterr().out
    assert "===== " in out
    assert '"total_score": 174' in out
    assert '"kind": "semi"' in out


def test_main_print_payload_summary_outputs_compact_lines(tmp_path, monkeypatch, capsys):
    _write_completed_payloads(tmp_path / "payloads", 7)

    monkeypatch.setattr(
        ecs_main,
        "download_static_assets",
        lambda **kwargs: {
            "word_list": str(tmp_path / "downloaded-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "downloaded-horizontal.txt"),
            "letter_scores": str(tmp_path / "downloaded-scores.json"),
        },
    )
    monkeypatch.setattr(
        ecs_main,
        "stage_assets_for_generator",
        lambda downloaded_assets, target_root: {
            "word_list": str(tmp_path / "staged-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "staged-horizontal.txt"),
            "letter_scores": str(tmp_path / "staged-scores.json"),
        },
    )
    monkeypatch.setattr(ecs_main, "download_s3_object_to_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(ecs_main, "upload_file_to_s3", lambda *args, **kwargs: None)
    monkeypatch.setattr(ecs_main, "existing_week_payload_keys_in_s3", lambda **kwargs: [])
    monkeypatch.setattr(
        ecs_main,
        "run_generation_pipeline",
        lambda config, *, cwd=None: (
            subprocess.CompletedProcess(["generator"], 0),
            subprocess.CompletedProcess(["validator"], 0),
        ),
    )

    ecs_main.main(
        [
            "--target-week",
            "2026-14",
            "--output-root",
            str(tmp_path / "payloads"),
            "--print-payload-summary",
        ]
    )

    out = capsys.readouterr().out
    assert "PAYLOAD date=2026-03-30 score=174 band=easy madness=False" in out
    assert '"difficultyBandApplied"' not in out


def test_main_fails_when_target_week_is_incomplete(tmp_path, monkeypatch):
    _write_completed_payloads(tmp_path / "payloads", 4)

    monkeypatch.setattr(
        ecs_main,
        "download_static_assets",
        lambda **kwargs: {
            "word_list": str(tmp_path / "downloaded-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "downloaded-horizontal.txt"),
            "letter_scores": str(tmp_path / "downloaded-scores.json"),
        },
    )
    monkeypatch.setattr(
        ecs_main,
        "stage_assets_for_generator",
        lambda downloaded_assets, target_root: {
            "word_list": str(tmp_path / "staged-word-list.txt"),
            "horizontal_exclude": str(tmp_path / "staged-horizontal.txt"),
            "letter_scores": str(tmp_path / "staged-scores.json"),
        },
    )
    monkeypatch.setattr(ecs_main, "download_s3_object_to_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(ecs_main, "existing_week_payload_keys_in_s3", lambda **kwargs: [])
    upload_calls = []
    monkeypatch.setattr(ecs_main, "upload_file_to_s3", lambda *args, **kwargs: upload_calls.append(args))
    monkeypatch.setattr(
        ecs_main,
        "run_generation_pipeline",
        lambda config, *, cwd=None: (
            subprocess.CompletedProcess(["generator"], 0),
            subprocess.CompletedProcess(["validator"], 0),
        ),
    )

    try:
        ecs_main.main(["--target-week", "2026-14", "--output-root", str(tmp_path / "payloads")])
        assert False, "Expected SystemExit for incomplete week"
    except SystemExit as exc:
        assert exc.code == 1

    assert upload_calls == []


def test_main_fails_when_week_already_exists_in_s3(tmp_path, monkeypatch):
    monkeypatch.setattr(
        ecs_main,
        "existing_week_payload_keys_in_s3",
        lambda **kwargs: [
            "public/daily-puzzles/2026/03/30/margana-completed.json",
            "public/daily-puzzles/2026/03/30/margana-semi-completed.json",
        ],
    )

    try:
        ecs_main.main(["--target-week", "2026-14", "--output-root", str(tmp_path / "payloads")])
        assert False, "Expected SystemExit when target week already exists in S3"
    except SystemExit as exc:
        assert exc.code == 1
