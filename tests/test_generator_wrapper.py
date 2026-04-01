from __future__ import annotations

import subprocess
from pathlib import Path

from ecs.generator_wrapper import (
    GeneratorWrapperConfig,
    build_generator_command,
    build_validator_command,
    run_generation_pipeline,
)


def test_build_generator_command_uses_classic_script_by_default(tmp_path):
    config = GeneratorWrapperConfig(
        payload_dir=tmp_path,
        generator_args=["--seed", "42"],
        python_executable="python3",
    )

    command = build_generator_command(config)

    assert command[0] == "python3"
    assert command[1].endswith("ecs/generate-column-puzzle.py")
    assert command[2:] == ["--output-root", str(tmp_path), "--seed", "42"]


def test_build_generator_command_can_switch_to_madness_script(tmp_path):
    config = GeneratorWrapperConfig(
        payload_dir=tmp_path,
        use_madness_generator=True,
        python_executable="python3",
    )

    command = build_generator_command(config)

    assert command[1].endswith("ecs/generate-column-puzzle-madness.py")


def test_build_validator_command_targets_payload_dir(tmp_path):
    config = GeneratorWrapperConfig(
        payload_dir=tmp_path,
        validator_args=["--summary-only"],
        python_executable="python3",
    )

    command = build_validator_command(config)

    assert command == [
        "python3",
        command[1],
        "--payload-dir",
        str(tmp_path),
        "--summary-only",
    ]
    assert command[1].endswith("ecs/validate-puzzle.py")


def test_run_generation_pipeline_stops_when_generator_fails(tmp_path, monkeypatch):
    calls: list[list[str]] = []

    def fake_run(command: list[str], *, cwd: Path | None = None):
        calls.append(command)
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr("ecs.generator_wrapper.run_command", fake_run)

    config = GeneratorWrapperConfig(payload_dir=tmp_path)
    gen_result, val_result = run_generation_pipeline(config)

    assert gen_result.returncode == 1
    assert val_result is None
    assert len(calls) == 1


def test_run_generation_pipeline_runs_validator_after_generator_success(tmp_path, monkeypatch):
    calls: list[list[str]] = []

    def fake_run(command: list[str], *, cwd: Path | None = None):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr("ecs.generator_wrapper.run_command", fake_run)

    config = GeneratorWrapperConfig(payload_dir=tmp_path)
    gen_result, val_result = run_generation_pipeline(config)

    assert gen_result.returncode == 0
    assert val_result is not None
    assert val_result.returncode == 0
    assert len(calls) == 2
    assert calls[0][1].endswith("ecs/generate-column-puzzle.py")
    assert calls[1][1].endswith("ecs/validate-puzzle.py")
