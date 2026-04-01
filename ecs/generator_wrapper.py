from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


ECS_DIR = Path(__file__).resolve().parent
DEFAULT_CLASSIC_GENERATOR = ECS_DIR / "generate-column-puzzle.py"
DEFAULT_MADNESS_GENERATOR = ECS_DIR / "generate-column-puzzle-madness.py"
DEFAULT_VALIDATOR = ECS_DIR / "validate-puzzle.py"


@dataclass(frozen=True)
class GeneratorWrapperConfig:
    payload_dir: Path
    generator_args: list[str] = field(default_factory=list)
    validator_args: list[str] = field(default_factory=list)
    use_madness_generator: bool = False
    python_executable: str = sys.executable
    classic_generator_script: Path = DEFAULT_CLASSIC_GENERATOR
    madness_generator_script: Path = DEFAULT_MADNESS_GENERATOR
    validator_script: Path = DEFAULT_VALIDATOR

    @property
    def generator_script(self) -> Path:
        return self.madness_generator_script if self.use_madness_generator else self.classic_generator_script


def build_generator_command(config: GeneratorWrapperConfig) -> list[str]:
    return [
        config.python_executable,
        str(config.generator_script),
        "--output-root",
        str(config.payload_dir),
        *config.generator_args,
    ]


def build_validator_command(config: GeneratorWrapperConfig) -> list[str]:
    return [
        config.python_executable,
        str(config.validator_script),
        "--payload-dir",
        str(config.payload_dir),
        *config.validator_args,
    ]


def run_command(command: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=str(cwd) if cwd else None, check=False, text=True)


def run_generation_pipeline(
    config: GeneratorWrapperConfig,
    *,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], subprocess.CompletedProcess[str] | None]:
    generator_result = run_command(build_generator_command(config), cwd=cwd)
    if generator_result.returncode != 0:
        return generator_result, None

    validator_result = run_command(build_validator_command(config), cwd=cwd)
    return generator_result, validator_result
