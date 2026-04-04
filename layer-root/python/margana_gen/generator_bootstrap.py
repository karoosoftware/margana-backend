from __future__ import annotations

from pathlib import Path
from typing import Callable

from margana_gen.s3_utils import download_usage_log_from_s3, download_word_list_from_s3, upload_usage_log_to_s3
from margana_gen.usage_log import load_usage_log, save_usage_log


def load_word_exclude_words(path: Path, *, min_len: int, max_len: int) -> set[str]:
    words: set[str] = set()
    if not path.exists():
        return words
    for line in path.read_text(encoding="utf-8").splitlines():
        word = line.strip().lower()
        if not word or word.startswith("#"):
            continue
        if word.isalpha() and min_len <= len(word) <= max_len:
            words.add(word)
    return words


def load_horizontal_exclude_words(path: Path) -> set[str]:
    return load_word_exclude_words(path, min_len=5, max_len=5)


def load_anagram_exclude_words(path: Path) -> set[str]:
    return load_word_exclude_words(path, min_len=8, max_len=10)


def ensure_words_file(
    *,
    words_path: Path,
    bucket: str,
    key: str,
    allow_s3_download: bool,
    logger: Callable[[str], None] | None = None,
) -> Path:
    resolved = words_path.resolve()
    if resolved.exists():
        return resolved
    if not allow_s3_download:
        raise FileNotFoundError(f"Word list not found at {resolved}")

    if logger:
        logger(f"words file missing locally at {resolved}, downloading from s3://{bucket}/{key}")
    etag_path = resolved.with_suffix(".etag")
    ok = download_word_list_from_s3(
        bucket=bucket,
        key=key,
        dest_path=str(resolved),
        etag_cache_path=str(etag_path),
        use_cache=True,
    )
    if logger:
        logger(f"word list download ok={ok} exists_now={resolved.exists()}")
    if not ok or not resolved.exists():
        raise FileNotFoundError(
            f"Word list not found and S3 download failed. Tried local '{resolved}' and s3://{bucket}/{key}"
        )
    return resolved


def load_usage_log_with_optional_s3_sync(
    *,
    bucket: str,
    key: str,
    usage_log_path: Path,
    sync_from_s3: bool,
    logger: Callable[[str], None] | None = None,
) -> dict:
    if sync_from_s3:
        if logger:
            logger(f"downloading usage log from s3://{bucket}/{key}")
        download_usage_log_from_s3(bucket=bucket, key=key, dest_path=usage_log_path)
    return load_usage_log(usage_log_path)


def save_usage_log_with_optional_s3_sync(
    *,
    usage_log: dict,
    usage_log_path: Path,
    bucket: str,
    key: str,
    sync_to_s3: bool,
) -> None:
    save_usage_log(usage_log, usage_log_path)
    if sync_to_s3:
        upload_usage_log_to_s3(bucket=bucket, key=key, src_path=usage_log_path)
