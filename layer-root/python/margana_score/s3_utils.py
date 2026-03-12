from pathlib import Path
from botocore.exceptions import BotoCoreError, ClientError

import logging, os, json
import boto3

# Logging
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(level=_level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(_level)


def ensure_parent_dir(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def download_word_list_from_s3(
    bucket: str,
    key: str,
    output_path,
    etag_cache_path: str | None = None,
    use_cache: bool = True,
) -> bool:
    dest_path = output_path
    ensure_parent_dir(dest_path)
    s3 = boto3.client("s3")

    remote_etag = None
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        remote_etag = head.get("ETag", "").strip('"')
    except (BotoCoreError, ClientError):
        remote_etag = None

    local_etag = None
    if use_cache and etag_cache_path and Path(etag_cache_path).exists():
        try:
            local_etag = Path(etag_cache_path).read_text(encoding="utf-8").strip()
        except Exception:
            local_etag = None

    # 1) Local cached file used (no download)
    if remote_etag and local_etag and remote_etag == local_etag and Path(dest_path).exists():
        logger.info(
            "Using cached word list at %s (remote ETag %s == local ETag %s), "
            "skipping S3 download",
            dest_path,
            remote_etag,
            local_etag,
        )
        return True

    try:
        # 2) Download from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        with open(dest_path, "wb") as f:
            f.write(data)

        if remote_etag and etag_cache_path:
            Path(etag_cache_path).write_text(remote_etag, encoding="utf-8")

        logger.info(
            "Downloaded word list from s3://%s/%s to %s (etag=%s)",
            bucket,
            key,
            dest_path,
            remote_etag,
        )
        return True

    except (BotoCoreError, ClientError, OSError) as e:
        # 3) Fallback to existing /tmp if S3 failed
        if Path(dest_path).exists():
            logger.warning(
                "Failed to download word list from s3://%s/%s (%r); "
                "using existing local file at %s",
                bucket,
                key,
                e,
                dest_path,
                exc_info=True,
            )
            return True

        logger.error(
            "Failed to download word list from s3://%s/%s and no local file at %s",
            bucket,
            key,
            dest_path,
            exc_info=True,
        )
        return False


def write_json_to_s3(
    bucket: str,
    key: str,
    data,
    cache_control: str | None = "no-cache",
) -> bool:
    """Serialize `data` as JSON and write to s3://bucket/key.

    Returns True on success, False on failure. Logs details.
    """
    try:
        s3 = boto3.client("s3")
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        extra = {"ContentType": "application/json"}
        if cache_control:
            extra["CacheControl"] = cache_control
        s3.put_object(Bucket=bucket, Key=key, Body=body, **extra)
        logger.info("Wrote JSON to s3://%s/%s (%d bytes)", bucket, key, len(body))
        return True
    except (BotoCoreError, ClientError, OSError) as e:
        logger.exception("Failed to write JSON to s3://%s/%s: %r", bucket, key, e)
        return False


def build_daily_results_key(date_str: str, user_sub: str | None) -> str:
    """Build the canonical per-user results key path used for commit writes.

    Desired format:
      public/users/<sub>/<YYYY>/<MM>/<DD>/margana-user-results.json
    """
    # Normalize user sub
    sub = (user_sub or "anonymous").strip() or "anonymous"
    # Normalize/parse date
    try:
        yyyy, mm, dd = date_str.split("-")
    except Exception:
        # Fallback to flat date folder if unexpected format
        return f"public/users/{sub}/{date_str}/margana-user-results.json"
    return f"public/users/{sub}/{yyyy}/{mm}/{dd}/margana-user-results.json"
