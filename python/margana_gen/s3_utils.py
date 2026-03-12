from pathlib import Path
from datetime import datetime

# Optional boto3 import isolated here
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    HAS_BOTO3 = True
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore
    BotoCoreError = ClientError = Exception  # type: ignore
    HAS_BOTO3 = False


def ensure_parent_dir(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def download_word_list_from_s3(
    bucket: str,
    key: str,
    dest_path,
    etag_cache_path,
    use_cache: bool = True,
) -> bool:
    """
    Fetch the word list from S3 if needed.
    Returns True if we have a local file to use (downloaded or already present).
    """
    ensure_parent_dir(dest_path)
    if not HAS_BOTO3:
        return Path(dest_path).exists()
    s3 = boto3.client("s3")

    remote_etag = None
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        remote_etag = head.get("ETag", "").strip('"')
    except (BotoCoreError, ClientError):
        remote_etag = None

    local_etag = None
    if use_cache and Path(etag_cache_path).exists():
        try:
            local_etag = Path(etag_cache_path).read_text(encoding="utf-8").strip()
        except Exception:
            local_etag = None

    if remote_etag and local_etag and remote_etag == local_etag and Path(dest_path).exists():
        return True

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        with open(dest_path, "wb") as f:
            f.write(data)
        if remote_etag:
            Path(etag_cache_path).write_text(remote_etag, encoding="utf-8")
        return True
    except (BotoCoreError, ClientError, OSError):
        # Fallback: if local exists, use it; otherwise signal failure
        return Path(dest_path).exists()


def download_usage_log_from_s3(
    bucket: str,
    key: str,
    dest_path,
) -> bool:
    """
    Try to download the usage log JSON from S3.
    Returns True if we have a local file to use (downloaded or already present).
    If the object does not exist, we leave any local file as-is (or create an empty one on first load()).
    """
    ensure_parent_dir(dest_path)
    if not HAS_BOTO3:
        return Path(dest_path).exists()
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        with open(dest_path, "wb") as f:
            f.write(data)
        return True
    except ClientError as e:  # type: ignore[name-defined]
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return Path(dest_path).exists()
        return Path(dest_path).exists()
    except (BotoCoreError, OSError):  # type: ignore[name-defined]
        return Path(dest_path).exists()


def upload_usage_log_to_s3(
    bucket: str,
    key: str,
    src_path,
) -> bool:
    """
    Upload the local usage log JSON back to S3.
    Returns True on success, False on failure.
    """
    if not HAS_BOTO3:
        return False
    s3 = boto3.client("s3")
    if not Path(src_path).exists():
        return False
    try:
        body = Path(src_path).read_bytes()
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            CacheControl="no-cache",
        )
        return True
    except (BotoCoreError, ClientError, OSError):  # type: ignore[name-defined]
        return False


def upload_puzzle_output_to_s3(
    bucket: str,
    prefix_root: str,
    puzzle_date_ddmmyyyy: str,
    src_path: str,
) -> str:
    """
    Uploads src_path to s3://bucket/{prefix_root}/YYYY/MM/DD/margana-puzzle-values.json
    Returns the s3 key used on success. Raises on failure.
    """
    if not Path(src_path).exists():
        raise FileNotFoundError(f"Puzzle output not found at {src_path}")

    # Parse dd/mm/yyyy
    from datetime import datetime as _dt
    try:
        d = _dt.strptime(puzzle_date_ddmmyyyy, "%d/%m/%Y").date()
    except ValueError:
        raise ValueError("Invalid --puzzle-date. Expected format DD/MM/YYYY.")

    if not HAS_BOTO3:
        raise RuntimeError("boto3 is required to upload puzzle output; install boto3 or omit --upload-puzzle.")

    key = f"{prefix_root}/{d.year:04d}/{d.month:02d}/{d.day:02d}/margana-puzzle-values.json"

    s3 = boto3.client("s3")
    body = Path(src_path).read_bytes()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="no-cache",
    )
    return key




def upload_margana_completed_to_s3(
    bucket: str,
    prefix_root: str,
    puzzle_date_ddmmyyyy: str,
    src_path: str,
) -> str:
    """
    Uploads src_path to s3://bucket/{prefix_root}/YYYY/MM/DD/margana-completed.json
    Returns the s3 key used on success. Raises on failure.
    """
    if not Path(src_path).exists():
        raise FileNotFoundError(f"Completed payload not found at {src_path}")

    from datetime import datetime as _dt
    try:
        d = _dt.strptime(puzzle_date_ddmmyyyy, "%d/%m/%Y").date()
    except ValueError:
        raise ValueError("Invalid --puzzle-date. Expected format DD/MM/YYYY.")

    if not HAS_BOTO3:
        raise RuntimeError("boto3 is required to upload payloads; install boto3 or omit --upload-puzzle.")

    key = f"{prefix_root}/{d.year:04d}/{d.month:02d}/{d.day:02d}/margana-completed.json"

    s3 = boto3.client("s3")
    body = Path(src_path).read_bytes()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="no-cache",
    )
    return key


def upload_margan_semi_completed_to_s3(
    bucket: str,
    prefix_root: str,
    puzzle_date_ddmmyyyy: str,
    src_path: str,
) -> str:
    """
    Uploads src_path to s3://bucket/{prefix_root}/YYYY/MM/DD/margana-semi-completed.json
    Returns the s3 key used on success. Raises on failure.
    """
    if not Path(src_path).exists():
        raise FileNotFoundError(f"Semi-completed payload not found at {src_path}")

    from datetime import datetime as _dt
    try:
        d = _dt.strptime(puzzle_date_ddmmyyyy, "%d/%m/%Y").date()
    except ValueError:
        raise ValueError("Invalid --puzzle-date. Expected format DD/MM/YYYY.")

    if not HAS_BOTO3:
        raise RuntimeError("boto3 is required to upload payloads; install boto3 or omit --upload-puzzle.")

    key = f"{prefix_root}/{d.year:04d}/{d.month:02d}/{d.day:02d}/margana-semi-completed.json"

    s3 = boto3.client("s3")
    body = Path(src_path).read_bytes()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="no-cache",
    )
    return key
