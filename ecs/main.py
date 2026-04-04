import argparse
import json
import os
import shutil
import sys
from datetime import date
from pathlib import Path

from ecs.generator_wrapper import GeneratorWrapperConfig, run_generation_pipeline


DEFAULT_ASSETS_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "margana-assets")
DEFAULT_OUTPUT_ROOT = os.path.join(os.environ.get("TMPDIR", "/tmp"), "payloads")


def run_smoke_test() -> None:
    import boto3  # noqa: F401

    print("Starting Margana Puzzle Generator Task smoke test")
    print("Smoke test completed successfully.")


def get_s3_object(bucket_name: str, object_key: str, region_name: str | None, preview_bytes: int) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3", region_name=region_name)

    print(f"Fetching s3://{bucket_name}/{object_key}")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
    except (BotoCoreError, ClientError) as exc:
        print(f"S3 get failed for s3://{bucket_name}/{object_key}: {exc}", file=sys.stderr)
        raise

    body = response["Body"].read(preview_bytes + 1)
    preview = body[:preview_bytes]
    truncated = len(body) > preview_bytes

    print("Get succeeded.")
    print(f"ContentType={response.get('ContentType')}")
    print(f"ContentLength={response.get('ContentLength')}")
    print(f"ETag={response.get('ETag')}")
    print("Preview:")
    print(preview.decode("utf-8", errors="replace"))
    if truncated:
        print(f"... preview truncated at {preview_bytes} bytes")


def download_s3_object_to_file(
    bucket_name: str,
    object_key: str,
    destination_path: str,
    region_name: str | None,
) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    dest = Path(destination_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client("s3", region_name=region_name)

    print(f"Downloading s3://{bucket_name}/{object_key} -> {dest}")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        body = response["Body"].read()
    except (BotoCoreError, ClientError) as exc:
        print(f"S3 download failed for s3://{bucket_name}/{object_key}: {exc}", file=sys.stderr)
        raise

    dest.write_bytes(body)
    print(f"Downloaded {len(body)} bytes to {dest}")


def upload_file_to_s3(
    source_path: str,
    bucket_name: str,
    object_key: str,
    region_name: str | None,
    *,
    content_type: str,
) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    src = Path(source_path).resolve()
    if not src.exists():
        raise FileNotFoundError(f"Cannot upload missing file: {src}")

    s3 = boto3.client("s3", region_name=region_name)
    print(f"Uploading {src} -> s3://{bucket_name}/{object_key}")
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=src.read_bytes(),
            ContentType=content_type,
            CacheControl="no-cache",
        )
    except (BotoCoreError, ClientError) as exc:
        print(f"S3 upload failed for s3://{bucket_name}/{object_key}: {exc}", file=sys.stderr)
        raise
    print(f"Uploaded {src} to s3://{bucket_name}/{object_key}")


def download_static_assets(
    bucket_name: str,
    region_name: str | None,
    assets_dir: str,
    word_list_key: str,
    horizontal_exclude_key: str,
    letter_scores_key: str,
) -> dict[str, str]:
    base_dir = Path(assets_dir).resolve()
    paths = {
        "word_list": str(base_dir / "margana-word-list.txt"),
        "horizontal_exclude": str(base_dir / "horizontal-exclude-words.txt"),
        "letter_scores": str(base_dir / "letter-scores-v3.json"),
    }

    download_s3_object_to_file(bucket_name, word_list_key, paths["word_list"], region_name)
    download_s3_object_to_file(bucket_name, horizontal_exclude_key, paths["horizontal_exclude"], region_name)
    download_s3_object_to_file(bucket_name, letter_scores_key, paths["letter_scores"], region_name)

    return paths


def stage_assets_for_generator(downloaded_assets: dict[str, str], target_root: str) -> dict[str, str]:
    root = Path(target_root).resolve()
    root.mkdir(parents=True, exist_ok=True)

    staged = {
        "word_list": str(root / "margana-word-list.txt"),
        "horizontal_exclude": str(root / "horizontal-exclude-words.txt"),
        "letter_scores": str(root / "letter-scores-v3.json"),
    }
    shutil.copyfile(downloaded_assets["word_list"], staged["word_list"])
    shutil.copyfile(downloaded_assets["horizontal_exclude"], staged["horizontal_exclude"])
    shutil.copyfile(downloaded_assets["letter_scores"], staged["letter_scores"])
    return staged


def usage_log_bucket_for_environment(environment: str) -> str:
    return f"margana-word-game-{environment}"


def dates_for_iso_week(year: int, iso_week: int) -> list[date]:
    return [date.fromisocalendar(year, iso_week, day) for day in range(1, 8)]


def parse_target_week(target_week: str) -> tuple[int, int]:
    try:
        year_str, week_str = str(target_week).split("-", 1)
        return int(year_str), int(week_str)
    except Exception as exc:
        raise ValueError("Expected --target-week in YYYY-WW format") from exc


def print_payload_files(payload_root: Path) -> None:
    for payload_path in sorted(payload_root.rglob("*.json")):
        print(f"===== {payload_path} =====")
        print(json.dumps(json.loads(payload_path.read_text(encoding="utf-8")), indent=2))


def print_payload_summaries(payload_root: Path) -> None:
    for payload_path in sorted(payload_root.rglob("margana-completed.json")):
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        meta = payload.get("meta", {})
        print(
            "PAYLOAD"
            f" date={meta.get('date')}"
            f" score={payload.get('total_score')}"
            f" band={meta.get('difficultyBandApplied')}"
            f" madness={meta.get('madnessAvailable')}"
            f" path={payload_path}"
        )


def count_completed_payloads(payload_root: Path) -> int:
    return sum(1 for _ in payload_root.rglob("margana-completed.json"))


def expected_s3_payload_keys_for_week(*, year: int, iso_week: int, prefix: str) -> list[str]:
    keys: list[str] = []
    normalized_prefix = prefix.strip("/")
    for day in dates_for_iso_week(year, iso_week):
        day_prefix = f"{normalized_prefix}/{day.year:04d}/{day.month:02d}/{day.day:02d}"
        keys.append(f"{day_prefix}/margana-completed.json")
        keys.append(f"{day_prefix}/margana-semi-completed.json")
    return keys


def s3_object_exists(bucket_name: str, object_key: str, region_name: str | None) -> bool:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3", region_name=region_name)
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    except BotoCoreError:
        raise


def week_payloads_exist_in_s3(*, bucket_name: str, region_name: str | None, year: int, iso_week: int, prefix: str) -> bool:
    for object_key in expected_s3_payload_keys_for_week(year=year, iso_week=iso_week, prefix=prefix):
        if s3_object_exists(bucket_name, object_key, region_name):
            return True
    return False


def existing_week_payload_keys_in_s3(
    *,
    bucket_name: str,
    region_name: str | None,
    year: int,
    iso_week: int,
    prefix: str,
) -> list[str]:
    existing_keys: list[str] = []
    for object_key in expected_s3_payload_keys_for_week(year=year, iso_week=iso_week, prefix=prefix):
        if s3_object_exists(bucket_name, object_key, region_name):
            existing_keys.append(object_key)
    return existing_keys


def upload_payload_directory_to_s3(*, payload_root: Path, bucket_name: str, region_name: str | None, prefix: str) -> int:
    source_root = payload_root / prefix.strip("/")
    if not source_root.exists():
        raise FileNotFoundError(f"Payload source directory not found: {source_root}")

    uploaded = 0
    for payload_path in sorted(source_root.rglob("*.json")):
        relative_key = payload_path.relative_to(payload_root).as_posix()
        upload_file_to_s3(
            str(payload_path),
            bucket_name,
            relative_key,
            region_name,
            content_type="application/json",
        )
        uploaded += 1
    return uploaded


def send_ses_email(
    source_email: str,
    destination_email: str,
    subject: str,
    body_text: str,
    region_name: str | None,
) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    ses = boto3.client("ses", region_name=region_name)

    print(f"Sending SES email from {source_email} to {destination_email}")
    try:
        response = ses.send_email(
            Source=source_email,
            Destination={"ToAddresses": [destination_email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body_text}},
            },
        )
    except (BotoCoreError, ClientError) as exc:
        print(f"SES send failed from {source_email} to {destination_email}: {exc}", file=sys.stderr)
        raise

    print("SES send succeeded.")
    print(f"MessageId={response.get('MessageId')}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Margana Puzzle Generator Task")
    parser.add_argument("--target-week", type=str, help="Target week for puzzle generation (YYYY-WW)")
    parser.add_argument("--print-payloads", action="store_true", help="Print generated JSON payload files after a successful run")
    parser.add_argument(
        "--print-payload-summary",
        action="store_true",
        help="Print a one-line summary for each generated completed payload after a successful run",
    )
    parser.add_argument("--environment", type=str, default=os.environ.get("PUZZLE_ENVIRONMENT", "preprod"))
    parser.add_argument("--force", action="store_true", help="Force regeneration if already exists")
    parser.add_argument("--smoke-test", action="store_true", help="Run a basic startup check and exit")
    parser.add_argument("--get-s3", action="store_true", help="Fetch an object from the configured S3 bucket and exit")
    parser.add_argument("--download-static-assets", action="store_true", help="Download the required static puzzle assets locally and exit")
    parser.add_argument("--send-ses", action="store_true", help="Send a test email through SES and exit")
    parser.add_argument("--output-root", type=str, default=os.environ.get("OUTPUT_ROOT", DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--diag-direction", type=str, default=os.environ.get("DIAG_DIRECTION", "random"))
    parser.add_argument("--madness-word", type=str, default=os.environ.get("MADNESS_WORD", "both"))
    parser.add_argument("--max-path-tries", type=int, default=int(os.environ.get("MAX_PATH_TRIES", "400")))
    parser.add_argument("--max-target-tries", type=int, default=int(os.environ.get("MAX_TARGET_TRIES", "300")))
    parser.add_argument("--max-diag-tries", type=int, default=int(os.environ.get("MAX_DIAG_TRIES", "200")))
    parser.add_argument("--max-usage-tries", type=int, default=int(os.environ.get("MAX_USAGE_TRIES", "200")))
    parser.add_argument("--cooldown-days", type=int, default=int(os.environ.get("COOLDOWN_DAYS", "1826")))
    parser.add_argument("--use-s3-path-layout", action="store_true", default=True)
    parser.add_argument(
        "--payload-bucket",
        type=str,
        default=os.environ.get("PAYLOAD_BUCKET"),
        help="Override S3 bucket for puzzle payload upload/check. Defaults to margana-word-game-<environment>.",
    )
    parser.add_argument(
        "--payload-prefix",
        type=str,
        default=os.environ.get("PAYLOAD_PREFIX", "public/daily-puzzles"),
        help="S3 prefix used for generated puzzle payloads.",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=os.environ.get("S3_BUCKET"),
        help="S3 bucket to inspect. Defaults to the S3_BUCKET environment variable.",
    )
    parser.add_argument(
        "--s3-key",
        type=str,
        default=os.environ.get("S3_KEY"),
        help="S3 object key to fetch. Defaults to the S3_KEY environment variable.",
    )
    parser.add_argument(
        "--static-assets-bucket",
        type=str,
        default=os.environ.get("STATIC_ASSETS_BUCKET", "margana-static-assets-preprod"),
        help="S3 bucket containing static puzzle assets.",
    )
    parser.add_argument(
        "--word-list-key",
        type=str,
        default=os.environ.get("WORD_LIST_KEY", "margana-word-list.txt"),
        help="S3 key for the main word list.",
    )
    parser.add_argument(
        "--horizontal-exclude-key",
        type=str,
        default=os.environ.get("HORIZONTAL_EXCLUDE_KEY", "horizontal-exclude-words.txt"),
        help="S3 key for the horizontal exclude words file.",
    )
    parser.add_argument(
        "--letter-scores-key",
        type=str,
        default=os.environ.get("LETTER_SCORES_KEY", "letter-scores-v3.json"),
        help="S3 key for the letter scores file.",
    )
    parser.add_argument(
        "--assets-dir",
        type=str,
        default=os.environ.get("ASSETS_DIR", DEFAULT_ASSETS_DIR),
        help="Local directory where static assets should be downloaded inside the container.",
    )
    parser.add_argument(
        "--usage-log-key",
        type=str,
        default=os.environ.get("USAGE_LOG_KEY", "usage-logs/margana-puzzle-usage-log.json"),
        help="S3 key for the mutable usage log file.",
    )
    parser.add_argument(
        "--usage-log-bucket",
        type=str,
        default=os.environ.get("USAGE_LOG_BUCKET"),
        help="Override S3 bucket for the mutable usage log. Defaults to margana-word-game-<environment>.",
    )
    parser.add_argument(
        "--aws-region",
        type=str,
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        help="AWS region override. Defaults to AWS_REGION/AWS_DEFAULT_REGION.",
    )
    parser.add_argument(
        "--preview-bytes",
        type=int,
        default=512,
        help="Maximum number of object bytes to print when using --get-s3.",
    )
    parser.add_argument(
        "--ses-from",
        type=str,
        default=os.environ.get("SES_FROM"),
        help="SES verified source email address. Defaults to SES_FROM.",
    )
    parser.add_argument(
        "--ses-to",
        type=str,
        default=os.environ.get("SES_TO"),
        help="SES destination email address. Defaults to SES_TO.",
    )
    parser.add_argument(
        "--ses-subject",
        type=str,
        default="Margana ECS SES test",
        help="SES email subject for --send-ses.",
    )
    parser.add_argument(
        "--ses-body",
        type=str,
        default="This is a test email sent from the Margana ECS task role.",
        help="SES plain text email body for --send-ses.",
    )

    args = parser.parse_args(argv)

    if args.smoke_test:
        run_smoke_test()
        return

    if args.get_s3:
        if not args.s3_bucket:
            parser.error("--get-s3 requires --s3-bucket or S3_BUCKET to be set")
        if not args.s3_key:
            parser.error("--get-s3 requires --s3-key or S3_KEY to be set")
        get_s3_object(args.s3_bucket, args.s3_key, args.aws_region, args.preview_bytes)
        return

    if args.download_static_assets:
        downloaded = download_static_assets(
            bucket_name=args.static_assets_bucket,
            region_name=args.aws_region,
            assets_dir=args.assets_dir,
            word_list_key=args.word_list_key,
            horizontal_exclude_key=args.horizontal_exclude_key,
            letter_scores_key=args.letter_scores_key,
        )
        print("Static assets downloaded successfully.")
        print(downloaded)
        return

    if args.send_ses:
        if not args.ses_from:
            parser.error("--send-ses requires --ses-from or SES_FROM to be set")
        if not args.ses_to:
            parser.error("--send-ses requires --ses-to or SES_TO to be set")
        send_ses_email(args.ses_from, args.ses_to, args.ses_subject, args.ses_body, args.aws_region)
        return

    if not args.target_week:
        parser.error("--target-week is required unless using a utility mode like --smoke-test")

    year, iso_week = parse_target_week(args.target_week)
    payload_bucket = args.payload_bucket or usage_log_bucket_for_environment(str(args.environment))

    # Stage 1: fail fast if this target week is already present in S3, unless the caller
    # explicitly forces regeneration. This avoids overwriting or duplicating published weeks.
    existing_payload_keys: list[str] = []
    if not args.force:
        existing_payload_keys = existing_week_payload_keys_in_s3(
            bucket_name=payload_bucket,
            region_name=args.aws_region,
            year=year,
            iso_week=iso_week,
            prefix=args.payload_prefix,
        )
    if existing_payload_keys:
        existing_keys_text = "\n".join(f"  - s3://{payload_bucket}/{key}" for key in existing_payload_keys)
        print(
            f"Puzzle payloads already exist for ISO week {args.target_week}:\n{existing_keys_text}",
            file=sys.stderr,
        )
        raise SystemExit(1)

    # Stage 2: download the static generation inputs from S3 and stage them into the repo
    # root inside the container so the existing generator scripts can use local file paths.
    print(f"Starting Margana Puzzle Generator Task for week: {args.target_week}")
    downloaded = download_static_assets(
        bucket_name=args.static_assets_bucket,
        region_name=args.aws_region,
        assets_dir=args.assets_dir,
        word_list_key=args.word_list_key,
        horizontal_exclude_key=args.horizontal_exclude_key,
        letter_scores_key=args.letter_scores_key,
    )
    staged = stage_assets_for_generator(downloaded, str(Path(__file__).resolve().parents[1]))
    print("Static assets staged locally for the container.")
    print(staged)

    # Stage 3: fetch the mutable usage log locally. The generator updates this file during
    # generation, but we keep S3 writes outside the generator so publish stays gated on validation.
    usage_log_bucket = args.usage_log_bucket or usage_log_bucket_for_environment(str(args.environment))
    usage_log_path = str(Path(__file__).resolve().parents[1] / "margana-puzzle-usage-log.json")
    download_s3_object_to_file(
        usage_log_bucket,
        args.usage_log_key,
        usage_log_path,
        args.aws_region,
    )

    generator_args = [
        "--environment",
        str(args.environment),
        "--year",
        str(year),
        "--iso-week",
        str(iso_week),
        "--diag-direction",
        str(args.diag_direction),
        "--madness-word",
        str(args.madness_word),
        "--max-path-tries",
        str(args.max_path_tries),
        "--max-target-tries",
        str(args.max_target_tries),
        "--max-diag-tries",
        str(args.max_diag_tries),
        "--max-usage-tries",
        str(args.max_usage_tries),
        "--cooldown-days",
        str(args.cooldown_days),
        "--words-file",
        staged["word_list"],
        "--no-s3-usage",
    ]
    if args.use_s3_path_layout:
        generator_args.append("--use-s3-path-layout")

    # Stage 4: run the generator first, then the standalone validator over the generated
    # payload directory. The wrapper keeps these as separate processes so validation can be
    # rerun independently when needed.
    config = GeneratorWrapperConfig(
        payload_dir=Path(args.output_root),
        generator_args=generator_args,
        validator_args=["--summary-only", "--horizontal-exclude-file", staged["horizontal_exclude"]],
    )
    generator_result, validator_result = run_generation_pipeline(config, cwd=Path(__file__).resolve().parents[1])
    if generator_result.returncode != 0:
        raise SystemExit(generator_result.returncode)
    if validator_result is None:
        raise SystemExit(1)
    if validator_result.returncode != 0:
        raise SystemExit(validator_result.returncode)

    # Stage 5: require a complete ISO week before publishing anything. Validation only checks
    # payloads that exist, so this guards against partial weeks slipping through as success.
    completed_payloads = count_completed_payloads(Path(args.output_root))
    if completed_payloads != 7:
        print(
            f"Expected 7 completed payloads for ISO week {args.target_week}, found {completed_payloads}.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    # Stage 6: publish the updated usage log and validated payload files back to S3.
    # This only happens after generation and validation have both succeeded.
    upload_file_to_s3(
        usage_log_path,
        usage_log_bucket,
        args.usage_log_key,
        args.aws_region,
        content_type="application/json",
    )

    uploaded_payloads = upload_payload_directory_to_s3(
        payload_root=Path(args.output_root),
        bucket_name=payload_bucket,
        region_name=args.aws_region,
        prefix=args.payload_prefix,
    )
    print(f"Uploaded {uploaded_payloads} payload files to s3://{payload_bucket}/{args.payload_prefix}")

    if args.print_payload_summary:
        print_payload_summaries(Path(args.output_root))

    if args.print_payloads:
        print_payload_files(Path(args.output_root))

    print("Task completed successfully.")


if __name__ == "__main__":
    main()
