import argparse
import os
import sys


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Margana Puzzle Generator Task")
    parser.add_argument("--target-week", type=str, help="Target week for puzzle generation (YYYY-WW)")
    parser.add_argument("--force", action="store_true", help="Force regeneration if already exists")
    parser.add_argument("--smoke-test", action="store_true", help="Run a basic startup check and exit")
    parser.add_argument("--get-s3", action="store_true", help="Fetch an object from the configured S3 bucket and exit")
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

    args = parser.parse_args()

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

    print(f"Starting Margana Puzzle Generator Task for week: {args.target_week}")
    # TODO: Implement puzzle generation logic
    print("Task completed successfully.")


if __name__ == "__main__":
    main()
