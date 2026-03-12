import json
import boto3
import os

# Create S3 client
s3 = boto3.client('s3')


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        # Fail fast with a clear message in CloudWatch logs and Lambda init error
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# Environment variables
ENVIRONMENT = require_env("ENVIRONMENT")
BUCKET_NAME = f"margana-word-game-{ENVIRONMENT}"
WORD_LIST_KEY = "word-lists/margana-word-list.txt"

# Cache the word list in memory to avoid multiple S3 fetches
word_set = None



def load_word_list():
    global word_set
    if word_set is None:
        resp = s3.get_object(Bucket=BUCKET_NAME, Key=WORD_LIST_KEY)
        content = resp['Body'].read().decode('utf-8')
        # Assume one word per line
        word_set = set(word.strip().upper() for word in content.splitlines())
    return word_set

def lambda_handler(event, context):
    # Ensure JSON input
    try:
        body = json.loads(event.get("body", "{}"))
        word = body.get("word", "").upper()
    except Exception:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid input"})
        }

    if not word:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Word is required"})
        }

    # Load words
    valid_words = load_word_list()
    is_valid = word in valid_words

    return {
        "statusCode": 200,
        "body": json.dumps({"valid": is_valid})
    }
