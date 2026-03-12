import boto3

def main():
    table_name = "WeekScoreStats-preprod"
    user_pk = "USER#96e2f2f4-8041-7075-c533-3502c0509c72"

    client = boto3.client("dynamodb")

    scan_kwargs = {
        "TableName": table_name,
        "FilterExpression": "SK = :sk",
        "ExpressionAttributeValues": {
            ":sk": {"S": user_pk}
        },
        "ProjectionExpression": "SK, week_id, week_start, user_total_score, user_daily_scores",
        "ConsistentRead": False,
    }

    items = []
    while True:
        resp = client.scan(**scan_kwargs)
        items.extend(resp.get("Items", []))

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

        scan_kwargs["ExclusiveStartKey"] = last_key

    print(f"Found {len(items)} item(s)")
    for i, item in enumerate(items, 1):
        print(f"\nItem {i}:")
        print(item)

if __name__ == "__main__":
    main()
