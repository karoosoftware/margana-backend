import boto3, time, random
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# ---------------- CONFIGURE THESE ----------------
TABLE_NAME = "MarganaUserResults-prod"
PK_NAME = "PK"            # change if your PK attribute is named differently
SK_NAME = "SK"            # set to None if you do NOT use a sort key
USER_PREFIX = "USER#"     # as per your example: USER#<sub>

# Paul
OLD_SUB = "b6524234-5001-70c2-fd18-e6832a40c33d"
NEW_SUB = "468212e4-0031-7072-3263-4e621d78c12d"

# If any attributes (including GSI keys) also contain the sub and must be rewritten,
# list them here; they’ll get a simple string .replace(OLD_SUB, NEW_SUB).
ATTRS_TO_REWRITE = [
    "sub",          # common to store raw sub as an attribute
    # "gsi1pk", "gsi1sk", "gsi2pk", "gsi2sk",  # add any that include the sub
]

# -------------- END USER CONFIG ------------------

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb")
table = dynamodb.Table(TABLE_NAME)

old_pk = f"{USER_PREFIX}{OLD_SUB}"
new_pk = f"{USER_PREFIX}{NEW_SUB}"

# 1) Query all items in the old user partition
query_kwargs = {
    "KeyConditionExpression": Key(PK_NAME).eq(old_pk)
}
items = []
resp = table.query(**query_kwargs)
items.extend(resp.get("Items", []))

while "LastEvaluatedKey" in resp:
    resp = table.query(ExclusiveStartKey=resp["LastEvaluatedKey"], **query_kwargs)
    items.extend(resp.get("Items", []))

print(f"Found {len(items)} items for partition {old_pk}")

def rewrite_item(item):
    """Return a new item with PK (and optional SK) rewritten and any attribute replacements applied."""
    new_item = dict(item)

    # Rewrite PK
    new_item[PK_NAME] = new_pk

    # Optionally rewrite SK if your SK also embeds the sub (common pattern).
    if SK_NAME and SK_NAME in new_item and isinstance(new_item[SK_NAME], str):
        new_item[SK_NAME] = new_item[SK_NAME].replace(OLD_SUB, NEW_SUB)

    # Rewrite other attributes that may embed the sub (including GSI key attrs)
    for attr in ATTRS_TO_REWRITE:
        if attr in new_item and isinstance(new_item[attr], str):
            new_item[attr] = new_item[attr].replace(OLD_SUB, NEW_SUB)

    return new_item

def key_dict(pk_value, sk_value=None):
    key = {PK_NAME: {"S": pk_value}} if isinstance(pk_value, str) else {PK_NAME: pk_value}
    if SK_NAME and sk_value is not None:
        key[SK_NAME] = {"S": sk_value} if isinstance(sk_value, str) else sk_value
    return key

# 2) For each item, transact: Put new (if not exists) + Delete old (if exists)
migrated = 0
for it in items:
    new_it = rewrite_item(it)

    # Build keys
    old_key = key_dict(it[PK_NAME], it.get(SK_NAME))
    new_key = key_dict(new_it[PK_NAME], new_it.get(SK_NAME))

    # DynamoDB expects AttributeValue maps on the low-level client
    def to_av_map(d):
        import json
        from botocore.compat import six
        # Use Table resource marshaller
        return boto3.dynamodb.types.TypeSerializer().serialize(d)["M"]

    put_item = {
        "TableName": TABLE_NAME,
        "Item": to_av_map(new_it),
        # Only create if it doesn't exist yet
        "ConditionExpression": f"attribute_not_exists({PK_NAME})" + (f" AND attribute_not_exists({SK_NAME})" if SK_NAME else "")
    }
    del_item = {
        "TableName": TABLE_NAME,
        "Key": old_key,
        # Only delete if it still exists (avoid deleting something else accidentally)
        "ConditionExpression": f"attribute_exists({PK_NAME})"
    }

    # Retry with jitter on TransactionCanceled (throughput/contention)
    for attempt in range(7):
        try:
            client.transact_write_items(
                TransactItems=[
                    {"Put": put_item},
                    {"Delete": del_item},
                ]
            )
            migrated += 1
            break
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("TransactionCanceledException", "ProvisionedThroughputExceededException"):
                time.sleep((2 ** attempt) * 0.1 + random.random() * 0.2)
                continue
            else:
                raise

print(f"Migrated {migrated}/{len(items)} items from {old_pk} → {new_pk}")
