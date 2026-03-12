import json
import logging
import os
import boto3
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Set, Tuple
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb_client = boto3.client("dynamodb")
s3_client = boto3.client("s3")

class CleanupAudit:
    def __init__(self, user_sub: str):
        self.user_sub = user_sub
        self.start_time = datetime.now(timezone.utc).isoformat()
        self.events = []
        self.milestones = []
        self.stats = {}
        self.verbose = os.environ.get("AUDIT_VERBOSE", "").lower() in ("1", "true", "yes")

    def add_event(self, message: str, **kwargs):
        """Adds an event to the audit trail."""
        if not self.verbose:
            return
        event = {"msg": message, "ts": datetime.now(timezone.utc).isoformat()}
        if kwargs:
            event.update(kwargs)
        self.events.append(event)

    def add_milestone(self, message: str, **kwargs):
        """Adds a compact milestone entry for troubleshooting."""
        entry = {"msg": message, "ts": datetime.now(timezone.utc).isoformat()}
        if kwargs:
            entry.update(kwargs)
        self.milestones.append(entry)

    def add_deletion(self, table: str, count: int = 1):
        """Increments the deletion count for a specific table or resource."""
        self.stats[table] = self.stats.get(table, 0) + count

    def get_summary(self) -> dict:
        """Returns the complete audit summary."""
        summary = {
            "user_sub": self.user_sub,
            "start_time": self.start_time,
            "end_time": datetime.now(timezone.utc).isoformat(),
            "total_deleted": sum(self.stats.values()),
            "stats": self.stats,
            "milestones": self.milestones
        }
        if self.verbose:
            summary["events"] = self.events
        return summary

def _extract_user_sub(event_detail: dict) -> Optional[str]:
    """
    Extracts the user's sub (UUID) from the Cognito DeleteUser CloudTrail event.

    Since we only listen for 'DeleteUser' (self-deletion), we focus on the 'sub'
    identifier. This avoids accidental matches on 'username' fields that might
    appear in other event types.
    """
    # 1. Check additionalEventData (Primary location for self-service DeleteUser)
    additional_data = event_detail.get("additionalEventData") or {}
    if "sub" in additional_data:
        return additional_data["sub"]

    # 2. Fallback to requestParameters (Check only sub-specific keys)
    request_params = event_detail.get("requestParameters") or {}
    return request_params.get("sub") or request_params.get("userSub")

def get_table_name(base_name: str) -> str:
    """Gets the environment-specific table name."""
    env = os.environ.get("ENVIRONMENT", "preprod")
    return f"{base_name}-{env}"

def delete_dynamodb_items(table_name: str, pk_value: str, audit: CleanupAudit, pk: str = "PK") -> int:
    """Deletes all items matching the Partition Key."""
    try:
        # Query and delete all items for the PK
        audit.add_event(f"Querying items to delete from {table_name} for {pk}={pk_value}", table=table_name, pk=pk, pk_value=pk_value)
        paginator = dynamodb_client.get_paginator("query")
        items_deleted = 0
        for page in paginator.paginate(
            TableName=table_name,
            KeyConditionExpression=f"{pk} = :pkval",
            ExpressionAttributeValues={":pkval": {"S": pk_value}}
        ):
            for item in page.get("Items", []):
                key = {pk: item[pk]}
                if "SK" in item:
                    key["SK"] = item["SK"]
                
                dynamodb_client.delete_item(TableName=table_name, Key=key)
                items_deleted += 1
        
        if items_deleted > 0:
            audit.add_event(f"Deleted {items_deleted} items from {table_name}", table=table_name, count=items_deleted)
        return items_deleted
    except ClientError as e:
        logger.error(f"Error deleting from {table_name}: {e}")
        raise


def handle_leaderboards(user_sub: str, audit: CleanupAudit):
    """Handles the complex logic for Leaderboards cleanup."""
    table_name = get_table_name("Leaderboards")
    pk_value = f"USER#{user_sub}"
    
    try:
        # 1. Find all memberships for this user
        audit.add_event(f"Querying Leaderboards memberships for user {user_sub}", table=table_name, user_sub=user_sub)
        paginator = dynamodb_client.get_paginator("query")
        
        for page in paginator.paginate(
            TableName=table_name,
            KeyConditionExpression="PK = :pkval",
            ExpressionAttributeValues={":pkval": {"S": pk_value}}
        ):
            for membership in page.get("Items", []):
                sk = membership.get("SK", {}).get("S", "")
                if not sk.startswith("MEMBERSHIP#LEADERBOARD#"):
                    if sk.startswith("PENDING#LEADERBOARD#"):
                        # If it's a join request, also delete the REQUEST item in the leaderboard partition
                        status = (membership.get("status") or {}).get("S")
                        if status == "pending":
                            leaderboard_id = sk.replace("PENDING#LEADERBOARD#", "")
                            audit.add_event(f"Deleting orphaned join request from Leaderboards: {leaderboard_id}", table=table_name)
                            dynamodb_client.delete_item(
                                TableName=table_name,
                                Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": f"REQUEST#USER#{user_sub}"}}
                            )
                            audit.add_deletion("Leaderboards")

                        audit.add_event(f"Deleting pending item from Leaderboards: {sk}", table=table_name, PK=pk_value)
                        dynamodb_client.delete_item(TableName=table_name, Key={"PK": {"S": pk_value}, "SK": {"S": sk}})
                        audit.add_deletion("Leaderboards")
                    continue
                    
                leaderboard_id = sk.replace("MEMBERSHIP#LEADERBOARD#", "")
                role = membership.get("role", {}).get("S", "member")
                
                # Check if user is sole admin
                leaderboard_pk = f"LEADERBOARD#{leaderboard_id}"
                
                # Query GSI3 to find all admins of this leaderboard
                admins_resp = dynamodb_client.query(
                    TableName=table_name,
                    IndexName="GSI3",
                    KeyConditionExpression="gsi3_pk = :lpk AND begins_with(gsi3_sk, :roleprefix)",
                    ExpressionAttributeValues={
                        ":lpk": {"S": leaderboard_pk},
                        ":roleprefix": {"S": "ROLE#ADMIN#"}
                    }
                )
                admins = admins_resp.get("Items", [])
                
                if not admins:
                    # Check if metadata exists even if no admins (unlikely but possible)
                    meta_check = dynamodb_client.get_item(TableName=table_name, Key={"PK": {"S": leaderboard_pk}, "SK": {"S": "METADATA"}})
                    if "Item" not in meta_check:
                        # Metadata not found, just delete the membership
                        dynamodb_client.delete_item(TableName=table_name, Key={"PK": {"S": pk_value}, "SK": {"S": sk}})
                        audit.add_deletion("Leaderboards")
                        continue

                admin_count = len(admins)
                
                if role == "admin" and admin_count <= 1:
                    # Sole admin: delete entire leaderboard
                    audit.add_milestone(
                        "User is sole admin; deleting entire leaderboard.",
                        leaderboard_id=leaderboard_id
                    )

                    # 0. Fetch Metadata BEFORE deletion to get normalized_name
                    meta_resp = dynamodb_client.get_item(TableName=table_name, Key={"PK": {"S": leaderboard_pk}, "SK": {"S": "METADATA"}})
                    metadata = meta_resp.get("Item", {})
                    norm_name = metadata.get("normalized_name", {}).get("S")
                    
                    # 1. Delete all items belonging to this leaderboard via GSI3
                    members_paginator = dynamodb_client.get_paginator("query")
                    for members_page in members_paginator.paginate(
                        TableName=table_name,
                        IndexName="GSI3",
                        KeyConditionExpression="gsi3_pk = :lpk",
                        ExpressionAttributeValues={":lpk": {"S": leaderboard_pk}}
                    ):
                        for member_item in members_page.get("Items", []):
                            m_pk = member_item.get("PK", {}).get("S", "")
                            m_sk = member_item.get("SK", {}).get("S", "")
                            if m_pk and m_sk:
                                dynamodb_client.delete_item(TableName=table_name, Key={"PK": {"S": m_pk}, "SK": {"S": m_sk}})
                                audit.add_deletion("Leaderboards")
                    
                    # 2. Delete leaderboard metadata
                    audit.add_event(f"Deleting leaderboard metadata", PK=leaderboard_pk)
                    dynamodb_client.delete_item(TableName=table_name, Key={"PK": {"S": leaderboard_pk}, "SK": {"S": "METADATA"}})
                    audit.add_deletion("Leaderboards")

                    # 3. Delete invites from LeaderboardInvites table via GSI3
                    invites_table = get_table_name("LeaderboardInvites")
                    audit.add_event(f"Querying and deleting invites for leaderboard {leaderboard_id} from {invites_table}", leaderboard_id=leaderboard_id)
                    invites_paginator = dynamodb_client.get_paginator("query")
                    for invites_page in invites_paginator.paginate(
                        TableName=invites_table,
                        IndexName="GSI3",
                        KeyConditionExpression="leaderboard_id = :lid",
                        ExpressionAttributeValues={":lid": {"S": leaderboard_id}}
                    ):
                        for invite_item in invites_page.get("Items", []):
                            if "PK" in invite_item and "SK" in invite_item:
                                dynamodb_client.delete_item(TableName=invites_table, Key={"PK": invite_item["PK"], "SK": invite_item["SK"]})
                                audit.add_deletion("LeaderboardInvites")

                    # 4. Delete name reservation
                    if norm_name:
                        audit.add_event(f"Deleting leaderboard name reservation: {norm_name}")
                        dynamodb_client.delete_item(TableName=table_name, Key={"PK": {"S": f"LEADERBOARD_NAME#{norm_name}"}, "SK": {"S": "RESERVATION"}})
                        audit.add_deletion("Leaderboards")
                else:
                    # Regular member or one of multiple admins: decrement counts and delete membership
                    audit.add_milestone(
                        "Removing user from leaderboard and decrementing counts.",
                        leaderboard_id=leaderboard_id,
                        role=role
                    )
                    
                    update_expr = "SET member_count = member_count - :one"
                    expr_attr_vals = {":one": {"N": "1"}}
                    if role == "admin":
                        update_expr += ", admin_count = admin_count - :one"
                    
                    try:
                        dynamodb_client.update_item(
                            TableName=table_name,
                            Key={"PK": {"S": leaderboard_pk}, "SK": {"S": "METADATA"}},
                            UpdateExpression=update_expr,
                            ExpressionAttributeValues=expr_attr_vals
                        )
                    except ClientError as e:
                        logger.warning(f"Could not update metadata for leaderboard {leaderboard_id}: {e}")
                    
                    # Delete user's membership
                    dynamodb_client.delete_item(TableName=table_name, Key={"PK": {"S": pk_value}, "SK": {"S": sk}})
                    audit.add_deletion("Leaderboards")

    except ClientError as e:
        logger.error(f"Error handling Leaderboards for {user_sub}: {e}")
        raise

def handle_leaderboard_invites(user_sub: str, email: str, audit: CleanupAudit) -> int:
    """Deletes invites sent by or to the user."""
    table_name = get_table_name("LeaderboardInvites")
    paginator = dynamodb_client.get_paginator("query")
    items_deleted = 0
    
    try:
        # 1. Sent by user (GSI1)
        audit.add_event(f"Querying invites sent by user {user_sub}", table=table_name, user_sub=user_sub)
        for page in paginator.paginate(
            TableName=table_name,
            IndexName="GSI1",
            KeyConditionExpression="inviter_sub = :sub",
            ExpressionAttributeValues={":sub": {"S": user_sub}}
        ):
            for item in page.get("Items", []):
                audit.add_event(f"Deleting sent invite", table=table_name)
                dynamodb_client.delete_item(TableName=table_name, Key={"PK": item["PK"], "SK": item["SK"]})
                items_deleted += 1
            
        # 2. Sent to user (GSI2)
        if email:
            audit.add_event("Querying invites sent to user email", table=table_name)
            for page in paginator.paginate(
                TableName=table_name,
                IndexName="GSI2",
                KeyConditionExpression="invitee_email = :email",
                ExpressionAttributeValues={":email": {"S": email}}
            ):
                for item in page.get("Items", []):
                    audit.add_event(f"Deleting received invite", table=table_name)
                    dynamodb_client.delete_item(TableName=table_name, Key={"PK": item["PK"], "SK": item["SK"]})
                    items_deleted += 1
        
        return items_deleted
    except ClientError as e:
        logger.error(f"Error handling LeaderboardInvites: {e}")
        raise

def handle_week_score_stats(user_sub: str, audit: CleanupAudit) -> int:
    """Deletes user's weekly score stats by querying GSI1 directly."""
    stats_table = get_table_name("WeekScoreStats")
    user_pk = f"USER#{user_sub}"
    paginator = dynamodb_client.get_paginator("query")
    items_deleted = 0
    
    try:
        audit.add_event(f"Querying WeekScoreStats for user {user_sub} via GSI1", table=stats_table, SK=user_pk)
        
        for page in paginator.paginate(
            TableName=stats_table,
            IndexName="GSI1",
            KeyConditionExpression="SK = :pkval",
            ExpressionAttributeValues={":pkval": {"S": user_pk}}
        ):
            for item in page.get("Items", []):
                pk = item.get("PK", {}).get("S")
                sk = item.get("SK", {}).get("S")
                if pk and sk:
                    dynamodb_client.delete_item(
                        TableName=stats_table,
                        Key={"PK": {"S": pk}, "SK": {"S": sk}}
                    )
                    items_deleted += 1
        
        if items_deleted > 0:
            audit.add_milestone(f"Deleted {items_deleted} weekly score stats items.", count=items_deleted)
            
        return items_deleted
    except ClientError as e:
        logger.error(f"Error handling WeekScoreStats: {e}")
        raise

def handle_s3_results(user_sub: str, audit: CleanupAudit) -> int:
    """Deletes user results from S3."""
    env = os.environ.get("ENVIRONMENT", "preprod")
    bucket_name = f"margana-game-results-{env}"
    prefix = f"public/users/{user_sub}/"
    items_deleted = 0
    
    try:
        audit.add_event(f"Deleting S3 objects in {bucket_name} with prefix {prefix}", bucket=bucket_name, prefix=prefix)
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                audit.add_milestone(
                    "Deleting S3 objects.",
                    bucket=bucket_name,
                    prefix=prefix,
                    count=len(objects)
                )
                s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
                items_deleted += len(objects)
        return items_deleted
    except ClientError as e:
        logger.warning(f"Error deleting S3 objects from {bucket_name}: {e}")
        return items_deleted

def lambda_handler(event, context):
    detail = event.get("detail") or {}
    user_sub = _extract_user_sub(detail)
    
    if not user_sub:
        logger.error("Could not extract user sub from event")
        logger.info(f"Event detail: {json.dumps(detail, default=str)}")
        return {"ok": False, "error": "No user sub found"}
    
    audit = CleanupAudit(user_sub)
    audit.add_milestone(f"Starting data cleanup for user: {user_sub}")
    
    try:
        # Phase 1: Metadata Collection
        # Get email from Marganians
        marganians_table = get_table_name("Marganians")
        
        username = None
        try:
            pk_val = f"USER#{user_sub}"
            resp = dynamodb_client.get_item(
                TableName=marganians_table,
                Key={"PK": {"S": pk_val}, "SK": {"S": "PROFILE"}}
            )
            if "Item" in resp:
                email = resp["Item"].get("email", {}).get("S")
                username = resp["Item"].get("username", {}).get("S")
                if email:
                    audit.add_milestone("Found email for user.")
                if username:
                    audit.add_milestone("Found username for user.")
        except ClientError as e:
            logger.warning(f"Could not retrieve email/username from Marganians: {e}")

        # Phase 2: Dependent Table Cleanup
        handle_leaderboards(user_sub, audit)
        audit.add_milestone("Leaderboards cleanup completed.")

        invites_deleted = handle_leaderboard_invites(user_sub, email, audit)
        audit.add_deletion("LeaderboardInvites", invites_deleted)
        audit.add_milestone("LeaderboardInvites cleanup completed.", count=invites_deleted)

        weekstats_deleted = handle_week_score_stats(user_sub, audit)
        audit.add_deletion("WeekScoreStats", weekstats_deleted)
        audit.add_milestone("WeekScoreStats cleanup completed.", count=weekstats_deleted)
        
        # Phase 3: Independent Table Cleanup
        user_pk = f"USER#{user_sub}"
        badges_deleted = delete_dynamodb_items(get_table_name("UserBadges"), user_pk, audit)
        audit.add_deletion("UserBadges", badges_deleted)
        audit.add_milestone("UserBadges cleanup completed.", count=badges_deleted)

        settings_deleted = delete_dynamodb_items(get_table_name("UserSettings"), user_pk, audit)
        audit.add_deletion("UserSettings", settings_deleted)
        audit.add_milestone("UserSettings cleanup completed.", count=settings_deleted)

        results_deleted = delete_dynamodb_items(get_table_name("MarganaUserResults"), user_pk, audit)
        audit.add_deletion("MarganaUserResults", results_deleted)
        audit.add_milestone("MarganaUserResults cleanup completed.", count=results_deleted)

        s3_deleted = handle_s3_results(user_sub, audit)
        audit.add_deletion("S3Objects", s3_deleted)
        audit.add_milestone("S3 cleanup completed.", count=s3_deleted)
        
        # Phase 4: Final Profile Erasure
        pk_val = f"USER#{user_sub}"
        marganians_deleted = delete_dynamodb_items(marganians_table, pk_val, audit)
        audit.add_deletion("Marganians", marganians_deleted)
        
        # Delete username reservation
        if username:
            lower_uname = username.lower()
            audit.add_event(f"Deleting username reservation: {lower_uname}")
            try:
                dynamodb_client.delete_item(
                    TableName=marganians_table,
                    Key={"PK": {"S": f"USERNAME#{lower_uname}"}, "SK": {"S": "RESERVATION"}}
                )
                audit.add_deletion("Marganians")
            except ClientError as e:
                logger.warning(f"Could not delete username reservation {lower_uname}: {e}")

        if marganians_deleted:
            audit.add_milestone("Marganians cleanup completed.", count=marganians_deleted)
            
        # Final Summary
        summary = audit.get_summary()
        logger.info(f"DATA CLEANUP COMPLETED: {json.dumps(summary, default=str)}")
        
        return {"ok": True, "user_sub": user_sub, "audit": summary}
        
    except Exception as e:
        logger.error(f"Cleanup failed for user {user_sub}: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}
