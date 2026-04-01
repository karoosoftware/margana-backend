#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-}"

if [[ -z "${MODE}" ]]; then
  echo "Usage: $0 <s3|ses|status [running|stopped]>"
  exit 1
fi

CLUSTER="margana-preprod"
TASK_DEFINITION="margana-puzzle-generator-preprod"
CONTAINER_NAME="margana-puzzle-generator-preprod"
SUBNET_ID="subnet-0f061c3585caf98cb"
SECURITY_GROUP_ID="sg-0920b234ce150b4c7"
REGION="eu-west-2"

case "${MODE}" in
  s3)
    OVERRIDES='{
      "containerOverrides": [
        {
          "name": "'"${CONTAINER_NAME}"'",
          "command": [
            "--get-s3",
            "--s3-bucket","margana-word-game-preprod",
            "--s3-key","usage-logs/margana-puzzle-usage-log.json",
            "--aws-region","eu-west-2"
          ]
        }
      ]
    }'
    ;;
  ses)
    OVERRIDES='{
      "containerOverrides": [
        {
          "name": "'"${CONTAINER_NAME}"'",
          "command": [
            "--send-ses",
            "--ses-from","paul@karoosoftware.com",
            "--ses-to","paul@karoosoftware.com",
            "--ses-subject","Margana ECS SES test",
            "--ses-body","This is a test email from the ECS task role.",
            "--aws-region","eu-west-2"
          ]
        }
      ]
    }'
    ;;
  status)
    STATUS_FILTER="${2:-running}"

    case "${STATUS_FILTER}" in
      running)
        DESIRED_STATUS="RUNNING"
        ;;
      stopped)
        DESIRED_STATUS="STOPPED"
        ;;
      *)
        echo "Unknown status filter: ${STATUS_FILTER}"
        echo "Usage: $0 status [running|stopped]"
        exit 1
        ;;
    esac

    TASK_ARNS="$(
      aws ecs list-tasks \
        --no-cli-pager \
        --no-paginate \
        --cluster "${CLUSTER}" \
        --family "${TASK_DEFINITION}" \
        --desired-status "${DESIRED_STATUS}" \
        --region "${REGION}" \
        --query 'taskArns' \
        --output text
    )"

    if [[ -z "${TASK_ARNS}" || "${TASK_ARNS}" == "None" ]]; then
      echo "No ${STATUS_FILTER} tasks found for ${TASK_DEFINITION} in ${CLUSTER}."
      exit 0
    fi

    aws ecs describe-tasks \
      --no-cli-pager \
      --cluster "${CLUSTER}" \
      --tasks ${TASK_ARNS} \
      --region "${REGION}" \
      --query 'tasks[].{TaskArn:taskArn,LastStatus:lastStatus,DesiredStatus:desiredStatus,LaunchType:launchType,StartedAt:startedAt,StoppedAt:stoppedAt,StopCode:stopCode,StoppedReason:stoppedReason,ContainerReason:containers[0].reason}' \
      --output table
    exit 0
    ;;
  *)
    echo "Unknown mode: ${MODE}"
    echo "Usage: $0 <s3|ses|status [running|stopped]>"
    exit 1
    ;;
esac

aws ecs run-task \
  --no-cli-pager \
  --cluster "${CLUSTER}" \
  --launch-type FARGATE \
  --task-definition "${TASK_DEFINITION}" \
  --network-configuration "awsvpcConfiguration={subnets=[\"${SUBNET_ID}\"],securityGroups=[\"${SECURITY_GROUP_ID}\"],assignPublicIp=ENABLED}" \
  --overrides "${OVERRIDES}" \
  --region "${REGION}" \
  --query 'tasks[].{TaskArn:taskArn,LastStatus:lastStatus,DesiredStatus:desiredStatus,LaunchType:launchType,StartedBy:startedBy,Subnet:attachments[0].details[?name==`subnetId`]|[0].value}' \
  --output table
