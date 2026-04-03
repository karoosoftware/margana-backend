#!/usr/bin/env bash

set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-margana-ecs-test}"
AWS_REGION_VALUE="${AWS_REGION:-eu-west-1}"
START_YEAR="${START_YEAR:-2026}"
START_WEEK="${START_WEEK:-14}"
END_YEAR="${END_YEAR:-2027}"
END_WEEK="${END_WEEK:-13}"
LOG_FILE="${1:-/Users/paulbradbury/IdeaProjects/margana-backend/tmp/container-year-run-$(date +%Y%m%d-%H%M%S).log}"

mkdir -p "$(dirname "${LOG_FILE}")"

run_week() {
  local year="$1"
  local week="$2"
  local week_padded
  printf -v week_padded "%02d" "${week}"

  echo "===== RUN ${year}-W${week_padded} ====="
  docker run --rm \
    -e AWS_REGION="${AWS_REGION_VALUE}" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
    -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    "${IMAGE_NAME}" \
    --target-week "${year}-${week_padded}" \
    --max-usage-tries 800 \
    --print-payload-summary
}

{
  for week in $(seq "${START_WEEK}" 53); do
    run_week "${START_YEAR}" "${week}"
  done

  for week in $(seq 1 "${END_WEEK}"); do
    run_week "${END_YEAR}" "${week}"
  done
} | tee "${LOG_FILE}"

echo
echo "Log written to ${LOG_FILE}"
