#!/usr/bin/env bash

set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-margana-ecs-test}"
AWS_REGION_VALUE="${AWS_REGION:-eu-west-1}"
START_YEAR="${START_YEAR:-2026}"
START_WEEK="${START_WEEK:-1}"
YEARS_TO_RUN="${YEARS_TO_RUN:-5}"
MAX_USAGE_TRIES="${MAX_USAGE_TRIES:-800}"
COOLDOWN_DAYS="${COOLDOWN_DAYS:-365000}"
LOG_DIR="${1:-/Users/paulbradbury/IdeaProjects/margana-backend/tmp/five-year-weekly-sweep}"

mkdir -p "${LOG_DIR}"

run_week() {
  local year="$1"
  local week="$2"
  local log_file="$3"
  local week_padded
  printf -v week_padded "%02d" "${week}"

  {
    echo "===== RUN ${year}-W${week_padded} ====="
    docker run --rm \
      -e AWS_REGION="${AWS_REGION_VALUE}" \
      -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
      -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
      -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
      "${IMAGE_NAME}" \
      --target-week "${year}-${week_padded}" \
      --max-usage-tries "${MAX_USAGE_TRIES}" \
      --cooldown-days "${COOLDOWN_DAYS}" \
      --print-payload-summary
  } | tee -a "${log_file}"
}

end_year=$((START_YEAR + YEARS_TO_RUN - 1))

for year in $(seq "${START_YEAR}" "${end_year}"); do
  year_log="${LOG_DIR}/${year}-weekly-sweep.log"
  : > "${year_log}"
  if [[ "${year}" -eq "${START_YEAR}" ]]; then
    first_week="${START_WEEK}"
  else
    first_week=1
  fi

  for week in $(seq "${first_week}" 53); do
    run_week "${year}" "${week}" "${year_log}"
  done

  echo
  echo "Year ${year} log written to ${year_log}"
done
