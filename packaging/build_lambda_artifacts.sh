#!/usr/bin/env bash

set -euo pipefail

ARTIFACT_ROOT="${ARTIFACT_ROOT:-${RUNNER_TEMP:-/tmp}/lambda-artifacts}"
LAYER_BUILD_ROOT="${LAYER_BUILD_ROOT:-${RUNNER_TEMP:-/tmp}/shared-layer}"
HANDLER_BUILD_ROOT="${HANDLER_BUILD_ROOT:-${RUNNER_TEMP:-/tmp}/handler-build}"
ARTIFACT_LIST_FILE="${ARTIFACT_LIST_FILE:-${RUNNER_TEMP:-/tmp}/artifact-list.txt}"
RUNTIME_REQUIREMENTS_FILE="${RUNTIME_REQUIREMENTS_FILE:-requirements.txt}"
SHA_SHORT="${SHA_SHORT:-${GITHUB_SHA:0:7}}"

if [[ -z "${SHA_SHORT}" ]]; then
  echo "SHA_SHORT is required" >&2
  exit 1
fi

derive_logical_name() {
  local source_file="$1"
  local base_name

  base_name="$(basename "${source_file}" .py)"
  base_name="${base_name#lambda_}"
  base_name="${base_name#lambda-}"
  echo "${base_name//_/-}"
}

build_shared_layer() {
  local artifact_name="shared-python-deps-layer__${SHA_SHORT}.zip"
  local artifact_path="${ARTIFACT_ROOT}/${artifact_name}"

  rm -rf "${LAYER_BUILD_ROOT}"
  mkdir -p "${ARTIFACT_ROOT}" "${LAYER_BUILD_ROOT}/python"

  if [[ ! -f "${RUNTIME_REQUIREMENTS_FILE}" ]]; then
    echo "Runtime requirements file not found: ${RUNTIME_REQUIREMENTS_FILE}" >&2
    exit 1
  fi

  python -m pip install --no-deps . --target "${LAYER_BUILD_ROOT}/python"
  python -m pip install -r "${RUNTIME_REQUIREMENTS_FILE}" --target "${LAYER_BUILD_ROOT}/python"

  test -s "${LAYER_BUILD_ROOT}/python/margana_metrics/badge-milestones.json"
  test -s "${LAYER_BUILD_ROOT}/python/margana_score/data/margana-word-list.txt"

  (
    cd "${LAYER_BUILD_ROOT}"
    zip -qr "${artifact_path}" python
  )

  printf '%s\n' "${artifact_name}" > "${ARTIFACT_LIST_FILE}"
}

build_handler_artifacts() {
  local source_file=""
  local logical_name=""
  local stage_dir=""
  local artifact_name=""
  local artifact_path=""

  rm -rf "${HANDLER_BUILD_ROOT}"
  mkdir -p "${HANDLER_BUILD_ROOT}"

  while IFS= read -r source_file; do
    logical_name="$(derive_logical_name "${source_file}")"
    stage_dir="${HANDLER_BUILD_ROOT}/${logical_name}"
    artifact_name="${logical_name}__${SHA_SHORT}.zip"
    artifact_path="${ARTIFACT_ROOT}/${artifact_name}"

    rm -rf "${stage_dir}"
    mkdir -p "${stage_dir}"

    cp "${source_file}" "${stage_dir}/$(basename "${source_file}")"
    test -s "${stage_dir}/$(basename "${source_file}")"

    (
      cd "${stage_dir}"
      zip -qr "${artifact_path}" .
    )

    printf '%s\n' "${artifact_name}" >> "${ARTIFACT_LIST_FILE}"
  done < <(find lambdas -maxdepth 1 -type f -name '*.py' ! -name '__init__.py' | sort)
}

build_shared_layer
build_handler_artifacts
