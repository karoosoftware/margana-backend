#!/usr/bin/env bash

set -euo pipefail

ARTIFACT_ROOT="${ARTIFACT_ROOT:-${RUNNER_TEMP:-/tmp}/lambda-artifacts}"
LAYER_BUILD_ROOT="${LAYER_BUILD_ROOT:-${RUNNER_TEMP:-/tmp}/shared-layer}"
HANDLER_BUILD_ROOT="${HANDLER_BUILD_ROOT:-${RUNNER_TEMP:-/tmp}/handler-build}"
ARTIFACT_LIST_FILE="${ARTIFACT_LIST_FILE:-${RUNNER_TEMP:-/tmp}/artifact-list.txt}"
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
  local package_name=""
  local package_source_dir=""
  local package_target_dir=""
  local shared_packages=(
    "margana_costing"
    "margana_metrics"
    "margana_score"
  )

  rm -rf "${LAYER_BUILD_ROOT}"
  mkdir -p "${ARTIFACT_ROOT}" "${LAYER_BUILD_ROOT}/python"

  for package_name in "${shared_packages[@]}"; do
    package_source_dir="layer-root/python/${package_name}"
    package_target_dir="${LAYER_BUILD_ROOT}/python/${package_name}"

    if [[ ! -d "${package_source_dir}" ]]; then
      echo "Shared layer package not found: ${package_source_dir}" >&2
      exit 1
    fi

    cp -R "${package_source_dir}" "${package_target_dir}"
  done

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
