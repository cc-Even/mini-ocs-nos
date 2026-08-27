#!/usr/bin/env bash

set -euo pipefail

TEST_LOG_DIR=${OCS_TEST_LOG_DIR:-artifacts/test-logs/compose-e2e}
readonly TEST_LOG_DIR
COMPOSE_PROJECT_NAME=${OCS_E2E_PROJECT_NAME:-mini-ocs-nos-e2e}
readonly COMPOSE_PROJECT_NAME
OCS_GNMI_PORT=${OCS_E2E_GNMI_PORT:-50052}
readonly OCS_GNMI_PORT
mkdir -p "${TEST_LOG_DIR}"
export COMPOSE_PROJECT_NAME OCS_GNMI_PORT

cleanup() {
    docker compose ps --all >"${TEST_LOG_DIR}/compose-ps.log" 2>&1 || true
    docker compose logs --no-color >"${TEST_LOG_DIR}/services.log" 2>&1 || true
    docker compose down --volumes || true
}
trap cleanup EXIT

docker compose up -d --build --wait 2>&1 | tee "${TEST_LOG_DIR}/compose-up.log"
OCS_COMPOSE_GNMI_TARGET="127.0.0.1:${OCS_GNMI_PORT}" \
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache}" \
uv run --frozen pytest python/tests/e2e --no-header -q \
    2>&1 | tee "${TEST_LOG_DIR}/pytest.log"
