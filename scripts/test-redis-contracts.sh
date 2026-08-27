#!/usr/bin/env bash

set -euo pipefail

TEST_LOG_DIR=${OCS_TEST_LOG_DIR:-artifacts/test-logs/redis-integration}
readonly TEST_LOG_DIR
CTEST_DIR=${OCS_CTEST_DIR:-build/dev}
readonly CTEST_DIR
RUN_PYTHON_INTEGRATION=${OCS_RUN_PYTHON_INTEGRATION:-1}
readonly RUN_PYTHON_INTEGRATION
if [[ "${RUN_PYTHON_INTEGRATION}" != "0" && "${RUN_PYTHON_INTEGRATION}" != "1" ]]; then
    printf 'OCS_RUN_PYTHON_INTEGRATION must be 0 or 1\n' >&2
    exit 2
fi
REDIS_RUNTIME_DIR=$(mktemp -d --tmpdir mini-ocs-redis-runtime.XXXXXX)
readonly REDIS_RUNTIME_DIR
chmod 0733 "${REDIS_RUNTIME_DIR}"
mkdir -p "${TEST_LOG_DIR}"
export OCS_REDIS_RUNTIME_DIR="${REDIS_RUNTIME_DIR}"
export OCS_TEST_LOG_DIR="${TEST_LOG_DIR}"

cleanup() {
    docker compose ps --all >"${TEST_LOG_DIR}/compose-ps.log" 2>&1 || true
    docker compose logs --no-color >"${TEST_LOG_DIR}/redis-service.log" 2>&1 || true
    docker compose down --volumes || true
    rm -f "${REDIS_RUNTIME_DIR}/redis.sock"
    rmdir "${REDIS_RUNTIME_DIR}" || true
}
trap cleanup EXIT

docker compose up -d --wait redis 2>&1 | tee "${TEST_LOG_DIR}/compose-up.log"
test "$(docker compose exec -T redis redis-cli ping)" = "PONG"

OCS_REDIS_SOCKET="${REDIS_RUNTIME_DIR}/redis.sock" \
ctest --test-dir "${CTEST_DIR}" --output-on-failure \
    -R "RedisContractTest|SyncdIntegrationTest|OrchestratorIntegrationTest" \
    2>&1 | tee "${TEST_LOG_DIR}/cpp-integration.log"

if [[ "${RUN_PYTHON_INTEGRATION}" == "1" ]]; then
    OCS_REDIS_SOCKET="${REDIS_RUNTIME_DIR}/redis.sock" \
    UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache}" \
    uv run --frozen pytest python/tests/integration --no-header -q \
        2>&1 | tee "${TEST_LOG_DIR}/python-integration.log"
fi
