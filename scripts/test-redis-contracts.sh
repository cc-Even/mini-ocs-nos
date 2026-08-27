#!/usr/bin/env bash

set -euo pipefail

REDIS_RUNTIME_DIR=$(mktemp -d --tmpdir mini-ocs-redis-runtime.XXXXXX)
readonly REDIS_RUNTIME_DIR
chmod 0733 "${REDIS_RUNTIME_DIR}"
export OCS_REDIS_RUNTIME_DIR="${REDIS_RUNTIME_DIR}"

cleanup() {
    docker compose down --volumes
    rm -f "${REDIS_RUNTIME_DIR}/redis.sock"
    rmdir "${REDIS_RUNTIME_DIR}"
}
trap cleanup EXIT

docker compose up -d --wait redis
test "$(docker compose exec -T redis redis-cli ping)" = "PONG"

OCS_REDIS_SOCKET="${REDIS_RUNTIME_DIR}/redis.sock" \
ctest --test-dir build/dev --output-on-failure \
    -R "RedisContractTest|SyncdIntegrationTest|OrchestratorIntegrationTest"
