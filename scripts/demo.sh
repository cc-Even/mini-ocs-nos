#!/usr/bin/env bash

set -euo pipefail

DEMO_LOG_DIR=${OCS_DEMO_LOG_DIR:-artifacts/demo}
readonly DEMO_LOG_DIR
COMPOSE_PROJECT_NAME=${OCS_DEMO_PROJECT_NAME:-mini-ocs-nos-demo}
readonly COMPOSE_PROJECT_NAME
OCS_GNMI_PORT=${OCS_DEMO_GNMI_PORT:-50053}
readonly OCS_GNMI_PORT
OCS_WEB_PORT=${OCS_DEMO_WEB_PORT:-8083}
readonly OCS_WEB_PORT
OCS_ENABLE_FAULT_API=1
OCS_ORCH_APPLY_RETRY_BASE_MS=${OCS_DEMO_RETRY_MS:-3000}
OCS_ORCH_APPLY_RETRY_MAX_MS=${OCS_DEMO_RETRY_MS:-3000}
UV_CACHE_DIR=${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache}
readonly OCS_ENABLE_FAULT_API OCS_ORCH_APPLY_RETRY_BASE_MS
readonly OCS_ORCH_APPLY_RETRY_MAX_MS UV_CACHE_DIR
export COMPOSE_PROJECT_NAME OCS_GNMI_PORT OCS_WEB_PORT OCS_ENABLE_FAULT_API
export OCS_ORCH_APPLY_RETRY_BASE_MS OCS_ORCH_APPLY_RETRY_MAX_MS UV_CACHE_DIR

mkdir -p "${DEMO_LOG_DIR}"

cleanup() {
    docker compose ps --all >"${DEMO_LOG_DIR}/compose-ps.log" 2>&1 || true
    docker compose logs --no-color >"${DEMO_LOG_DIR}/services.log" 2>&1 || true
    docker compose down --volumes || true
}
trap cleanup EXIT

run_cli() {
    uv run --frozen ocsctl \
        --target "127.0.0.1:${OCS_GNMI_PORT}" \
        --timeout-seconds 8 \
        "$@"
}

wait_for_value() {
    local expected=$1
    shift
    local output=""
    for _ in $(seq 1 80); do
        output=$(run_cli --json "$@" 2>&1 || true)
        if grep -q "${expected}" <<<"${output}"; then
            printf '%s\n' "${output}"
            return 0
        fi
        sleep 0.1
    done
    printf 'Timed out waiting for %s; last output:\n%s\n' "${expected}" "${output}" >&2
    return 1
}

printf '==> Starting isolated mini-ocs-nos demo stack\n'
docker compose up -d --build --wait

printf '\n==> 1. Health and capabilities\n'
run_cli capabilities
run_cli diagnostics show ocs0

printf '\n==> 2. Start bounded ON_CHANGE subscription\n'
run_cli --json connection watch ocs0 --duration-seconds 12 \
    >"${DEMO_LOG_DIR}/connection-watch.log" 2>&1 &
watch_pid=$!
for _ in $(seq 1 50); do
    if grep -q 'sync-response' "${DEMO_LOG_DIR}/connection-watch.log" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if ! grep -q 'sync-response' "${DEMO_LOG_DIR}/connection-watch.log"; then
    printf 'Subscription did not reach sync_response\n' >&2
    exit 1
fi

printf '\n==> 3. Create three valid optical circuits\n'
run_cli connection create ocs0 demo-001 --input 1 --output 9
run_cli connection create ocs0 demo-002 --input 2 --output 10
run_cli connection create ocs0 demo-003 --input 3 --output 11
run_cli connection list ocs0

printf '\n==> 4. Prove conflicting batch rejection is atomic\n'
if run_cli connection batch ocs0 \
    --connection demo-conflict-a:4:12 \
    --connection demo-conflict-b:5:12; then
    printf 'Conflicting batch unexpectedly succeeded\n' >&2
    exit 1
else
    printf 'Conflict rejected as expected; neither member was committed.\n'
fi

printf '\n==> 5. Inject NEXT_APPLY_TIMEOUT through gNMI and syncd\n'
run_cli fault inject ocs0 next-apply-timeout
run_cli connection create ocs0 demo-timeout --input 6 --output 13 --no-wait-active
wait_for_value OCS_APPLY_TIMEOUT get \
    '/ocs/devices/device[name=ocs0]/connections/connection[id=demo-timeout]/state'
run_cli alarm list ocs0
run_cli counters show ocs0

printf '\n==> 6. Clear simulator faults and observe automatic convergence\n'
run_cli fault clear ocs0 --all
wait_for_value '"apply-status": "ACTIVE"' get \
    '/ocs/devices/device[name=ocs0]/connections/connection[id=demo-timeout]/state'
run_cli alarm list ocs0
run_cli diagnostics show ocs0

wait "${watch_pid}"
printf '\n==> 7. Captured ON_CHANGE stream\n'
sed -n '1,240p' "${DEMO_LOG_DIR}/connection-watch.log"

printf '\n==> 8. Run isolated packaged E2E and print its summary\n'
env OCS_ENABLE_FAULT_API=1 OCS_TEST_LOG_DIR="${DEMO_LOG_DIR}/e2e" make e2e

printf '\nDemo completed successfully. Logs: %s\n' "${DEMO_LOG_DIR}"
