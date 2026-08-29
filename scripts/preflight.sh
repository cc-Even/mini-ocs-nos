#!/usr/bin/env bash

set -uo pipefail

readonly REQUIRED_PYTHON_MINOR="3.12"
readonly REQUIRED_UV_VERSION="0.12.1"
readonly DOCKER_DESKTOP_CLI="/mnt/wsl/docker-desktop/cli-tools/usr/bin/docker"
readonly DOCKER_DESKTOP_COMPOSE="/mnt/wsl/docker-desktop/cli-tools/usr/local/lib/docker/cli-plugins/docker-compose"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache}"

failures=0

pass() {
    printf 'PASS  %s\n' "$1"
}

fail() {
    printf 'FAIL  %s\n' "$1" >&2
    failures=$((failures + 1))
}

check_command() {
    local command_name="$1"
    local version_argument="$2"

    if ! command -v "${command_name}" >/dev/null 2>&1; then
        fail "${command_name} is not installed or not on PATH"
        return
    fi

    local version_output
    if ! version_output=$("${command_name}" "${version_argument}" 2>&1); then
        fail "${command_name} failed its version check"
        return
    fi
    pass "${command_name}: $(printf '%s\n' "${version_output}" | head -n 1)"
}

resolve_docker() {
    if command -v docker >/dev/null 2>&1; then
        command -v docker
    elif [[ -x "${DOCKER_DESKTOP_CLI}" ]]; then
        printf '%s\n' "${DOCKER_DESKTOP_CLI}"
    else
        return 1
    fi
}

resolve_compose() {
    local docker_cli="$1"

    if "${docker_cli}" compose version >/dev/null 2>&1; then
        printf '%s\n' "${docker_cli} compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        command -v docker-compose
    elif [[ -x "${DOCKER_DESKTOP_COMPOSE}" ]]; then
        printf '%s\n' "${DOCKER_DESKTOP_COMPOSE}"
    else
        return 1
    fi
}

check_python() {
    if ! command -v uv >/dev/null 2>&1; then
        fail "uv ${REQUIRED_UV_VERSION} is required; run scripts/bootstrap.sh"
        return
    fi

    local uv_version
    uv_version=$(uv --version | awk '{print $2}')
    if [[ "${uv_version}" != "${REQUIRED_UV_VERSION}" ]]; then
        fail "uv ${REQUIRED_UV_VERSION} is required, found ${uv_version}"
        return
    fi
    pass "uv: ${uv_version}"

    local python_path
    if ! python_path=$(uv python find "${REQUIRED_PYTHON_MINOR}" 2>/dev/null); then
        fail "Python ${REQUIRED_PYTHON_MINOR} is unavailable; run scripts/bootstrap.sh"
        return
    fi

    local python_version
    python_version=$("${python_path}" -c 'import platform; print(platform.python_version())')
    if [[ "${python_version}" != "${REQUIRED_PYTHON_MINOR}."* ]]; then
        fail "Python ${REQUIRED_PYTHON_MINOR}.x is required, found ${python_version}"
        return
    fi
    pass "python: ${python_version} (${python_path})"
}

check_node() {
    if ! command -v node >/dev/null 2>&1; then
        fail "Node.js 22.12 or newer is required for dashboard checks"
        return
    fi
    local node_version
    node_version=$(node --version)
    if ! node -e 'const [major, minor] = process.versions.node.split(".").map(Number); process.exit(major > 22 || (major === 22 && minor >= 12) ? 0 : 1)'; then
        fail "Node.js 22.12 or newer is required, found ${node_version}"
        return
    fi
    pass "node: ${node_version}"
}

check_docker() {
    local docker_cli
    if ! docker_cli=$(resolve_docker); then
        fail "Docker CLI is unavailable"
        return
    fi
    pass "docker client: $(${docker_cli} --version)"

    local compose_cli
    if ! compose_cli=$(resolve_compose "${docker_cli}"); then
        fail "Docker Compose is unavailable"
    else
        local compose_version
        if [[ "${compose_cli}" == "${docker_cli} compose" ]]; then
            compose_version=$(${docker_cli} compose version)
        else
            compose_version=$(${compose_cli} version)
        fi
        pass "compose: ${compose_version}"
    fi

    if ! "${docker_cli}" info >/dev/null 2>&1; then
        fail "Docker daemon is unreachable. Under WSL2, enable Docker Desktop Settings > Resources > WSL Integration for this distribution, reopen the shell, and rerun this script"
        return
    fi
    pass "docker daemon is reachable"
}

check_command git --version
check_command cmake --version
check_command g++ --version
check_command make --version
check_node
check_command npm --version
check_python
check_docker

if (( failures > 0 )); then
    printf '\nPreflight failed with %d problem(s).\n' "${failures}" >&2
    exit 1
fi

printf '\nPreflight passed. The Iteration 01 toolchain gate is satisfied.\n'
