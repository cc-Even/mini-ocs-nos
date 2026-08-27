#!/usr/bin/env bash

set -euo pipefail

readonly UV_VERSION="0.12.1"
readonly PYTHON_MINOR="3.12"
readonly UV_INSTALL_URL="https://astral.sh/uv/${UV_VERSION}/install.sh"
readonly LOCAL_BIN_DIR="${HOME}/.local/bin"
readonly UV_EXECUTABLE="${LOCAL_BIN_DIR}/uv"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache}"

install_uv() {
    local installer_path
    installer_path=$(mktemp --tmpdir mini-ocs-uv-installer.XXXXXX.sh)
    trap 'rm -f "${installer_path}"' RETURN

    printf 'Installing pinned uv %s...\n' "${UV_VERSION}"
    curl --proto '=https' --tlsv1.2 -LsSf "${UV_INSTALL_URL}" -o "${installer_path}"
    UV_NO_MODIFY_PATH=1 sh "${installer_path}"
}

if [[ ! -x "${UV_EXECUTABLE}" ]] || [[ "$("${UV_EXECUTABLE}" --version | awk '{print $2}')" != "${UV_VERSION}" ]]; then
    install_uv
fi

export PATH="${LOCAL_BIN_DIR}:${PATH}"
if ! uv python find "${PYTHON_MINOR}" >/dev/null 2>&1; then
    uv python install "${PYTHON_MINOR}"
fi

printf 'Python toolchain: %s\n' "$(uv python find "${PYTHON_MINOR}")"
printf 'Running repository preflight...\n'
exec "$(dirname "$0")/preflight.sh"
