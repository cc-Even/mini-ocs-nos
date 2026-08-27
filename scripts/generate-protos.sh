#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
proto_root="${repo_root}/proto"
python_root="${repo_root}/python"
uv_cache_dir="${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache}"

gnmi_proto="github.com/openconfig/gnmi/proto/gnmi/gnmi.proto"
gnmi_ext_proto="github.com/openconfig/gnmi/proto/gnmi_ext/gnmi_ext.proto"

(cd "${proto_root}" && sha256sum --check SHA256SUMS)

UV_CACHE_DIR="${uv_cache_dir}" uv run --frozen --no-sync python -m grpc_tools.protoc \
  --proto_path="${proto_root}" \
  --python_out="${python_root}" \
  "${proto_root}/${gnmi_ext_proto}" \
  "${proto_root}/${gnmi_proto}"

UV_CACHE_DIR="${uv_cache_dir}" uv run --frozen --no-sync python -m grpc_tools.protoc \
  --proto_path="${proto_root}" \
  --grpc_python_out="${python_root}" \
  "${proto_root}/${gnmi_proto}"
