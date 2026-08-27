FROM python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2 AS builder

COPY --from=ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded /uv /usr/local/bin/uv

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates=20250419~deb12u1 \
        cmake=3.25.1-1 \
        g++=4:12.2.0-3 \
        ninja-build=1.11.1-2~deb12u1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /source
COPY CMakeLists.txt CMakePresets.json ./
COPY cpp ./cpp
RUN cmake -S . -B build/runtime -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTING=OFF \
    && cmake --build build/runtime --parallel

COPY pyproject.toml uv.lock .python-version ./
COPY python ./python
COPY proto/github.com/openconfig/gnmi/LICENSE ./proto/github.com/openconfig/gnmi/LICENSE
RUN UV_PROJECT_ENVIRONMENT=/opt/mini-ocs/.venv \
    uv sync --frozen --no-dev --no-editable --compile-bytecode

FROM python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2 AS runtime

LABEL org.opencontainers.image.title="mini-ocs-nos services" \
      org.opencontainers.image.description="SONiC-inspired control plane for a simulated OCS" \
      org.opencontainers.image.version="0.1.0"

ENV PATH="/opt/mini-ocs/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=builder /source/build/runtime/cpp/ocs-hwsim /usr/local/bin/ocs-hwsim
COPY --from=builder /source/build/runtime/cpp/ocs-hwsimctl /usr/local/bin/ocs-hwsimctl
COPY --from=builder /source/build/runtime/cpp/ocs-orch /usr/local/bin/ocs-orch
COPY --from=builder /source/build/runtime/cpp/ocs-syncd /usr/local/bin/ocs-syncd
COPY --from=builder /opt/mini-ocs/.venv /opt/mini-ocs/.venv

RUN groupadd --gid 10001 mini-ocs \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin mini-ocs \
    && install -d --owner=10001 --group=10001 --mode=0770 /run/mini-ocs

USER 10001:10001
WORKDIR /opt/mini-ocs

FROM runtime AS hwsim
ENTRYPOINT ["ocs-hwsim"]

FROM runtime AS syncd
ENTRYPOINT ["ocs-syncd"]

FROM runtime AS orch
ENTRYPOINT ["ocs-orch"]

FROM runtime AS gnmi
ENTRYPOINT ["gnmi-server"]
