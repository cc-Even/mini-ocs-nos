# mini-ocs-nos

mini-ocs-nos is a **SONiC-inspired control-plane prototype for a simulated
16×16 optical circuit switch (OCS)**. It separates desired configuration,
orchestration, device commands, and confirmed operational state across
Redis-backed services. Management uses the official gNMI protobuf API; the
device boundary uses a versioned Unix-domain-socket (UDS) protocol.

This is not a SONiC distribution, a SONiC-compatible implementation, a standard
SAI OCS, or a hardware driver. The optical matrix and ports are simulated.

An OCS establishes dedicated input-to-output circuits. Unlike a packet switch,
it does not inspect, buffer, route, or forward packets hop by hop. The project
borrows SONiC's useful control-plane separation—database-backed desired,
application, device, and operational views—without copying unrelated packet
switch components.

## Quick start

Prerequisites are Linux or WSL2, Docker Engine with Compose, Git, GNU Make,
CMake 3.25 or newer, a C++20 compiler, and Node.js 22.12 or newer for local
dashboard tests. The bootstrap script installs pinned `uv` 0.12.1 and Python
3.12 in user space, then runs preflight checks.

```bash
make bootstrap
make build
make up
docker compose ps

uv run --frozen ocsctl capabilities
uv run --frozen ocsctl connection create ocs0 conn-001 --input 3 --output 11
uv run --frozen ocsctl connection list ocs0
uv run --frozen ocsctl diagnostics show ocs0

# Interactive matrix dashboard: http://127.0.0.1:8080

make down
```

`make up` builds and starts Redis, `ocs-hwsim`, `ocs-orch`, `ocs-syncd`, the
gNMI server, and the separately packaged gNMI-only web gateway, then waits for
their dependency-aware health checks. The default gNMI endpoint is
`127.0.0.1:50051`; the bounded REST/WebSocket gateway is
`http://127.0.0.1:8080`, including the interactive 16×16 dashboard. `make down`
stops the services but preserves the named Redis volume; `docker compose down
--volumes` also deletes local runtime data.

### 中文快速开始

请先安装 Linux/WSL2、Docker Engine 与 Compose、Git、GNU Make、CMake 3.25+、
支持 C++20 的编译器，以及用于本地前端测试的 Node.js 22.12+，然后执行：

```bash
make bootstrap       # 安装固定版本的 uv/Python，并检查开发环境
make build           # 构建 C++ 服务并同步 Python 依赖
make up              # 构建镜像，启动全部服务并等待健康检查通过

uv run --frozen ocsctl connection create ocs0 conn-001 --input 3 --output 11
uv run --frozen ocsctl connection list ocs0
uv run --frozen ocsctl diagnostics show ocs0

make e2e             # 在隔离的 Compose 项目中运行端到端测试
make down            # 停止默认环境；保留 Redis 命名卷
```

开发环境使用未加密、未认证的 gNMI，仅绑定到本机回环地址；不要把它暴露
到不可信网络。Redis 只位于 Compose internal network，不是管理接口。

## What happens after a Set

```text
ocsctl / gNMI client
        |
        v
Python gNMI server --atomic Set--> CONFIG_DB + OCS_CONFIG_EVENTS
                                           |
                                           v
                                      C++ ocs-orch
                                           |
                                  APPL_DB + DEVICE_DB stream
                                           |
                                           v
                                      C++ ocs-syncd
                                           |
                                  versioned UDS request
                                           |
                                           v
                                standalone C++ ocs-hwsim
                                           |
                               confirmed result/state/events
                                           v
                              STATE_DB / COUNTERS_DB / ALARM_DB
                                           |
                                           v
                                     gNMI Get/Subscribe
```

A successful gNMI Set means that the complete candidate passed validation and
its desired snapshots plus one reliable batch event were atomically committed
to CONFIG_DB. **Set success does not mean that the device is ACTIVE.** Use Get,
the default wait performed by `connection create`/`replace`, or
`connection watch` to confirm `apply-status=ACTIVE` and equal desired/applied
versions.

The gNMI service uses the official protocol and `JSON_IETF`, but exposes the
project-native `mini-ocs-native` model rather than an OpenConfig YANG model.
Redis, gRPC, process boundaries, deadlines, retries, consumer recovery, and UDS
framing are real software behavior. The 16×16 optical matrix, switching delay,
ports, faults, and firmware identity are simulated.

See [Architecture](docs/architecture.md) for component ownership and the
Python/C++ boundary, and [Data model](docs/data-model.md) for paths, logical
databases, versions, state transitions, and atomicity.

## Reliability model

- Every external Redis, gRPC, and UDS operation is deadline-bounded; retries
  and optimistic transactions are bounded.
- Set validates the complete candidate, so a conflicting batch changes neither
  desired state nor the device. Device apply is also atomic.
- Redis Streams and durable phase markers let orch and syncd claim pending work
  after a crash without repeating completed effects.
- A mutation whose UDS reply is lost is never assumed successful. syncd
  reconnects, reads confirmed hardware truth, and repairs state only when the
  whole requested result is observable.
- An hwsim restart changes the device generation. syncd refreshes actual state
  and publishes a full-snapshot recovery command when desired and actual state
  differ.
- Polling detects out-of-band drift and port DOWN, raises alarms and counters,
  and reconciles with a complete desired snapshot.

The browser gateway translates only to gNMI Get/Set/Subscribe and has no Redis
or UDS access. Its HTTP `202` response retains the same desired-state admission
semantics as gNMI Set. See the [web gateway contract](docs/web-gateway.md).

The formal integration path uses `UdsDeviceBackend` and a standalone
`ocs-hwsim`, and the reproducible operator demo uses the same path. Unit tests
also keep `InProcessSimBackend` for fast, deterministic domain testing; it is
not used to bypass the formal service boundary.

UDS is an inter-process transport, not a Linux device driver or sysfs interface.
A real FPGA integration would retain gNMI, Redis contracts, orch, and most syncd
recovery logic, while replacing the `OcsDeviceApi` backend and simulator with a
deadline-aware hardware backend and its driver/SDK integration.

See [Failure model](docs/failure-model.md) for scenarios C–H and
[Known limitations](docs/limitations.md) for the exact MVP boundary.

## Operator commands

`ocsctl` communicates only through gNMI and never imports a Redis client.

```bash
uv run --frozen ocsctl device show ocs0
uv run --frozen ocsctl port list ocs0
uv run --frozen ocsctl connection batch ocs0 \
  --connection conn-001:1:9 \
  --connection conn-002:2:10 \
  --connection conn-003:3:11
uv run --frozen ocsctl --json connection watch ocs0 --duration-seconds 30
uv run --frozen ocsctl alarm list ocs0
uv run --frozen ocsctl counters show ocs0
```

The development-only fault subtree is disabled unless the gNMI server starts
with `OCS_ENABLE_FAULT_API=1`. When enabled, `ocsctl fault` sends supported
timeout/error and port DOWN/clear commands through gNMI, a reliable DEVICE_DB
stream, syncd, and UDS. It never edits Redis or opens the simulator socket
itself. Out-of-band drift, process crashes, and restarts remain automated-test
controls. See [the CLI guide](docs/cli.md) for the exact boundary.

For a self-cleaning guided run on ports and Compose resources isolated from the
default stack:

```bash
make demo
```

The demo exercises health, normal configuration, ON_CHANGE, atomic conflict
rejection, timeout failure, alarm/counter evidence, recovery, diagnostics, and
the packaged E2E summary. See [Demo](docs/demo.md) and the validated
[test report](docs/test-report.md).

## Build and test

```bash
make test                         # C++/Python/dashboard unit suites, build, Ruff
make redis-integration-test       # real Redis + standalone service processes
make sanitizer-test               # C++ unit suite under ASan/UBSan
make sanitizer-integration-test   # Redis integration under ASan/UBSan
make image                        # all five non-root service images
make e2e                          # isolated full-Compose gNMI vertical slice
make demo                         # guided operator flow plus packaged E2E
npm run --prefix web test         # dashboard SVG/API unit tests
```

The integration harness removes its temporary containers and volumes and saves
logs under the ignored `artifacts/test-logs/` directory. The Compose E2E uses
port 50052 and an isolated project name. The demo uses port 50053 and its own
project name; neither reuses the default stack.

See [Testing](docs/testing.md) for suite coverage, scenarios A–H, CI jobs, and
failure artifacts.

## Documentation

- [Architecture](docs/architecture.md)
- [Data model](docs/data-model.md)
- [Failure and recovery model](docs/failure-model.md)
- [Testing](docs/testing.md)
- [Reproducible demo](docs/demo.md)
- [Validated test report](docs/test-report.md)
- [Known limitations](docs/limitations.md)
- [Redis state and event contract](docs/redis-schema.md)
- [CLI workflows](docs/cli.md)
- [Web gateway contract](docs/web-gateway.md)
- [Interactive dashboard](docs/dashboard.md)
- [Development environment](docs/development-environment.md)
- [Structured logging contract](docs/logging.md)
- [Architecture decision records](docs/decisions/README.md)
- [Official gNMI protobuf provenance](proto/README.md)

The authoritative product scope remains
[`mini-ocs-network-os-development-spec.md`](mini-ocs-network-os-development-spec.md).
