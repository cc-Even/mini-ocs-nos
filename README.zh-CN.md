# mini-ocs-nos

[English](README.md) | **中文**

mini-ocs-nos 是一个**受 SONiC 架构启发、面向 16×16 模拟光路交换机
（Optical Circuit Switch，OCS）的控制面原型**。它通过 Redis 支撑的多个
服务，将期望配置、编排、设备命令和已确认的运行状态分离。管理面使用官方
gNMI protobuf API，设备边界使用带版本的 Unix 域套接字（UDS）协议。

本项目不是 SONiC 发行版、SONiC 兼容实现、标准 SAI OCS 或硬件驱动。光学
矩阵和端口均为模拟实现。

OCS 在输入端口与输出端口之间建立专用光路。与分组交换机不同，它不会逐跳
检查、缓存、路由或转发数据包。本项目借鉴了 SONiC 中有价值的控制面分层，
以数据库分别表达期望状态、应用状态、设备状态和运行状态，但不复制与分组
交换无关的组件。

## 快速开始

环境要求：Linux 或 WSL2、Docker Engine 与 Compose、Git、GNU Make、
CMake 3.25 或更高版本、支持 C++20 的编译器，以及用于本地控制台测试的
Node.js 22.12 或更高版本。引导脚本会在用户空间安装固定版本的 `uv` 0.12.1
和 Python 3.12，并执行环境预检。

```bash
make bootstrap       # 安装固定版本的 uv/Python，并检查开发环境
make build           # 构建 C++ 服务、同步 Python 依赖并构建前端
make up              # 构建镜像，启动全部服务并等待健康检查通过
docker compose ps

uv run --frozen ocsctl capabilities
uv run --frozen ocsctl connection create ocs0 conn-001 --input 3 --output 11
uv run --frozen ocsctl connection list ocs0
uv run --frozen ocsctl diagnostics show ocs0

# 交互式 16×16 光路矩阵控制台：http://127.0.0.1:8080

make e2e             # 在隔离的 Compose 项目中运行端到端测试
make down            # 停止默认环境，但保留 Redis 命名卷
```

`make up` 会构建并启动 Redis、`ocs-hwsim`、`ocs-orch`、`ocs-syncd`、gNMI
服务器和独立打包的 gNMI Web 网关，并等待所有依赖感知健康检查通过。默认
gNMI 地址为 `127.0.0.1:50051`；提供 REST、WebSocket 和交互式控制台的地址
为 `http://127.0.0.1:8080`。

`make down` 会停止服务但保留 Redis 命名卷。如需同时删除本地运行数据，请
执行：

```bash
docker compose down --volumes
```

开发环境中的 gNMI 和 Web 接口未加密、未认证，并且仅绑定到本机回环地址；
不要将其暴露到不可信网络。Redis 仅位于 Compose 内部网络，不是管理接口。

## 一次 gNMI Set 之后会发生什么

```text
ocsctl / gNMI 客户端
        |
        v
Python gNMI 服务器 --原子 Set--> CONFIG_DB + OCS_CONFIG_EVENTS
                                              |
                                              v
                                         C++ ocs-orch
                                              |
                                     APPL_DB + DEVICE_DB stream
                                              |
                                              v
                                         C++ ocs-syncd
                                              |
                                      带版本的 UDS 请求
                                              |
                                              v
                                   独立 C++ ocs-hwsim 进程
                                              |
                                    已确认的结果/状态/事件
                                              v
                                 STATE_DB / COUNTERS_DB / ALARM_DB
                                              |
                                              v
                                        gNMI Get/Subscribe
```

gNMI Set 成功表示完整候选配置通过校验，并且期望状态快照与一个可靠批量事件
已原子写入 CONFIG_DB。**Set 成功不代表设备已经进入 ACTIVE 状态。** 请使用
Get、`connection create`/`replace` 默认执行的等待逻辑，或 `connection watch`
确认 `apply-status=ACTIVE`，并确认 desired/applied version 相等。

gNMI 服务使用官方协议和 `JSON_IETF` 编码，但公开的是项目原生
`mini-ocs-native` 模型，而不是 OpenConfig YANG 模型。Redis、gRPC、进程
边界、deadline、重试、消费者恢复和 UDS framing 都是真实的软件行为；
16×16 光学矩阵、切换延迟、端口、故障和固件标识则是模拟数据。

组件职责及 Python/C++ 边界见[系统架构](docs/architecture.md)，路径、逻辑
数据库、版本、状态转换和原子性见[数据模型](docs/data-model.md)。

## 可靠性模型

- 所有外部 Redis、gRPC 和 UDS 操作都有 deadline；重试和乐观事务次数均有
  上限。
- Set 校验完整候选配置，因此存在冲突的批次不会改变期望状态或设备状态；
  设备端 batch apply 同样是原子的。
- Redis Streams 和持久化阶段标记允许 orch、syncd 在崩溃后认领 pending
  工作，而不会重复已经完成的副作用。
- UDS 回复丢失时绝不假定变更成功。syncd 会重新连接、读取已确认的硬件真值，
  并且只在完整请求结果可观测时修复状态。
- hwsim 重启会改变 device generation。syncd 会刷新实际状态，并在期望状态
  与实际状态不一致时发布全量快照恢复命令。
- 轮询能够发现带外漂移和端口 DOWN，生成 alarm/counter，并用完整期望快照
  执行 reconciliation。

浏览器网关只转换 gNMI Get/Set/Subscribe，不访问 Redis 或 UDS。其 HTTP
`202` 响应沿用 gNMI Set 的“期望状态已接纳”语义，不代表硬件已经应用成功。
详见 [Web 网关协议](docs/web-gateway.md)。

正式集成路径使用 `UdsDeviceBackend` 和独立 `ocs-hwsim` 进程，可复现演示也
使用同一路径。单元测试保留 `InProcessSimBackend`，用于快速、确定性的领域
逻辑验证，但不会借此绕过正式服务边界。

UDS 是进程间传输协议，不是 Linux 设备驱动或 sysfs 接口。接入真实 FPGA 时，
可以保留 gNMI、Redis 协议、orch 和大部分 syncd 恢复逻辑，并将
`OcsDeviceApi` backend 与模拟器替换为带 deadline 的硬件 backend 及其
驱动或 SDK 集成。

场景 C～H 见[故障与恢复模型](docs/failure-model.md)，MVP 的准确边界见
[已知限制](docs/limitations.md)。

## 运维命令

`ocsctl` 只通过 gNMI 通信，不导入 Redis 客户端。

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

仅开发环境使用的故障子树默认关闭；只有 gNMI 服务器以
`OCS_ENABLE_FAULT_API=1` 启动时才会启用。启用后，`ocsctl fault` 会通过
gNMI、可靠 DEVICE_DB stream、syncd 和 UDS 下发支持的 timeout/error、端口
DOWN 和 clear 命令。它不会直接修改 Redis，也不会自行打开模拟器套接字。
带外漂移、进程崩溃和重启仍由自动化测试控制。准确边界见
[CLI 使用指南](docs/cli.md)。

运行以下命令，可以在与默认环境端口和 Compose 资源隔离的项目中执行自动
清理的引导演示：

```bash
make demo
```

演示覆盖健康状态、正常配置、ON_CHANGE、原子冲突拒绝、timeout 失败、
alarm/counter 证据、恢复、诊断和已打包 E2E 汇总。详见
[演示说明](docs/demo.md)与[最终测试报告](docs/test-report.md)。

## 构建与测试

```bash
make test                         # C++/Python/控制台单元测试、构建与 Ruff
make redis-integration-test       # 真实 Redis + 独立服务进程
make sanitizer-test               # 在 ASan/UBSan 下运行 C++ 单元测试
make sanitizer-integration-test   # 在 ASan/UBSan 下运行 Redis 集成测试
make image                        # 构建五个非 root 服务镜像
make e2e                          # 隔离的完整 Compose gNMI/Web 垂直链路
make demo                         # 引导式运维流程与已打包 E2E
npm run --prefix web test         # 控制台 SVG/API 单元测试
```

集成测试脚本会删除其临时容器和卷，并将日志保存到被 Git 忽略的
`artifacts/test-logs/` 目录。Compose E2E 默认使用端口 50052 和独立项目名；
演示默认使用端口 50053 和自己的项目名，两者都不会复用默认环境。

测试层次、场景 A～H、CI job 和失败产物见[测试指南](docs/testing.md)。

## 文档

- [系统架构](docs/architecture.md)
- [数据模型](docs/data-model.md)
- [故障与恢复模型](docs/failure-model.md)
- [测试指南](docs/testing.md)
- [可复现演示](docs/demo.md)
- [最终测试报告](docs/test-report.md)
- [已知限制](docs/limitations.md)
- [Redis 状态与事件协议](docs/redis-schema.md)
- [CLI 使用指南](docs/cli.md)
- [Web 网关协议](docs/web-gateway.md)
- [交互式控制台](docs/dashboard.md)
- [开发环境](docs/development-environment.md)
- [结构化日志协议](docs/logging.md)
- [架构决策记录](docs/decisions/README.md)
- [官方 gNMI protobuf 来源](proto/README.md)
