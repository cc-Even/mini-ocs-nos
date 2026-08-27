# mini-ocs-nos

mini-ocs-nos is a small optical-circuit-switch network OS built around a native
gNMI management API, reliable Redis streams, a C++ orchestration/device plane,
and a standalone Unix-socket hardware simulator. The authoritative scope is in
[`mini-ocs-network-os-development-spec.md`](mini-ocs-network-os-development-spec.md).

The development profile uses insecure gNMI and binds it to localhost. It is for
local development only; do not expose the endpoint to an untrusted network.
Redis is kept on the internal Compose network and is not a management API.

## Validate the repository

```bash
make test
make redis-integration-test
```

The second command starts pinned Redis in Docker, uses a temporary Unix socket,
runs the real Redis/UDS/gNMI tests, and removes its containers and volumes.

## Management semantics

`ocsctl` communicates only with gNMI. A successful Set means that the complete
candidate passed validation and its desired snapshots plus reliable events were
atomically committed to CONFIG_DB. It does **not** claim that hardware applied
the change. Use Get, `connection create --wait-active`, or
`connection watch` to confirm `apply-status=ACTIVE` and matching desired/applied
versions.

Start `ocs-hwsim` and `ocs-syncd` before accepting configuration. During syncd
startup, the backend handshake initializes the valid device inventory in
CONFIG_DB; gNMI rejects device names that were not discovered this way.

After `make build`, start the management server with `gnmi-server` and inspect
the CLI with:

```bash
ocsctl --help
ocsctl capabilities
ocsctl connection create ocs0 conn-001 --input 3 --output 11
ocsctl connection list ocs0
ocsctl connection watch ocs0 --duration-seconds 30
```

See [docs/cli.md](docs/cli.md) for the command and JSON workflows and
[docs/development-environment.md](docs/development-environment.md) for host
setup.
