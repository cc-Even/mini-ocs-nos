# Vendored gNMI protocol sources

The gNMI protobuf definitions in `github.com/openconfig/gnmi` are vendored from
the official OpenConfig `gnmi` repository:

- Source: <https://github.com/openconfig/gnmi>
- Tag: `v0.14.1`
- Commit: `8b7dd494c4f6ff517431965d662621d8884bad0f`
- License: Apache License 2.0; see
  `github.com/openconfig/gnmi/LICENSE`

Vendored file checksums are recorded in `SHA256SUMS` and verified before every
generation. The proto entries, relative to the upstream repository, are:

```text
45abf90bfee289544e2430c8ce1b5b10e4f851736a508ddba54d3cf12e82f7b7  proto/gnmi/gnmi.proto
b0e96bd0c540cf249512783ee5abeb3f0d05805115f18c7d714ac90316981e2e  proto/gnmi_ext/gnmi_ext.proto
```

The upstream directory structure is retained because `gnmi.proto` imports
`github.com/openconfig/gnmi/proto/gnmi_ext/gnmi_ext.proto`. Regenerate the
checked-in Python bindings with the pinned `grpcio-tools` dependency:

```bash
make generate-protos
```

The generated modules are placed below `python/github/com/openconfig/gnmi` and
are re-exported through `gnmi_server.proto` for application code.
