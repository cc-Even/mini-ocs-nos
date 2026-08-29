# Development Environment

## Supported baseline

- Linux or WSL2 on x86-64
- Docker Engine with Docker Compose
- Python 3.12 managed by pinned `uv` 0.12.1
- CMake 3.25 or newer and a C++20 compiler
- Node.js 22.12 or newer and npm for local dashboard builds/tests
- GNU Make and Git

Run:

```bash
scripts/bootstrap.sh
```

The bootstrap installs only the pinned user-space Python tool manager and Python
3.12. It does not attempt privileged Docker or Node.js installation. It finishes
by running the strict preflight gate.

## WSL2 and Docker Desktop

If preflight can see Docker Desktop files but reports that the daemon is
unreachable:

1. Open Docker Desktop on Windows.
2. Open **Settings > Resources > WSL Integration**.
3. Enable integration for the WSL distribution containing this repository.
4. Apply the change, reopen the WSL shell, and run `scripts/preflight.sh`.

The gate requires a real daemon response; finding a Docker CLI binary alone is
not sufficient. A native Docker Engine installation is also supported when both
`docker info` and Compose work for the current unprivileged user.

## Version policy

`.python-version` selects Python 3.12. The exact patch release may advance within
the 3.12 line when bootstrap is rerun, while the `uv` tool itself remains pinned.
Application dependencies will be locked separately by the Python project in
Iteration 02.

The dashboard production builder pins Node 22.22.3 by immutable container
digest. `web/package-lock.json` pins the TypeScript/Vite/Vitest/Playwright
toolchain and all transitive npm dependencies. A newer compatible host Node may
run local checks, but image and browser acceptance remain on the pinned tools.

## Hardened test profiles

Run the complete non-Redis regression with `make test`. Run the C++ suite under
AddressSanitizer and UndefinedBehaviorSanitizer with:

```bash
make sanitizer-test
```

The sanitizer preset uses a separate `build/sanitizer` tree, keeps frame
pointers, enables leak detection, and stops on the first ASan or UBSan finding.
It requires GCC or Clang. With Docker available, run
`make sanitizer-integration-test` to execute the Redis-dependent orch/syncd
suite with the same instrumentation.

`make redis-integration-test` writes command output, Redis container logs, and
captured service-process logs below `artifacts/test-logs/redis-integration`.
The directory is intentionally ignored by Git; CI uploads it when a test job
fails so the local worktree remains clean after successful acceptance runs.
