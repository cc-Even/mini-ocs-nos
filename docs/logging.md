# Structured Logging Contract

All long-running services write one JSON object per line to standard output and
standard error. `config/logging.yaml` lists the stable context fields. A service
includes every field relevant to the operation and omits unavailable values;
`timestamp`, `severity`, `service`, and `message` are mandatory.

Durations use a monotonic clock and are serialized in milliseconds. Event
timestamps use UTC system time. Tests should assert stable fields and error codes,
not complete human-readable messages. Passwords, authentication tokens, and
private keys must never be logged.

Container logging remains on the Docker default driver during MVP. CI captures
Compose logs as failure artifacts in later integration-test iterations.
