# Observability

squirix nodes expose HTTP health and metrics endpoints plus server-side OpenTelemetry tracing.

## Health routes

| Route | Purpose |
| --- | --- |
| `GET /health` | Aggregate health |
| `GET /health/live` | Liveness probe |
| `GET /health/ready` | Readiness probe |
| `GET /health/ready/details` | JSON diagnostics payload |

Readiness stays unhealthy until journal recovery completes. Fatal journal maintenance failures also affect readiness.
Critical memory pressure does **not** flip readiness by itself — use `/health/ready/details` and metrics for capacity
incidents.

`/health/ready/details` includes journal backlog, snapshot age, compaction state, client pool peers, coordination
leases, and memory pressure aggregates (no raw keys or values).

Full field reference: [diagnostics.md](diagnostics.md).

## Metrics

`GET /metrics` — Prometheus text exposition (enabled by default; configurable in settings).

Loopback scrapes are anonymous; remote clients (including host → Docker published ports) must send `X-Api-Key` or a JWT
when server auth is enabled. Details: [diagnostics — Metrics route](diagnostics.md#metrics-route).

Client-side bootstrap warm-up skips emit `squirix_client_pool_bootstrap_warmup_skipped_total`.

## Tracing

- Server journal operations emit OpenTelemetry spans
- Client logical cache operations emit spans through `TracingCacheDecorator<T>`
- Bootstrap warm-up skips emit `client.bootstrap.warmup.peer_skipped` activities

Use logical spans (`cache.operation`, `cache.result`) for operation triage and gRPC interceptor spans for transport
failures.

## Operational response

When a node behaves unexpectedly:

1. Check `/health/ready/details` and metrics
2. Capture logs and configuration before restart
3. Back up the data directory before repair or upgrade

Runbook: [operational-runbook.md](operational-runbook.md).
