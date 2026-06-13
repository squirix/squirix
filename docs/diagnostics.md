# Diagnostics and Health

For a route summary, see [observability.md](observability.md). This document describes the machine-readable health and
diagnostics surfaces currently exposed by the node host.

## Health routes

- `GET /health`
- `GET /health/live`
- `GET /health/ready`
- `GET /health/ready/details`

## Readiness details

`GET /health/ready/details` returns a JSON payload with:

- `journalBacklogOps` (persistence enabled): journal operations not covered by the latest snapshot watermark
- `snapshotAgeSeconds` (persistence enabled): age of the latest snapshot, or `null` if no snapshot exists
- `snapshotInFlight` (persistence enabled): whether a snapshot is currently running
- `compaction.state` (persistence enabled): current journal compaction service state
- `compaction.lastRunUtc`
- `compaction.inFlight`
- `clientPool.configured`
- `clientPool.peers`
- `coordination.leases`
- `coordination.watches`
- `memoryPressure.state`: coarse pressure derived from configured limits and **decorator-maintained** approximate
  accounting — `normal`, `high`, or `critical` (see [configuration.md#memory-pressure-squirixsettingsjson](configuration.md#memory-pressure-squirixsettingsjson)).
  `LocalCache<T>` does not own this policy.
- `memoryPressure.maxEstimatedCacheBytes`: resolved estimated byte limit (explicit setting or 80% RAM default)
- `memoryPressure.estimatedCacheBytes`: current global approximate accounted bytes for the node
- `memoryPressure.entryCount`: current global approximate accounted live entry count
- `memoryPressure.rejectedWriteCount`: number of memory admission rejections recorded since process start for this
  accounting instance
- `memoryPressure.writeRejectionActive`: always `true`; growing writes are rejected at critical pressure

Readiness behavior (`GET /health/ready`):

- The route is the machine readiness probe for schedulers and load balancers.
- When persistence is enabled, `journal_recovery` is **Unhealthy** until journal startup recovery opens the gate.
  Ephemeral nodes omit journal recovery checks.
- `journal_maintenance` is **Unhealthy** after a fatal journal periodic flush-loop failure, a failed journal compaction
  state, or a fatal snapshot trigger failure.
- The default ASP.NET Core readiness check is unchanged: **normal** and **high** memory pressure do **not** fail
  readiness by themselves.
- **Critical** pressure does **not** flip readiness to unhealthy in the current host: operators rely on
  `/health/ready/details` (`memoryPressure`) and metrics for visibility. Treat **critical** plus rising
  `rejectedWriteCount` as a capacity incident; see [operational-runbook.md](operational-runbook.md).

Privacy and bounds:

- This payload intentionally exposes **only aggregates** produced from `IMemoryUsageAccounting` (maintained by
  memory-accounting decorators, not by `LocalCache<T>` directly). It does **not** include raw keys, values, serialized
  value previews, or per-cache/per-entry listings.
- User-controlled cache names are **not** enumerated here.

Current limitation:

- `coordination.watches` reports `configured = false` and zero counters because watch coordination metrics are not
  exposed by the squirix node host.

This route is a readiness/diagnostics payload, not a complete observability surface.

Access control matches `/metrics`: loopback clients may scrape anonymously; remote clients must authenticate with
the same JWT bearer token used for cache routes when server auth is enabled. `/health`, `/health/live`, and
`/health/ready` stay anonymous for probes.

## Logical operation tracing

Logical cache operation spans are owned by `TracingCacheDecorator<T>` in the hosted cache pipeline. The decorator wraps
validation so rejected invalid requests are still observable, but it does not change cancellation, exception, or
operation behavior.

Spans are emitted through the shared `Squirix` `ActivitySource` with bounded names such as `cache.get`,
`cache.get_entry`, `cache.get_expiration`, `cache.insert`, `cache.add`, `cache.try_add`, `cache.remove`,
`cache.try_remove`, `cache.contains`, and `cache.touch` (server pipeline; exported client
`RemoveExpirationAsync`). Tags are intentionally limited to:

- `cache.operation`
- `cache.result`
- `squirix.node_id`

Logical operation tracing reuses `CacheOperationNames`, `CacheOperationResults`, and `CacheOperationClassifier`; it does
not fork operation labels or exception classification. It must not include raw keys, raw values, serialized payloads,
exception messages, unbounded cache names, or other user-controlled high-cardinality values.

Ownership boundaries:

- `TracingCacheDecorator<T>` owns logical cache operation spans.
- RPC interceptors own transport-level gRPC spans and correlation.
- journal, snapshot, and compaction components own storage-specific spans.
- Memory-pressure components own memory-pressure diagnostics.

## Metrics ownership

Generic logical cache operation metrics (`squirix_ops_total`, `squirix_op_latency_seconds`) are recorded by
`MetricsCacheDecorator<T>`. Operation names and result categories use shared server classifiers (`CacheOperationNames`,
`CacheOperationResults`, `CacheOperationClassifier`). `MetricsCacheDecorator<T>` bridges `INamespacedCache<T>` to
`CacheMetrics.RecordOperation` using those types.

These instruments describe logical cache operations only and use bounded `operation` / `result` labels on the
`Squirix` meter. They also record a `cache` tag internally for debugging through OpenTelemetry and other
`MeterListener` exporters. The HTTP `/metrics` public scrape profile strips `cache` and `exception_type` before export.

Missing reads are reported as `not_found` when the API shape can distinguish them (`GetValueAsync`, `GetEntryAsync`, and
remove paths). Use `GetValueAsync` or `GetEntryAsync` when metrics need miss classification.

Memory-pressure metrics remain owned by `MemoryPressureMetricsService`, `MemoryPressureGate`, and memory-pressure
components; they are not part of the generic operation observability model. journal, snapshot, compaction, recovery,
manifest, and storage health metrics remain owned by the storage layer (`JournalWriter`, `JournalReader`,
`SnapshotCoordinator`, `JournalMetricsExporterService`, and related storage services).

Backpressure metrics are owned by `BackpressureGate` and exposed through the `Squirix` meter as runtime cache-operation
admission diagnostics. `BackpressureCacheDecorator<T>` applies this policy before logical reads and writes enter memory
admission, clustered routing, journal append, memory mutation, memory accounting, or idempotency outcome updates.
REST/gRPC adapters keep transport-specific protection and map runtime backpressure failures to HTTP 429 or gRPC
`ResourceExhausted`; they do not own duplicate logical cache-operation backpressure. Keep these signals separate from
memory-pressure metrics: backpressure describes request concurrency, queueing, slowdown, and rate-limit pressure, while
memory pressure describes estimated cache working-set capacity.

Runtime validation is owned by `ValidationCacheDecorator<T>` in the hosted cache pipeline. **Cache names** from clients
follow the same boundary: invalid names fail before memory admission, generic operation metrics, journal append, memory
accounting, and local mutation. `LocalCache<T>` may still keep defensive invariants for direct construction,
recovery/trusted replay, and internal data-structure correctness, but it is not the hosted validation policy owner.

<!-- markdownlint-disable-next-line MD033 -->
<a id="metrics-route"></a>

## Metrics route

When enabled (default), the host exposes a Prometheus-compatible text scrape endpoint on the **primary HTTPS listener**:

- `GET /metrics` (default path; configurable)

The scrape surface is a lightweight exporter over the `Squirix` .NET meter. Disable it or change the path through
`PrometheusMetrics` in `Squirix.settings.json`. See
[configuration](configuration.md#prometheus-metrics-squirixsettingsjson).

Access control is enforced on every request:

- **Loopback clients** (`127.0.0.1`, `::1`) may scrape without credentials. This is a deliberate tradeoff for same-host
  Prometheus and local development: **loopback is treated as trusted**. Any process on the host can reach loopback.
- **All other clients** must authenticate with a JWT bearer token. There is no settings flag to change loopback access.

<!-- markdownlint-disable-next-line MD033 -->
<a id="metrics-loopback-trust"></a>

### Loopback trust and multi-tenant hosts

**Risk (low / design tradeoff):** on shared or multi-tenant machines, co-located processes can scrape `/metrics`
anonymously over loopback and learn operational state (throughput, memory pressure, journal backlog, and similar).

**Assumption:** production nodes that expose `/metrics` on loopback expect a **single-tenant** host or a controlled
environment where local processes are trusted.

**Mitigations when that assumption does not hold:**

- Disable the HTTP scrape endpoint (`PrometheusMetrics.enabled: false`) and use OpenTelemetry or `MeterListener`
  exporters with your platform's auth model.
- Keep the primary listener on loopback only when the node must not accept remote clients (see
  [server-mode.md](server-mode.md#loopback-development-default-not-production-posture)).
- Run one squirix node per dedicated VM or container with network policies that limit who can reach the listener.

Implementation: `SquirixMetricsConnectionSecurity` in `src/squirix.server/Node/Observability/Metrics/`.

Remote scrapers should use the same JWT as cache routes. Example header: `Authorization: Bearer <token>`. See
[configuration — Prometheus metrics](configuration.md#prometheus-metrics-squirixsettingsjson)
for a `prometheus.yml` fragment.

<!-- markdownlint-disable-next-line MD033 -->
<a id="scrape-privacy-model"></a>

### Scrape privacy model

HTTP `/metrics` always exports the **public scrape profile**:

- **Stripped labels:** `cache`, `exception_type` (aggregated away before export).
- **Retained labels:** bounded operational dimensions such as `operation`, `result`, `node`, `state`, `op`, `impl`.
- **Not configurable:** there is no settings flag to export identifying labels over HTTP.

Full-fidelity series (including `cache` and `exception_type`) remain on the `Squirix` .NET meter for OpenTelemetry and
other `MeterListener` exporters.

## Security

- `/health`, `/health/live`, and `/health/ready` stay anonymous for probes.
- `/health/ready/details` and `/metrics` are served on the primary HTTPS listener only.
- Loopback `/metrics` and `/health/ready/details` scrapes stay anonymous by design (loopback is trusted); remote
  clients must present a JWT bearer token when server auth is enabled. See
  [Metrics route — Loopback trust](#metrics-loopback-trust) for multi-tenant risk and mitigations.
- Traces and additional metrics are also available through .NET observability primitives (`ActivitySource`, `Meter`)
  independent of the HTTP scrape route.

See also:

- [configuration.md](configuration.md)
- [configuration.md#memory-pressure-squirixsettingsjson](configuration.md#memory-pressure-squirixsettingsjson)
- [operational-runbook.md](operational-runbook.md)
- [storage-maintenance.md](storage-maintenance.md)
