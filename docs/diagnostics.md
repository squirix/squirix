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

- `journalBacklogOps`: journal operations not covered by the latest snapshot watermark
- `snapshotAgeSeconds`: age of the latest snapshot, or `null` if no snapshot exists
- `snapshotInFlight`: whether a snapshot is currently running
- `compaction.state`: current journal compaction service state
- `compaction.lastRunUtc`
- `compaction.inFlight`
- `clientPool.configured`
- `clientPool.peers`
- `coordination.leases`
- `coordination.watches`
- `memoryPressure.state`: coarse pressure derived from configured limits and **decorator-maintained** approximate
  accounting — `normal`, `high`, or `critical` (see [configuration.md#memory-pressure-squirixsettingsjson](configuration.md#memory-pressure-squirixsettingsjson)).
  `LocalCache<T>` does not own this policy.
- `memoryPressure.maxEstimatedCacheBytes`: configured estimated byte limit, or `null` when no limit is configured
- `memoryPressure.estimatedCacheBytes`: current global approximate accounted bytes for the node
- `memoryPressure.entryCount`: current global approximate accounted live entry count
- `memoryPressure.rejectedWriteCount`: number of memory admission rejections recorded since process start for this
  accounting instance
- `memoryPressure.writeRejectionActive`: whether the policy **would** reject memory-growing writes at critical pressure
  (`Enabled`, positive `MaxEstimatedCacheBytes`, and `RejectWritesOnCriticalPressure`)

Readiness behavior (`GET /health/ready`):

- The route is the machine readiness probe for schedulers and load balancers.
- `journal_recovery` is **Unhealthy** until journal startup recovery opens the gate.
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

These instruments describe logical cache operations only and use bounded `operation` / `result` labels; they do not
include raw keys, values, serialized payloads, or unbounded cache names.

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

When enabled (default), the host exposes a Prometheus-compatible text scrape endpoint:

- `GET /metrics` (default path; configurable)

The scrape surface is a lightweight exporter over the `Squirix` .NET meter. Disable it or change the path through
`PrometheusMetrics` in `Squirix.settings.json`. See
[configuration](configuration.md#prometheus-metrics-squirixsettingsjson).

Access control is enforced on every request:

- **Loopback clients** (`127.0.0.1`, `::1`) may scrape without credentials (typical same-host Prometheus).
- **All other clients** must authenticate with `X-Api-Key` or a JWT bearer token. There is no settings flag to disable
  this rule.

Remote scrapers should use the same credentials as cache/admin routes. Example `Authorization` header:
`X-Api-Key: your-api-key`. See [configuration — Prometheus metrics](configuration.md#prometheus-metrics-squirixsettingsjson)
for a `prometheus.yml` fragment.

<!-- markdownlint-disable-next-line MD033 -->
<a id="admin-routes-v01"></a>

## Admin routes (v0.1)

- `GET /admin/whoami`
- `GET /admin/owner/{key}`
- `GET /admin/ring`

## Ring diagnostics

`GET /admin/ring` returns:

- `virtualNodes`
- `members`
- `sampleSize`
- `vnodeDistribution`
- `ownerLookupSamples`

Use `ownerLookupSamples` for quick sanity checks and `/admin/owner/{key}` for a specific key lookup.

Important:

- This is the node's local ring view.
- Dynamic topology changes, membership mutation, rebalance history, manual compaction triggers, and deep storage
  diagnostics HTTP routes are not part of the v0.1 admin surface documented here.

## Security

- Admin routes use the same route-level authorization policy as the rest of `/admin`.
- When API keys are configured, callers must provide a valid `X-Api-Key` header or equivalent configured auth.
- `/metrics` is enabled by default and is plaintext unless the host uses HTTPS. Loopback scrapes stay anonymous; remote
  clients must present `X-Api-Key` or a JWT bearer token (see [Metrics route](#metrics-route)).
- Traces and additional metrics are also available through .NET observability primitives (`ActivitySource`, `Meter`)
  independent of the HTTP scrape route.

See also:

- [configuration.md](configuration.md)
- [configuration.md#memory-pressure-squirixsettingsjson](configuration.md#memory-pressure-squirixsettingsjson)
- [operational-runbook.md](operational-runbook.md)
- [storage-maintenance.md](storage-maintenance.md)
