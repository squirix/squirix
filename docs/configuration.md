# Configuration

squirix validates node options on startup. Invalid values fail fast with `OptionsValidationException` before the node
starts serving traffic.

## File discovery

The bootstrap file name used by the current container/bootstrap flow is `Squirix.settings.json` or
`squirix.settings.json`.

Search order:

- Current working directory
- `AppContext.BaseDirectory`

In Docker, mount settings read-only (for example `docker/node-a/Squirix.settings.json` → `/app/Squirix.settings.json`).
See [containerization.md](containerization.md) for dev and release image layouts.

The standalone `squirix-server` host, `await builder.AddSquirixServerAsync(...)`, and `SquirixServer.StartAsync()` load
`Squirix:Cluster` through `Configurator` when a settings file is discovered or supplied. `StartAsync()`
then hosts the node through the same `AddSquirixServerAsync` / `MapSquirixServer` pipeline as the standalone executable.
Other sections such as `MemoryPressure`, `Snapshot`, and `PrometheusMetrics` are still merged from the same settings file
at runtime when present. Custom ASP.NET Core hosts configure cluster topology and optional persistence through
`SquirixServerOptions` (`UsePersistence()`); `app.MapSquirixServer()` maps gRPC, health, and metrics endpoints.

## Remote client (`SquirixClientOptions`)

Configure the v0.1 client when calling `SquirixClient.ConnectAsync`:

| Member                | Purpose                                                                                                           |
| --------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `Endpoints`           | Bootstrap server URLs (HA front door, not shards). See [bootstrap client failover](bootstrap-client-failover.md). |
| `BearerTokenProvider` | Supplies a JWT bearer token for each gRPC call when the server requires authentication.                           |
| `Serializer`          | Per-session `ISquirixSerializer`; null uses default JSON for that client. See [serialization](serialization.md).  |

For local HTTPS development, trust the ASP.NET Core development certificate with
`dotnet dev-certs https --trust`.

Example:

```csharp
using System;
using System.Threading.Tasks;
using Squirix.Client;

await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add(new Uri("https://cache-a.example.internal:5001"));
        options.Endpoints.Add(new Uri("https://cache-b.example.internal:5002"));
        options.BearerTokenProvider = _ => new ValueTask<string>(Environment.GetEnvironmentVariable("SQUIRIX_CLIENT_JWT")!);
    },
    cancellationToken);
```

Client authentication uses `BearerTokenProvider` when the server requires JWT bearer authentication.

<!-- markdownlint-disable-next-line MD033 -->
<a id="memory-pressure-squirixsettingsjson"></a>

## Memory pressure (`Squirix.settings.json`)

The optional `Squirix:MemoryPressure` section is merged when present (same file discovery as `Squirix:Cluster`).
Environment variables listed below override merged file values. Memory pressure is **always active** at runtime.
The node may reject **growing** writes under critical estimated memory usage. Those rejections occur before durable
journal append. gRPC returns **`ResourceExhausted`** with stable pressure details (bounded payloads; field semantics are
in the table below).

| Field                              | Type  | Default                        | Validation                                                                                          |
| ---------------------------------- | ----- | ------------------------------ | --------------------------------------------------------------------------------------------------- |
| `MaxEstimatedCacheBytes`           | long? | `80%` of available process RAM | unset uses the RAM default; when set must be `> 0` and `<= 80%` of available process RAM at startup |
| `HighPressureThresholdPercent`     | int   | `80`                           | `(0, 100]`                                                                                          |
| `CriticalPressureThresholdPercent` | int   | `95`                           | `(0, 100]`, must be `>` `HighPressureThresholdPercent`                                              |

Available process RAM is read from `GC.GetGCMemoryInfo().TotalAvailableMemoryBytes` at startup (in containers this is
usually the pod memory limit). Legacy JSON fields such as `enabled` and `rejectWritesOnCriticalPressure` are ignored.

Example fragment:

```json
{
    "Squirix": {
        "MemoryPressure": {
            "maxEstimatedCacheBytes": 1073741824,
            "highPressureThresholdPercent": 80,
            "criticalPressureThresholdPercent": 95
        }
    }
}
```

## Cluster settings

`Squirix:Cluster` is loaded by `Configurator` (`TryLoadFromFileAsync`, `LoadFromFileAsync`) for the
standalone host, `AddSquirixServerAsync(...)`, and `SquirixServer.StartAsync()`.

| Field            | Type   | Default                                | Validation                                                                                                                           |
| ---------------- | ------ | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `NodeId`         | string | loader fallback                        | Required, non-empty, maximum 128 characters                                                                                          |
| `ClusterId`      | string | loader fallback                        | Required, non-empty, maximum 128 characters                                                                                          |
| `Uri`            | URI    | loader fallback                        | Absolute `https` origin URI (max 2048); rejects `http://`; no credentials, path, query, or fragment                                  |
| `VirtualNodes`   | int    | `128`                                  | `> 0` and `<= 16384`                                                                                                                 |
| `Peers`          | array  | runtime local-peer fallback when empty | When non-empty: must include local `NodeId`; peer ids and URIs must be unique; local peer `Uri` must match `Uri`; maximum 1024 peers |
| `Peers[].NodeId` | string | none                                   | Required, non-empty, maximum 128 characters                                                                                          |
| `Peers[].Uri`    | URI    | none                                   | Same validation as `Uri`                                                                                                             |

CLI validation:

- `squirix-server validate-config --settings PATH` validates `Squirix:Cluster` only.
- `squirix-server validate-config --settings PATH --strict` also validates optional `MemoryPressure`, `Snapshot`, and
  `PrometheusMetrics` sections when they are present. Host startup always resolves memory pressure (80% RAM default when
  `MaxEstimatedCacheBytes` is unset) even when the JSON section is absent; `--strict` only checks the section if it
  exists in the file.

Example:

```json
{
    "Squirix": {
        "Cluster": {
            "ClusterId": "dev-cluster",
            "NodeId": "node-a",
            "Uri": "https://localhost:5001",
            "VirtualNodes": 128,
            "Peers": [
                { "NodeId": "node-a", "Uri": "https://localhost:5001" },
                { "NodeId": "node-b", "Uri": "https://localhost:5002" }
            ]
        }
    }
}
```

For local standalone hosts, `https://localhost:5001` is the default gRPC listen URL. In Docker Compose and other container
networks, set `Uri` and the local peer entry to the **service hostname** reachable by other nodes (for example
`https://squirix-node-a:5000`), not `https://0.0.0.0:5000`. The local peer `Uri` must exactly match `Cluster.Uri`.

When exposing a container to host client apps: map the primary HTTPS listener (for example host **5001** → container **5000**)
so gRPC clients and operational routes (`/health`, `/metrics`) share one TLS port. See [containerization.md](containerization.md).

## Hosting options (`SquirixServerOptions`)

Configure these through `await builder.AddSquirixServerAsync(...)`, `SquirixServer.StartAsync(...)`, or the `Squirix:Cluster`
section in settings (mapped into the same options model).

| Field                       | Type   | Default | Validation                                                                 |
| --------------------------- | ------ | ------- | -------------------------------------------------------------------------- |
| `PersistenceEnabled`        | bool   | `false` | Any boolean                                                                |
| `WaitForRecovery`           | bool   | `true`  | Any boolean; applies when persistence is enabled                           |
| `DataDirectory`             | string | `null`  | Optional path when persistence is enabled; requires `UsePersistence()`     |

Call `options.UsePersistence()` (or `options.UsePersistence("./data")`) to enable journal/snapshot persistence. The standalone
host accepts `--persist`; `--data-dir` requires `--persist`.

Example:

```csharp
await builder.AddSquirixServerAsync(options =>
{
    options.NodeId = "node-a";
    options.Uri = new Uri("https://localhost:5001");
    options.UsePersistence("./data");
});
```

### Recovery startup (`WaitForRecovery`)

When persistence is enabled and `WaitForRecovery` is `true` (default), the node blocks serving until hosted journal
replay completes.

When `WaitForRecovery` is `false`, replay runs in the background:

- journal mutations wait on the startup gate (unchanged).
- Cache reads wait on the same gate until replay completes.
- `/health/ready` stays **Unhealthy** until the gate opens (`journal_recovery` check).
- `/health/ready` also reports **Unhealthy** for fatal durability maintenance failures (`journal_maintenance`),
  including journal periodic flush-loop failure, failed journal compaction state, or fatal snapshot trigger failure.
- `/health/live` remains available for process liveness.

Use non-blocking recovery only when load balancers honor `/health/ready` and callers tolerate delayed read availability
during startup.

## Node settings file (`Squirix.settings.json`)

Optional sections below are **not** properties on `SquirixServerOptions`. In v0.1 public hosting, only some of them are
merged from the settings file at startup:

| Section | Loaded from `Squirix.settings.json`? | Notes |
| --- | --- | --- |
| `MemoryPressure` | Yes | Merged when present |
| `Snapshot` | Yes | Merged when present |
| `PrometheusMetrics` | Yes | Merged when present |
| Persistence knobs (`PersistenceOptions`) | No | Host defaults when `--persist` / `UsePersistence()`; not a JSON section today |
| Backpressure (`AdmissionOptions`) | No | Host defaults; not a JSON section today |
| Idempotency store | Env only | `SQUIRIX_IDEMPOTENCY_*` overrides; not a JSON section |
| Journal compaction / metrics exporter interval | No | Hardcoded in host composition |

`squirix-server validate-config --strict` validates optional `MemoryPressure`, `Snapshot`, and `PrometheusMetrics`
sections together with cluster settings.

### Persistence (host defaults)

When persistence is enabled (`UsePersistence()` / `--persist`), the node uses internal `PersistenceOptions` defaults.
There is **no** `Squirix:Persistence` JSON merge in v0.1 public hosting — putting a Persistence object in
`Squirix.settings.json` has no effect. Data directory comes from `SquirixServerOptions.DataDirectory` / `--data-dir`
(otherwise the host resolves a per-node default under local app data).

| Field                         | Type   | Default in node host                                       | Validation                                                                                                                                 |
| ----------------------------- | ------ | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `DataDir`                     | string | `%LocalAppData%/squirix/<cluster>/<node>` or temp fallback | Required, non-empty when persistence is enabled                                                                                            |
| `JournalMaxSegmentMb`         | int    | `64`                                                       | `> 0`                                                                                                                                      |
| `FlushIntervalMs`             | int    | `10`                                                       | `> 0`                                                                                                                                      |
| `ManifestRetentionCount`      | int    | `3`                                                        | `> 0`                                                                                                                                      |
| `SnapshotRetentionCount`      | int    | `3`                                                        | `> 0`                                                                                                                                      |
| `JournalGroupCommitMaxWait`   | ms     | `0` (disabled)                                             | `>= 0`; internal JSON name `groupCommitMaxWait` (tests / explicit `PersistenceOptions` only)                                               |
| `JournalGroupCommitMaxBatch`  | int    | `32`                                                       | `> 0`; used only when group commit is enabled                                                                                              |
| `JournalPlatformBackend`      | string | `Auto`                                                     | `Auto`, `RandomAccess`, or `Uring` (Linux only)                                                                                            |
| `JournalMaxSegmentCount`      | int    | `32`                                                       | `> 0` (Pipelined journal segment count cap)                                                                                                |
| `JournalMaxTotalBytesMb`      | int    | `2048`                                                     | `> 0` (Pipelined journal total on-disk size hard cap)                                                                                      |

`JournalMaxTotalBytesMb` soft high-water for `/health/ready/details` is fixed at 80% of this limit. Durable writes
that would exceed the hard cap are rejected with `JOURNAL_DISK_QUOTA` (gRPC `ResourceExhausted`); readiness
stays healthy. See [Journal disk quota](operational-runbook.md#journal-disk-quota) for operator guidance.

See [journal group commit](journal-group-commit.md) for defaults, when to enable, and tuning guidance.

### Snapshot

Optional `Squirix:Snapshot` object merged when present. `TriggerOptions` has no `Enabled` flag — snapshot triggering
uses the fields below (omit the section to keep host defaults).

| Field                        | Type            | Default in node host | Validation     |
| ---------------------------- | --------------- | -------------------- | -------------- |
| `SnapshotInterval`           | TimeSpan string | `00:05:00`           | `> 0`          |
| `SnapshotEveryNOps`          | long            | `250000`             | `>= 0`         |
| `SnapshotEveryNBytes`        | long            | `134217728`          | `>= 0`         |
| `MinGapBetweenSnapshots`     | TimeSpan string | `00:01:00`           | `>= 0`         |
| `JournalGrowthThrottleBytes` | long            | `0`                  | `>= 0`         |
| `LatencySloMilliseconds`     | double          | `0`                  | finite, `>= 0` |
| `LatencyThrottleDuration`    | TimeSpan string | `00:00:10`           | `>= 0`         |

### Backpressure

Backpressure uses internal `AdmissionOptions` host defaults. There is **no** Backpressure JSON section merge in v0.1
public hosting. Limits apply before logical reads and writes under load. gRPC transport adapters still enforce
transport-level limits (auth, payload size, deadlines, cancellation). Memory pressure is a separate policy.

Per-client limits (`PerClientMaxInFlight`, `PerClientMaxQueue`, `PerClientRateLimit*`) key off a **backpressure client
id** resolved for each cache operation:

| Source | Client id | When |
| ------ | --------- | ---- |
| JWT bearer principal | `jwt:{subject}` | Authenticated request with a non-empty `sub` / `NameIdentifier` claim |
| ASP.NET Core connection | `conn:{connectionId}` | Request has an `HttpContext` but no usable principal id (anonymous loopback, internal owner RPCs without JWT, authenticated token missing `sub`) |
| In-process / missing context | `runtime` | No `HttpContext` (host bootstrap, some tests, non-HTTP callers). All such callers share one bucket |

v0.1 external auth is JWT-only; there is no API-key principal. Inter-node cluster forwarding uses mTLS on the internal
listener and typically lands in the `conn:` or `runtime` bucket rather than a shared external JWT subject.

| Field                         | Type            | Default        | Validation                                    |
| ----------------------------- | --------------- | -------------- | --------------------------------------------- |
| `Enabled`                     | bool            | `true`         | Any boolean                                   |
| `MaxInFlight`                 | int             | `256`          | `> 0`                                         |
| `PerClientMaxInFlight`        | int?            | `null`         | unset or `1..MaxInFlight`                     |
| `MaxQueue`                    | int             | `128`          | `>= 0`                                        |
| `PerClientMaxQueue`           | int?            | `null`         | unset or `>= 0`                               |
| `SlowdownThreshold`           | int             | `192`          | `1..MaxInFlight`                              |
| `RejectThreshold`             | int             | `256`          | `1..MaxInFlight`, `>= SlowdownThreshold`      |
| `NodeRateLimitPerSecond`      | int?            | `null`         | unset or `> 0` with `NodeRateLimitBurst`      |
| `NodeRateLimitBurst`          | int?            | `null`         | unset or `>= NodeRateLimitPerSecond`          |
| `PerClientRateLimitPerSecond` | int?            | `null`         | unset or `> 0` with `PerClientRateLimitBurst` |
| `PerClientRateLimitBurst`     | int?            | `null`         | unset or `>= PerClientRateLimitPerSecond`     |
| `MaxSlowdownDelay`            | TimeSpan string | `00:00:00.025` | `>= 0`                                        |
| `MaxQueueWait`                | TimeSpan string | `00:00:00.250` | `> 0`                                         |

### Journal compaction

Host composition hardcodes these values when persistence is enabled (not loaded from `Squirix.settings.json`):

| Field             | Type            | Default in node host | Validation  |
| ----------------- | --------------- | -------------------- | ----------- |
| `Enabled`         | bool            | `true`               | Any boolean |
| `MinTailSegments` | int             | `2`                  | `>= 0`      |
| `MinTailBytes`    | long            | `67108864`           | `>= 0`      |
| `MinGap`          | TimeSpan string | `00:02:00`           | `>= 0`      |

### Journal metrics exporter

Host composition hardcodes the export interval when persistence is enabled (not loaded from `Squirix.settings.json`):

| Field      | Type            | Default in node host | Validation |
| ---------- | --------------- | -------------------- | ---------- |
| `Interval` | TimeSpan string | `00:00:05`           | `> 0`      |

<!-- markdownlint-disable-next-line MD033 -->
<a id="prometheus-metrics-squirixsettingsjson"></a>

### Prometheus metrics (`PrometheusMetrics`)

The optional `PrometheusMetrics` section configures the built-in Prometheus-compatible HTTP scrape endpoint mapped by
`MapSquirixServer()`.

| Field     | Type   | Default in node host | Validation                                              |
| --------- | ------ | -------------------- | ------------------------------------------------------- |
| `Enabled` | bool   | `true`               | Any boolean                                             |
| `Path`    | string | `/metrics`           | Non-empty, must start with `/` when `Enabled` is `true` |

Example fragment:

```json
{
    "Squirix": {
        "PrometheusMetrics": {
            "enabled": true,
            "path": "/metrics"
        }
    }
}
```

Access control is not configurable: loopback clients may scrape anonymously; all other clients must authenticate with
the same JWT bearer token used for cache routes (see
[diagnostics — Metrics route](diagnostics.md#metrics-route)).

**Loopback trust assumption:** anonymous loopback scrapes assume the host is **single-tenant** and that any local
process reaching `127.0.0.1` / `::1` is trusted. On **shared or multi-tenant** hosts, another tenant's process can
scrape `/metrics` without JWT and read operational data. Mitigations: bind the primary listener to loopback only when
appropriate, disable the HTTP scrape (`PrometheusMetrics.enabled: false`) and export through OpenTelemetry /
`MeterListener` instead, or run nodes on dedicated hosts. There is no settings flag to require JWT for loopback
scrapes — the tradeoff is intentional for same-host Prometheus ergonomics.

Privacy is not configurable either: HTTP `/metrics` always uses the public scrape profile (`cache` and `exception_type`
labels are stripped before export). See [diagnostics — Scrape privacy model](diagnostics.md#scrape-privacy-model).

Remote Prometheus example (`prometheus.yml`):

```yaml
scrape_configs:
  - job_name: squirix
    scheme: https
    tls_config:
      insecure_skip_verify: true   # use proper CA trust in production
    authorization:
      type: Bearer
      credentials: your-jwt-bearer-token
    static_configs:
      - targets: ["node.example:5001"]
    metrics_path: /metrics
```

See [diagnostics](diagnostics.md#metrics-route) for scrape semantics and security notes.

## In-process test hosts

Production and standalone `squirix-server` processes configure JWT through environment variables (see below).
In-process test hosts (`TestNodeHost`, `TestNodeHostFactory`) also accept an optional **per-node security override**
so parallel tests do not share process-wide environment state.

Use `TestNodeSecurityOptions` from `Squirix.Server.TestKit` when starting a node in tests. When provided, the override
replaces environment-variable lookup for that startup only; omit it on `NodeIntegrationTestBase.StartNodeAsync` to keep
env-based behavior, or rely on the smoke-test default (empty override, unauthenticated node).

```csharp
// E2E / integration auth (JWT)
var credentials = TestJwtHelper.CreateRandomCredentials();
await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

// Smoke default: unauthenticated without touching process env
await StartNodeAsync(url, peers);
```

Symmetric JWT-protected nodes use `JwtSigningKey`, `JwtIssuer`, and `JwtAudience`. OIDC authority URLs use
`JwtAuthority` with a required `JwtAudience`, optional `JwtIssuer`, and `JwtAllowHttpMetadata` (set `true` for `http://`
mock authorities in tests). Startup fails when `SQUIRIX_JWT_AUTHORITY` is set without `SQUIRIX_JWT_AUDIENCE`, including
on loopback listeners.

```csharp
// OIDC authority JWT (integration / smoke)
await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
await StartNodeAsync(url, peers, security: authority.ToSecurityOptions("squirix-test"));
var token = authority.CreateBearerToken("squirix-test");
```

`MockOidcAuthority` lives in `Squirix.Server.TestKit.Security` and serves discovery metadata plus JWKS on loopback
without external network access. E2E tests run with xUnit parallelization enabled; auth scenarios must use explicit
`TestNodeSecurityOptions` overrides rather than process environment variables.

## Environment variables

Deployment, Docker, and standalone hosts load security settings from the process environment. These variables map to
the same auth pipeline used by in-process overrides above. Docker images also set
`ASPNETCORE_Kestrel__Certificates__Default__Path` for the
bundled development PFX; see [containerization.md](containerization.md#https-in-containers).

| Variable                                             | Purpose                                                                                                                                                                                                            |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SQUIRIX_JWT_AUTHORITY`                              | JWT authority for bearer authentication. Requires `SQUIRIX_JWT_AUDIENCE`.                                                                                                                                          |
| `SQUIRIX_JWT_AUDIENCE`                               | JWT audience validation value. Required when `SQUIRIX_JWT_AUTHORITY` is set.                                                                                                                                       |
| `SQUIRIX_JWT_ISSUER`                                 | JWT issuer. Required when using `SQUIRIX_JWT_SIGNING_KEY` without authority.                                                                                                                                       |
| `SQUIRIX_JWT_SIGNING_KEY`                            | Symmetric JWT signing key, raw text or base64.                                                                                                                                                                     |
| `SQUIRIX_JWT_ALLOW_HTTP_METADATA`                    | Allows non-HTTPS authority metadata for JWT in dev/test.                                                                                                                                                           |
| `SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT`                 | Dedicated cluster/internal HTTPS listener port for inter-node gRPC mTLS. Required when remote cluster peers are configured and must differ from the primary `Cluster.Uri` port.                                    |
| `SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH`                 | PKCS#12/PFX path for the local node certificate. Certificate CN must equal `Cluster.NodeId`. Mutually exclusive with PEM cert/key paths.                                                                           |
| `SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD`             | Optional password for `SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH`.                                                                                                                                                        |
| `SQUIRIX_CLUSTER_MTLS_CERT_PATH`                     | PEM-encoded node certificate path. Certificate CN must equal `Cluster.NodeId`. Requires `SQUIRIX_CLUSTER_MTLS_KEY_PATH`.                                                                                           |
| `SQUIRIX_CLUSTER_MTLS_KEY_PATH`                      | PEM-encoded node private key path.                                                                                                                                                                                 |
| `SQUIRIX_CLUSTER_MTLS_CA_PATH`                       | PEM-encoded cluster CA / trust root. Required when remote cluster peers are configured.                                                                                                                            |
| `SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES`  | Overrides `MemoryPressure.MaxEstimatedCacheBytes` (must be positive and within the 80% RAM cap at startup).                                                                                                        |
| `SQUIRIX_MEMORY_PRESSURE_HIGH_THRESHOLD_PERCENT`     | Overrides `MemoryPressure.HighPressureThresholdPercent`.                                                                                                                                                           |
| `SQUIRIX_MEMORY_PRESSURE_CRITICAL_THRESHOLD_PERCENT` | Overrides `MemoryPressure.CriticalPressureThresholdPercent`.                                                                                                                                                       |
| `SQUIRIX_IDEMPOTENCY_MAX_IN_FLIGHT_RECORDS`          | Caps in-memory mutation idempotency replay records (default `65536`).                                                                                                                                              |
| `SQUIRIX_IDEMPOTENCY_RETENTION_MINUTES`              | How long successful mutation outcomes remain replayable (default `15`).                                                                                                                                            |
| `SQUIRIX_IDEMPOTENCY_SWEEP_INTERVAL_SECONDS`         | Background sweep interval for expired idempotency records (default `60`).                                                                                                                                          |
| `SQUIRIX_TEST_ROOT`                                  | Test-only root for generated node data directories.                                                                                                                                                                |

## Security notes

### Loopback trust model

When the primary listen URL host is loopback (`localhost`, `127.0.0.1`, or another `IPAddress.IsLoopback` address),
Squirix allows **unauthenticated** access to gRPC cache routes unless you configure `SQUIRIX_JWT_*`. `/metrics` scrapes
from loopback clients stay anonymous even when JWT is enabled; remote clients still need a bearer token (see
[diagnostics.md](diagnostics.md#metrics-route)).

This trusts **every local process on the machine**, not just your application. It is appropriate for local development,
benchmarks, and in-process tests. It is **not** a substitute for JWT/OIDC on shared hosts, containers published to the
host network, or any interface reachable by other machines.

Implementation: `SquirixExternalAccessSecurity.EnsureDataPlaneAuthenticatedForListenUri` skips the auth requirement only
for loopback bind hosts. Non-loopback URLs (`0.0.0.0`, Docker DNS names, public interfaces) **require** JWT settings at
startup; the process refuses to start without them.

### External authentication

- Non-loopback listen URLs **require** JWT settings at startup as described above. When auth is configured, loopback
  gRPC and remote clients must present valid JWT bearer tokens for cache routes (missing or invalid credentials are
  rejected).
- Operational routes (`/health`, `/metrics`) are served on the **primary HTTPS listener** only.
- When remote cluster peers are configured (`Peers[]` contains at least one node other than the local `NodeId`),
  inter-node mTLS is required at startup. Inter-node gRPC is served on the dedicated internal HTTPS listener
  (`SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT`) with required peer client certificates. Each node certificate CN must match
  its `Cluster.NodeId`; peer certificates are accepted only when they chain to the cluster CA and their CN matches the
  expected peer `NodeId`. Outbound `ClientPool` calls attach the local node certificate and apply the same trust and
  identity checks to peer server certificates. Standalone nodes without remote peers do not require cluster mTLS
  material. The primary listener keeps external client behavior unchanged.
- Deployment, rotation, and dev certificate generation for **inter-node mTLS** are documented in
  [security/inter-node-mtls.md](security/inter-node-mtls.md). Squirix consumes externally managed cluster certificates;
  it does not act as a production CA. Inter-node trust requires the PEM cluster CA at
  `SQUIRIX_CLUSTER_MTLS_CA_PATH` and certificate CN equal to the expected cluster `NodeId`.
- **External JWT** signing, blast radius, and rotation (symmetric vs OIDC) are documented in
  [security/jwt-signing-keys.md](security/jwt-signing-keys.md). Inter-node forwarding does not use JWT when mTLS is
  enforced.

## Sample `appsettings.json`

```json
{
    "Squirix": {
        "Cluster": {
            "ClusterId": "prod-cache",
            "NodeId": "cache-a",
            "Uri": "https://cache-a.example.internal:5001",
            "VirtualNodes": 256,
            "Peers": [
                { "NodeId": "cache-a", "Uri": "https://cache-a.example.internal:5001" },
                { "NodeId": "cache-b", "Uri": "https://cache-b.example.internal:5002" },
                { "NodeId": "cache-c", "Uri": "https://cache-c.example.internal:5003" }
            ]
        }
    }
}
```

## Validation failures

Typical examples:

- `Backpressure RejectThreshold must be greater than or equal to SlowdownThreshold.`
- `Backpressure PerClientMaxInFlight cannot exceed MaxInFlight.`
- `Backpressure NodeRateLimitBurst must be greater than zero when configured.`
- `Persistence DataDir is required.`
- `Persistence JournalMaxSegmentMb must be greater than zero.`
- `MemoryPressure HighPressureThresholdPercent must be less than CriticalPressureThresholdPercent.`
- `MemoryPressure MaxEstimatedCacheBytes must be positive when set.`
- `MemoryPressure MaxEstimatedCacheBytes ({configured}) exceeds the 80% RAM cap ({cap}).`
- `MemoryPressure cannot resolve RAM budget: available process memory is zero.`
