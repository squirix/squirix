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

The standalone `squirix-server` host, `builder.AddSquirixServer(...)`, and `SquirixServer.StartAsync()` load
`Squirix:Cluster` through `SquirixServerConfiguration` when a settings file is discovered or supplied. `StartAsync()`
then hosts the node through the same `AddSquirixServer` / `MapSquirixServer` pipeline as the standalone executable.
Other sections such as `MemoryPressure` and `PrometheusMetrics` are still merged from the same settings file at runtime
when present. Custom ASP.NET Core hosts configure cluster topology and persistence directory through
`SquirixServerOptions`; `app.MapSquirixServer()` maps gRPC, health, and metrics endpoints.

## Remote client (`SquirixOptions`)

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
await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add("https://cache-a.example.internal:5001");
        options.Endpoints.Add("https://cache-b.example.internal:5002");
        options.BearerTokenProvider = _ => new ValueTask<string>(Environment.GetEnvironmentVariable("SQUIRIX_JWT")!);
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
journal append. REST returns HTTP **429** with code **`MEMORY_PRESSURE`**; gRPC returns **`ResourceExhausted`** (bounded
payloads; field semantics are in the table below).

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

`Squirix:Cluster` is loaded by `SquirixServerConfiguration` for the standalone host, `AddSquirixServer(...)`, and
`SquirixServer.StartAsync()`.

| Field            | Type   | Default                                | Validation                                                                                                                           |
| ---------------- | ------ | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `NodeId`         | string | loader fallback                        | Required, non-empty, maximum 128 characters                                                                                          |
| `ClusterId`      | string | loader fallback                        | Required, non-empty, maximum 128 characters                                                                                          |
| `Url`            | URI    | loader fallback                        | Absolute `https` origin URI (max 2048); rejects `http://`; no credentials, path, query, or fragment                                  |
| `VirtualNodes`   | int    | `128`                                  | `> 0` and `<= 16384`                                                                                                                 |
| `Peers`          | array  | runtime local-peer fallback when empty | When non-empty: must include local `NodeId`; peer ids and URLs must be unique; local peer `Url` must match `Url`; maximum 1024 peers |
| `Peers[].NodeId` | string | none                                   | Required, non-empty, maximum 128 characters                                                                                          |
| `Peers[].Url`    | URI    | none                                   | Same validation as `Url`                                                                                                             |

CLI validation:

- `squirix-server validate-config --settings PATH` validates `Squirix:Cluster` only.
- `squirix-server validate-config --settings PATH --strict` also validates optional `MemoryPressure` and
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
            "Url": "https://localhost:5001",
            "VirtualNodes": 128,
            "Peers": [
                { "NodeId": "node-a", "Url": "https://localhost:5001" },
                { "NodeId": "node-b", "Url": "https://localhost:5002" }
            ]
        }
    }
}
```

For local standalone hosts, `https://localhost:5001` is the default gRPC listen URL. In Docker Compose and other container
networks, set `Url` and the local peer entry to the **service hostname** reachable by other nodes (for example
`https://squirix-node-a:5000`), not `https://0.0.0.0:5000`. The local peer `Url` must exactly match `Cluster.Url`.

When exposing a container to host client apps: map the primary HTTPS listener (for example host **5001** → container **5000**)
so gRPC clients and operational routes (`/health`, `/metrics`) share one TLS port. See [containerization.md](containerization.md).

## Hosting options (`SquirixServerOptions`)

Configure these through `builder.AddSquirixServer(...)`, `SquirixServer.StartAsync(...)`, or the `Squirix:Cluster`
section in settings (mapped into the same options model).

| Field                       | Type   | Default                        | Validation                                                                 |
| --------------------------- | ------ | ------------------------------ | -------------------------------------------------------------------------- |
| `WaitForRecovery`           | bool   | `true`                         | Any boolean                                                                |
| `DataDirectory`             | string | `null` (platform default path) | Optional; non-empty when set                                               |

### Recovery startup (`WaitForRecovery`)

When `WaitForRecovery` is `true` (default), the node blocks serving until hosted journal replay completes.

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

The sections below are **not** properties on `SquirixServerOptions`. They are loaded from the same settings file at node
startup (standalone `squirix-server`, `AddSquirixServer` with discovered settings, or `SquirixServer.StartAsync`). Use
`squirix-server validate-config --strict` to validate optional sections together with cluster settings.

### Persistence

| Field                         | Type   | Default in node host                                       | Validation                                                                                                                                 |
| ----------------------------- | ------ | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `DataDir`                     | string | `%LocalAppData%/squirix/<cluster>/<node>` or temp fallback | Required, non-empty                                                                                                                        |
| `JournalMaxSegmentMb`         | int    | `64`                                                       | `> 0`                                                                                                                                      |
| `FlushIntervalMs`             | int    | `10`                                                       | `> 0`                                                                                                                                      |
| `SnapshotIntervalSec`         | int    | `60`                                                       | `> 0`; persisted setting validated at startup. Snapshot **scheduling** uses the Snapshot section's `SnapshotInterval` (default 5 minutes). |
| `ManifestRetentionCount`      | int    | `3`                                                        | `> 0`                                                                                                                                      |
| `SnapshotRetentionCount`      | int    | `3`                                                        | `> 0`                                                                                                                                      |
| `StrictFsync`                 | bool   | `true`                                                     | Any boolean                                                                                                                                |
| `JournalGroupCommitMaxWaitMs` | int    | `0`                                                        | `>= 0` (`0` disables group commit)                                                                                                         |
| `JournalGroupCommitMaxBatch`  | int    | `32`                                                       | `> 0`                                                                                                                                      |

See [journal group commit](journal-group-commit.md) for latency vs throughput tradeoffs.

### Snapshot

| Field                        | Type            | Default in node host | Validation     |
| ---------------------------- | --------------- | -------------------- | -------------- |
| `Enabled`                    | bool            | `true`               | Any boolean    |
| `SnapshotInterval`           | TimeSpan string | `00:05:00`           | `> 0`          |
| `SnapshotEveryNOps`          | long            | `250000`             | `>= 0`         |
| `SnapshotEveryNBytes`        | long            | `134217728`          | `>= 0`         |
| `MinGapBetweenSnapshots`     | TimeSpan string | `00:01:00`           | `>= 0`         |
| `JournalGrowthThrottleBytes` | long            | `0`                  | `>= 0`         |
| `LatencySloMilliseconds`     | double          | `0`                  | finite, `>= 0` |
| `LatencyThrottleDuration`    | TimeSpan string | `00:00:10`           | `>= 0`         |

### Backpressure

Backpressure limits tune runtime cache-operation admission when present in settings. They apply before logical reads and
writes under load. REST/gRPC adapters still enforce transport-level limits (auth, payload size, deadlines,
cancellation). Memory pressure is a separate policy and is not configured by these fields.

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

| Field             | Type            | Default in node host | Validation  |
| ----------------- | --------------- | -------------------- | ----------- |
| `Enabled`         | bool            | `true`               | Any boolean |
| `MinTailSegments` | int             | `2`                  | `>= 0`      |
| `MinTailBytes`    | long            | `67108864`           | `>= 0`      |
| `MinGap`          | TimeSpan string | `00:02:00`           | `>= 0`      |

### Journal metrics exporter

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
[diagnostics — Security](diagnostics.md#metrics-route)).

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
In-process test hosts (`SquirixNodeHost`, `TestNodeHostFactory`) also accept an optional **per-node security override**
so parallel tests do not share process-wide environment state.

Use `TestNodeSecurityOptions` from `Squirix.Server.TestKit` when starting a node in tests. When provided, the override
replaces environment-variable lookup for that startup only; omit it on `IntegrationTestBase.StartNodeAsync` to keep
env-based behavior, or rely on the smoke-test default (empty override, unauthenticated node).

```csharp
// E2E / integration auth (JWT)
var credentials = TestJwtHelper.CreateRandomCredentials();
await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

// Smoke default: unauthenticated without touching process env
await StartNodeAsync(url, peers);
```

Symmetric JWT-protected nodes use `JwtSigningKey`, `JwtIssuer`, and `JwtAudience`. OIDC authority URLs use
`JwtAuthority`, `JwtAudience`, optional `JwtIssuer`, and `JwtAllowHttpMetadata` (set `true` for `http://` mock
authorities in tests).

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
`ASPNETCORE_Kestrel__Certificates__Default__Path` and `ASPNETCORE_Kestrel__Certificates__Default__Password` for the
bundled development PFX; see [containerization.md](containerization.md#https-in-containers).

| Variable                                             | Purpose                                                                                                                                                                                                            |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SQUIRIX_MTLS`                                       | Enables mutual TLS when `true` or `1`.                                                                                                                                                                             |
| `SQUIRIX_MTLS_ALLOW_SELF_SIGNED`                     | Allows self-signed client certificates for mTLS validation. Dev/test only.                                                                                                                                         |
| `SQUIRIX_JWT_AUTHORITY`                              | JWT authority for bearer authentication.                                                                                                                                                                           |
| `SQUIRIX_JWT_AUDIENCE`                               | JWT audience validation value.                                                                                                                                                                                     |
| `SQUIRIX_JWT_ISSUER`                                 | JWT issuer. Required when using `SQUIRIX_JWT_SIGNING_KEY` without authority.                                                                                                                                       |
| `SQUIRIX_JWT_SIGNING_KEY`                            | Symmetric JWT signing key, raw text or base64.                                                                                                                                                                     |
| `SQUIRIX_JWT_ALLOW_HTTP_METADATA`                    | Allows non-HTTPS authority metadata for JWT in dev/test.                                                                                                                                                           |
| `SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES`  | Overrides `MemoryPressure.MaxEstimatedCacheBytes` (must be positive and within the 80% RAM cap at startup).                                                                                                        |
| `SQUIRIX_MEMORY_PRESSURE_HIGH_THRESHOLD_PERCENT`     | Overrides `MemoryPressure.HighPressureThresholdPercent`.                                                                                                                                                           |
| `SQUIRIX_MEMORY_PRESSURE_CRITICAL_THRESHOLD_PERCENT` | Overrides `MemoryPressure.CriticalPressureThresholdPercent`.                                                                                                                                                       |
| `SQUIRIX_TEST_ROOT`                                  | Test-only root for generated node data directories.                                                                                                                                                                |

Security notes:

- Non-loopback listen URLs (`0.0.0.0`, public interfaces, Docker service hostnames) **require** JWT settings at
  startup; the process refuses to start without them. Loopback binds (`localhost`, `127.0.0.1`) allow unauthenticated
  cache access unless auth is explicitly configured.
- JWT bearer authentication is enforced server-side for gRPC cache endpoints when auth is
  enabled. Missing or invalid credentials are rejected.
- Operational routes (`/health`, `/metrics`) are served on the **primary HTTPS listener** only.

## Sample `appsettings.json`

```json
{
    "Squirix": {
        "Cluster": {
            "ClusterId": "prod-cache",
            "NodeId": "cache-a",
            "Url": "https://cache-a.example.internal:5001",
            "VirtualNodes": 256,
            "Peers": [
                { "NodeId": "cache-a", "Url": "https://cache-a.example.internal:5001" },
                { "NodeId": "cache-b", "Url": "https://cache-b.example.internal:5002" },
                { "NodeId": "cache-c", "Url": "https://cache-c.example.internal:5003" }
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
