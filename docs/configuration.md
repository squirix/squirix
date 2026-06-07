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
`SquirixServerOptions`; `app.MapSquirixServer()` maps gRPC, REST, health, admin, and metrics endpoints.

## Remote client (`SquirixOptions`)

Configure the v0.1 client when calling `SquirixClient.ConnectAsync`:

| Member                | Purpose                                                                                                           |
| --------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `Endpoints`           | Bootstrap server URLs (HA front door, not shards). See [bootstrap client failover](bootstrap-client-failover.md). |
| `ApiKey`              | Static API key sent as `x-api-key` on gRPC calls.                                                                 |
| `BearerTokenProvider` | Optional bearer token for each gRPC call.                                                                         |
| `Serializer`          | Per-session `ISquirixSerializer`; null uses default JSON for that client. See [serialization](serialization.md).  |

Example:

```csharp
await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add("https://cache-a.example.internal:5001");
        options.Endpoints.Add("https://cache-b.example.internal:5002");
        options.ApiKey = Environment.GetEnvironmentVariable("SQUIRIX_API_KEY");
    },
    cancellationToken);
```

Client authentication uses the same API key / bearer options as gRPC transport configuration on the server when auth is
enabled.

<!-- markdownlint-disable-next-line MD033 -->
<a id="memory-pressure-squirixsettingsjson"></a>

## Memory pressure (`Squirix.settings.json`)

The optional `Squirix:MemoryPressure` section is merged when present (same file discovery as `Squirix:Cluster`).
Environment variables listed below override merged file values. When enabled, the node may reject **growing** writes
under critical estimated memory usage. Those rejections occur before durable journal append. REST returns HTTP **429**
with code **`MEMORY_PRESSURE`**; gRPC returns **`ResourceExhausted`** (bounded payloads; field semantics are in the
table below).

| Field                              | Type  | Default           | Validation                                                                      |
| ---------------------------------- | ----- | ----------------- | ------------------------------------------------------------------------------- |
| `Enabled`                          | bool  | `false`           | Any boolean                                                                     |
| `MaxEstimatedCacheBytes`           | long? | `null` (no limit) | unset or `>= 0`; non-positive values are treated as no limit for classification |
| `HighPressureThresholdPercent`     | int   | `80`              | `(0, 100]`                                                                      |
| `CriticalPressureThresholdPercent` | int   | `95`              | `(0, 100]`, must be `>` `HighPressureThresholdPercent`                          |
| `RejectWritesOnCriticalPressure`   | bool  | `true`            | Any boolean (admission gate; REST/gRPC mapping when rejection occurs)           |

Example fragment:

```json
{
    "Squirix": {
        "MemoryPressure": {
            "enabled": true,
            "maxEstimatedCacheBytes": 1073741824,
            "highPressureThresholdPercent": 80,
            "criticalPressureThresholdPercent": 95,
            "rejectWritesOnCriticalPressure": true
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
| `Url`            | URI    | loader fallback                        | Absolute `http` or `https` origin URI, maximum 2048 characters; no credentials, path, query, or fragment                             |
| `VirtualNodes`   | int    | `128`                                  | `> 0` and `<= 16384`                                                                                                                 |
| `Peers`          | array  | runtime local-peer fallback when empty | When non-empty: must include local `NodeId`; peer ids and URLs must be unique; local peer `Url` must match `Url`; maximum 1024 peers |
| `Peers[].NodeId` | string | none                                   | Required, non-empty, maximum 128 characters                                                                                          |
| `Peers[].Url`    | URI    | none                                   | Absolute `http` or `https` origin URI, maximum 2048 characters; no credentials, path, query, or fragment                             |

CLI validation:

- `squirix-server validate-config --settings PATH` validates `Squirix:Cluster` only.
- `squirix-server validate-config --settings PATH --strict` also validates optional `MemoryPressure` and
  `PrometheusMetrics` sections when they are present.

Example:

```json
{
    "Squirix": {
        "Cluster": {
            "ClusterId": "dev-cluster",
            "NodeId": "node-a",
            "Url": "http://localhost:5001",
            "VirtualNodes": 128,
            "Peers": [
                { "NodeId": "node-a", "Url": "http://localhost:5001" },
                { "NodeId": "node-b", "Url": "http://localhost:5002" }
            ]
        }
    }
}
```

For local `--dev` hosts, `http://localhost:5001` is a common gRPC listen URL. In Docker Compose and other container
networks, set `Url` and the local peer entry to the **service hostname** reachable by other nodes (for example
`http://squirix-node-a:5000`), not `http://0.0.0.0:5000`. The local peer `Url` must exactly match `Cluster.Url`.

When exposing a container to host client apps: map gRPC port **5000** and set `SQUIRIX_HTTP1_PORT=5001` for health/admin
over HTTP/1. See [containerization.md](containerization.md).

## Hosting options (`SquirixServerOptions`)

Configure these through `builder.AddSquirixServer(...)`, `SquirixServer.StartAsync(...)`, or the `Squirix:Cluster`
section in settings (mapped into the same options model).

| Field                       | Type   | Default                        | Validation                                                                 |
| --------------------------- | ------ | ------------------------------ | -------------------------------------------------------------------------- |
| `WaitForRecovery`           | bool   | `true`                         | Any boolean                                                                |
| `AllowHttpInAnyEnvironment` | bool   | `false`                        | Any boolean; set `true` when using plaintext `http://` outside Development |
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

| Field         | Type   | Default in node host | Validation                                                               |
| ------------- | ------ | -------------------- | ------------------------------------------------------------------------ |
| `Enabled`     | bool   | `true`               | Any boolean                                                              |
| `Path`        | string | `/metrics`           | Non-empty, must start with `/` when `Enabled` is `true`                  |
| `RequireAuth` | bool   | `false`              | Any boolean; when `true` and server auth is enabled, requires `ApiOrJwt` |

Example fragment:

```json
{
    "Squirix": {
        "PrometheusMetrics": {
            "enabled": true,
            "path": "/metrics",
            "requireAuth": false
        }
    }
}
```

See [diagnostics](diagnostics.md#metrics-route) for scrape semantics and security notes.

## Environment variables

| Variable                                             | Purpose                                                                                                                                                                                                            |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SQUIRIX_API_KEYS`                                   | Comma-separated API keys. Enables the `ApiOrJwt` auth policy for REST cache routes, `/admin`, and gRPC cache endpoints.                                                                                            |
| `SQUIRIX_ADMIN_ENABLED`                              | Exposes `/admin` outside development when `true` or `1`.                                                                                                                                                           |
| `SQUIRIX_HTTP1_PORT`                                 | Adds a plaintext HTTP/1.1 sidecar listener for health/admin/browser-friendly access.                                                                                                                               |
| `SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL`              | Allows `SQUIRIX_HTTP1_PORT` to bind on non-loopback interfaces. Must be explicitly set to `true`/`1` for insecure external exposure.                                                                               |
| `SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL`             | Allows the primary listen URL on a non-loopback host without `SQUIRIX_API_KEYS` or JWT. Must be explicitly set to `true`/`1` for insecure external cache access. Loopback binds are always permitted without auth. |
| `SQUIRIX_MTLS`                                       | Enables mutual TLS when `true` or `1`.                                                                                                                                                                             |
| `SQUIRIX_MTLS_ALLOW_SELF_SIGNED`                     | Allows self-signed client certificates for mTLS validation. Dev/test only.                                                                                                                                         |
| `SQUIRIX_JWT_AUTHORITY`                              | JWT authority for bearer authentication.                                                                                                                                                                           |
| `SQUIRIX_JWT_AUDIENCE`                               | JWT audience validation value.                                                                                                                                                                                     |
| `SQUIRIX_JWT_ISSUER`                                 | JWT issuer. Required when using `SQUIRIX_JWT_SIGNING_KEY` without authority.                                                                                                                                       |
| `SQUIRIX_JWT_SIGNING_KEY`                            | Symmetric JWT signing key, raw text or base64.                                                                                                                                                                     |
| `SQUIRIX_JWT_ALLOW_HTTP_METADATA`                    | Allows non-HTTPS authority metadata for JWT in dev/test.                                                                                                                                                           |
| `SQUIRIX_MEMORY_PRESSURE_ENABLED`                    | When set to `true`/`1` or `false`/`0`, overrides `MemoryPressure.Enabled` after JSON merge.                                                                                                                        |
| `SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES`  | Overrides `MemoryPressure.MaxEstimatedCacheBytes` (non-positive clears the limit).                                                                                                                                 |
| `SQUIRIX_MEMORY_PRESSURE_HIGH_THRESHOLD_PERCENT`     | Overrides `MemoryPressure.HighPressureThresholdPercent`.                                                                                                                                                           |
| `SQUIRIX_MEMORY_PRESSURE_CRITICAL_THRESHOLD_PERCENT` | Overrides `MemoryPressure.CriticalPressureThresholdPercent`.                                                                                                                                                       |
| `SQUIRIX_MEMORY_PRESSURE_REJECT_WRITES_ON_CRITICAL`  | When set to `true`/`1` or `false`/`0`, overrides `MemoryPressure.RejectWritesOnCriticalPressure` (admission behavior when enabled).                                                                                |
| `SQUIRIX_TEST_ROOT`                                  | Test-only root for generated node data directories.                                                                                                                                                                |

Security notes:

- `ApiOrJwt` is enforced server-side for REST cache routes, `/admin`, and gRPC cache endpoints when auth is enabled.
- API key and JWT credentials are accepted consistently across REST and gRPC when configured; missing/invalid
  credentials are rejected.
- The sidecar created by `SQUIRIX_HTTP1_PORT` is plaintext even when the main node endpoint is HTTPS.
- By default, plaintext sidecar binding is loopback-only. Non-loopback plaintext exposure fails startup unless
  `SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL=true`.
- `SQUIRIX_ADMIN_ENABLED` is not a safe production toggle by itself. Pair it with network restriction and
  authentication.
- The v0.1 `/admin` surface is limited to `whoami`, owner lookup, and ring inspection. See
  [diagnostics](diagnostics.md#admin-routes-v01).

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
- `MemoryPressure MaxEstimatedCacheBytes cannot be negative.`
