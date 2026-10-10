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
then hosts the node through the same `AddSquirixServerAsync` / `MapSquirixServerAsync` pipeline as the standalone executable.
Other sections such as `MemoryPressure`, `Snapshot`, and `PrometheusMetrics` are still merged from the same settings file
at runtime when present. Custom ASP.NET Core hosts configure cluster topology and optional persistence through
`SquirixServerOptions` (`UsePersistence()`); `await app.MapSquirixServerAsync()` opens node storage and maps gRPC, health, and metrics endpoints.

## Remote client (`SquirixClientOptions`)

Configure the v0.1 client when calling `SquirixClient.ConnectAsync`:

| Member                | Purpose                                                                                                           |
| --------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `Endpoints`           | Bootstrap server URLs (HA front door, not shards). See [bootstrap client failover](bootstrap-client-failover.md). |
| `BearerTokenProvider` | Supplies a JWT bearer token for each gRPC call when the server requires authentication.                           |
| `Serializer`          | Per-session `ISquirixSerializer`; null uses default JSON for that client. See [serialization](serialization.md).  |

For local HTTPS development, trust the ASP.NET Core development certificate with
`dotnet dev-certs https --trust`.

Example (requires a JWT in `SQUIRIX_CLIENT_JWT`, for example `export SQUIRIX_CLIENT_JWT=...` /
`set SQUIRIX_CLIENT_JWT=...`):

```csharp
using System;
using System.Threading.Tasks;
using Squirix.Client;

await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add(new Uri("https://cache-a.example.internal:5001"));
        options.Endpoints.Add(new Uri("https://cache-b.example.internal:5002"));
        options.BearerTokenProvider = static _ =>
        {
            var token = Environment.GetEnvironmentVariable("SQUIRIX_CLIENT_JWT");
            if (string.IsNullOrWhiteSpace(token))
                throw new InvalidOperationException("SQUIRIX_CLIENT_JWT is not set.");

            return new ValueTask<string>(token);
        };
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

| Field                      | Type     | Default                                | Validation                                                                                                                                                                            |
| -------------------------- | -------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NodeId`                   | string   | loader fallback                        | Required, non-empty, maximum 128 characters                                                                                                                                           |
| `ClusterId`                | string   | loader fallback                        | Required, non-empty, maximum 128 characters                                                                                                                                           |
| `Uri`                      | URI      | loader fallback                        | Absolute `https` origin URI (max 2048); rejects `http://`; no credentials, path, query, or fragment                                                                                   |
| `VirtualNodes`             | int      | `128`                                  | `> 0` and `<= 16384`                                                                                                                                                                  |
| `ReplicaCount`             | int      | `1`                                    | `1..5`, at most the peer count; RF>1 needs persistence, cluster mTLS, and `ReplicationEnabled`; see the [topology stamp](#activated-topology-stamp-topologystamp)                     |
| `ConfigurationGeneration`  | ulong    | `1`                                    | `> 0`; recorded in the [activated topology stamp](#activated-topology-stamp-topologystamp) on the first RF>1 start                                                                    |
| `ForwardConnectTimeout`    | TimeSpan | `00:00:01`                             | `>= 00:00:00.010` and below `00:00:03`, the per-attempt timeout of a forwarded call; see [forward connect timeout](#forward-connect-timeout-forwardconnecttimeout)                    |
| `AutomaticFailoverEnabled` | bool     | `false`                                | Elects a new group leader when the leader goes silent; requires `ReplicaCount` of at least 3 and `QuorumReadsEnabled`; see [automatic failover](#automatic-failover-and-quorum-reads) |
| `QuorumReadsEnabled`       | bool     | `false`                                | Confirms each read with a majority of its replica group; requires `AutomaticFailoverEnabled`; see [automatic failover](#automatic-failover-and-quorum-reads)                          |
| `Peers`                    | array    | runtime local-peer fallback when empty | When non-empty: must include local `NodeId`; peer ids and URIs must be unique; local peer `Uri` must match `Uri`; maximum 1024 peers                                                  |
| `Peers[].NodeId`           | string   | none                                   | Required, non-empty, maximum 128 characters                                                                                                                                           |
| `Peers[].Uri`              | URI      | none                                   | Same validation as `Uri`                                                                                                                                                              |
| `Backpressure`             | object   | see below                              | Optional; keys and validation are listed in [Backpressure](#backpressure)                                                                                                             |
| `Journal`                  | object   | see below                              | Optional; keys and validation are listed in [Journal](#journal)                                                                                                                       |

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

| Field                      | Type     | Default                  | Validation                                                                                                                                                                            |
| -------------------------- | -------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ClusterId`                | string   | `cluster`                | Non-empty; validated with topology                                                                                                                                                    |
| `NodeId`                   | string   | `node`                   | Non-empty; validated with topology                                                                                                                                                    |
| `Uri`                      | `Uri`    | `https://localhost:5001` | Absolute HTTPS URI                                                                                                                                                                    |
| `Peers`                    | peers    | empty (local added)      | `SquirixServerPeerOptions` `NodeId` / `Uri` topology rules                                                                                                                            |
| `VirtualNodes`             | int      | `128`                    | `1..16384`                                                                                                                                                                            |
| `ReplicaCount`             | int      | `1`                      | `1..5`, at most the peer count; RF>1 needs persistence, cluster mTLS, and `ReplicationEnabled`; see the [topology stamp](#activated-topology-stamp-topologystamp)                     |
| `ConfigurationGeneration`  | ulong    | `1`                      | `> 0`; recorded in the [activated topology stamp](#activated-topology-stamp-topologystamp) on the first RF>1 start                                                                    |
| `ForwardConnectTimeout`    | TimeSpan | `00:00:01`               | `>= 00:00:00.010` and below `00:00:03`, the per-attempt timeout of a forwarded call; see [forward connect timeout](#forward-connect-timeout-forwardconnecttimeout)                    |
| `AutomaticFailoverEnabled` | bool     | `false`                  | Elects a new group leader when the leader goes silent; requires `ReplicaCount` of at least 3 and `QuorumReadsEnabled`; see [automatic failover](#automatic-failover-and-quorum-reads) |
| `QuorumReadsEnabled`       | bool     | `false`                  | Confirms each read with a majority of its replica group; requires `AutomaticFailoverEnabled`; see [automatic failover](#automatic-failover-and-quorum-reads)                          |
| `Backpressure`             | options  | see below                | `SquirixServerBackpressureOptions`; see [Backpressure](#backpressure)                                                                                                                 |
| `Journal`                  | options  | see below                | `SquirixServerJournalOptions`; see [Journal](#journal)                                                                                                                                |
| `PersistenceEnabled`       | bool     | `false`                  | Any boolean                                                                                                                                                                           |
| `ReplicationEnabled`       | bool     | `false`                  | Opt-in for RF>1 replication; RF>1 without it refuses startup                                                                                                                          |
| `WaitForRecovery`          | bool     | `true`                   | Any boolean; applies when persistence is enabled                                                                                                                                      |
| `DataDirectory`            | string?  | `null`                   | Optional path when persistence is enabled; requires `UsePersistence()`                                                                                                                |

Call `options.UsePersistence()` (or `options.UsePersistence("./data")`) to enable journal/snapshot persistence. The standalone
host accepts `--persist`; `--data-dir` requires `--persist`.

`ReplicaCount`, `ConfigurationGeneration`, `AutomaticFailoverEnabled`, and `QuorumReadsEnabled` have no CLI flag; the
standalone host reads them from `Squirix:Cluster:ReplicaCount`, `Squirix:Cluster:ConfigurationGeneration`,
`Squirix:Cluster:AutomaticFailoverEnabled`, and `Squirix:Cluster:QuorumReadsEnabled`. The first RF>1 start records
the replica count and the generation in the [activated topology stamp](#activated-topology-stamp-topologystamp), and
the two switches through its topology fingerprint; later RF>1 starts with a different value are refused.

Example:

```csharp
await builder.AddSquirixServerAsync(options =>
{
    options.NodeId = "node-a";
    options.Uri = new Uri("https://localhost:5001");
    options.UsePersistence("./data");
});
```

### Replication opt-in (`ReplicationEnabled`)

RF>1 replication activates only when explicitly opted in. `ReplicationEnabled` defaults to `false`: starting
with `ReplicaCount` greater than one and without the opt-in fails fast naming the switch. For RF>1 the opt-in
error is reported only after the persistence and mTLS prerequisite checks. RF=1 nodes run
with or without it. The standalone host accepts `--enable-replication`; the settings key is
`Squirix:Cluster:ReplicationEnabled`.

### Automatic failover and quorum reads

`AutomaticFailoverEnabled` and `QuorumReadsEnabled` default to `false`. With both off, the owner of each key leads its
replica group for the lifetime of the topology and serves reads from its local state; while the owner is down, writes
to its group fail.

With both on, the members of each replica group elect a new leader when the current leader goes silent, and a read of
a replicated key is confirmed by a majority of its replica group before it is served, so it observes every write
committed before it. A node that cannot reach a majority of a group refuses current reads and writes for that group.

Validation refuses every other combination in `SquirixServerOptions.Validate()`, settings loading, `validate-config`,
and at host startup:

- `AutomaticFailoverEnabled` requires `QuorumReadsEnabled`, and `QuorumReadsEnabled` requires
  `AutomaticFailoverEnabled`: set both or neither.
- `AutomaticFailoverEnabled` requires `ReplicaCount` of at least 3: groups of one or two replicas never elect a leader.
  RF>1 already requires persistence, cluster mTLS, and `ReplicationEnabled`.

Every node of a cluster must use the same values. Both switches are inputs of the topology fingerprint: a node
configured differently refuses replication with its peers, and the
[activated topology stamp](#activated-topology-stamp-topologystamp) refuses a start that changes them on an existing
data directory. Turn them on when the cluster is created, on empty data directories. There is no CLI flag; the
settings keys are `Squirix:Cluster:AutomaticFailoverEnabled` and `Squirix:Cluster:QuorumReadsEnabled`. A fragment
of the cluster section (identity and peers omitted):

```json
{
    "Squirix": {
        "Cluster": {
            "ReplicaCount": 3,
            "ReplicationEnabled": true,
            "PersistenceEnabled": true,
            "AutomaticFailoverEnabled": true,
            "QuorumReadsEnabled": true
        }
    }
}
```

A member stops routing to a leader it has not heard from for one election timeout and waits for the next one; a follower
without a live leader answers `Unavailable` (no leader) instead of a stale-owner hint that names the silent leader.

### Forward connect timeout (`ForwardConnectTimeout`)

`ForwardConnectTimeout` bounds how long a client call this node forwards to another cluster node waits for a new
connection to that node to be established, before any TLS handshake. The default is one second; the settings key is
`Squirix:Cluster:ForwardConnectTimeout` (for example `"00:00:01"`). It must be at least 10 milliseconds and below the
3-second per-attempt timeout of a forwarded call, so a node whose host is down or drops connection attempts fails the
connect before the call times out. A connect that fails sends nothing to the peer, so the forward fails as unavailable
with the detail `owner_unreachable` instead of an ambiguous timeout. After a connect timed out, further forwards to the
same node fail the same way at once for one more bound, instead of each waiting for it in turn.

- Name resolution and the attempts on every resolved address count against the bound, so a slow resolver or a peer name
  with several addresses uses it up sooner.
- On high-latency or WAN links, raise it toward 2 seconds; a value close to 3 seconds leaves no time for another member
  to take the call.
- It covers new connections only. Connections to other nodes that carry forwards are checked with HTTP/2 keepalive
  pings (one per second of silence, closed when unanswered for two seconds), so a peer that stops answering without
  closing the connection is found within about five seconds and the next forward connects anew. The two-second answer
  time trades one more second of detection for fewer healthy connections closed by a paused process. A forward already
  written to such a connection still ends as an ambiguous timeout or failure.
- A live node accepts the connection at once; the TLS handshake that follows keeps a longer bound, so a loaded node is
  not cut off. The setting applies to client forwards only: replication and election traffic between nodes keeps its
  own longer bounds.

### Activated topology stamp (`topology.stamp`)

The first start of an RF>1 node with persistence enabled writes `topology.stamp` into the data directory. The stamp
records the activated topology:

- `ConfigurationGeneration`;
- `ReplicaCount`;
- the topology fingerprint, a SHA-256 digest over the cluster id, replica count, virtual nodes, configuration
  generation, the peer set (each peer's node id, client URI, and internode URI), the minimum cluster package version,
  the `AutomaticFailoverEnabled` and `QuorumReadsEnabled` switches, and the replication policy constants of the build.

Only RF>1 activation writes the stamp, and nodes without persistence never read it. Nothing rewrites the stamp after
the first RF>1 start: every later RF>1 start compares the configured topology with it and refuses startup on any
difference. An RF=1 start with persistence refuses any data directory that carries a stamp, because an RF=1 node would
ignore the replica group logs in it.

Changing the activated topology of an existing data directory is not supported in this release. That covers a new
`ConfigurationGeneration` or `ReplicaCount`, a changed peer set, cluster id, or virtual node count, a change to
`AutomaticFailoverEnabled` or `QuorumReadsEnabled`, and an upgrade to a
release whose minimum cluster package version or replication policy constants differ. Moving existing RF=1 data to
RF>1, or starting an RF>1 data directory as RF=1, is not supported either.

Startup errors:

- `Data directory holds durable cache journal state but no activated topology stamp, so it was last used by an RF=1
  node; moving existing RF=1 data to RF>1 is not supported in this release. Start the RF>1 node on an empty data
  directory, or migrate the data at the application level.` An RF>1 start found cache journal segments but no stamp.
  Point the RF>1 node at an empty data directory and reload the data through the client API, or keep running the
  directory as RF=1.
- `Configured topology does not match the activated topology stamp in the data directory: {changes}. Changing the
  activated topology of an existing data directory is not supported in this release; start the node with the
  configuration and package version the directory was activated with, or on an empty data directory.` `{changes}`
  names each changed field with its stamped and configured values: `generation changed (stamped 1, configured 2)`,
  `replica count changed (stamped 2, configured 3)`, or, when both of those match, `topology fingerprint changed
  (stamped {hex}, configured {hex})` followed by the inputs that can cause it. Restore the settings and package version
  the directory was activated with, or start the node on an empty data directory.
- `Data directory was activated for replica count {n} (generation {g}); starting it as RF=1 is not supported in this
  release. Start the node with the replica count the directory was activated with, or on an empty data directory.` An
  RF=1 start found a stamp, so the directory was activated as RF>1 and holds replica group logs. Restore the replica
  count the directory was activated with, or start the RF=1 node on an empty data directory.
- `Replica group '{group}' log was written for a different topology (stored fingerprint {hex} generation {g},
  configured fingerprint {hex} generation {g}); applying it could write keys this node no longer owns. Run
  'squirix-server doctor' to inspect the data directory.` The stamp matched, but a group log records another topology,
  for example a group directory copied from another node. Run `squirix-server doctor`, then start the node on an empty
  data directory.

`squirix-server doctor` reports the stamp next to the configured topology, a stamp under an RF=1 configuration, and
RF=1 journal state under an RF>1 configuration, without starting the node; it exits with code 1 when it reports a mismatch. Without `--data-dir` or
`DataDirectory`, it checks the default data directory the node would use. See
[operational-runbook.md](operational-runbook.md#activated-topology-stamp).

### Recovery startup (`WaitForRecovery`)

When persistence is enabled and `WaitForRecovery` is `true` (default), the node blocks serving until hosted journal
replay completes.

When `WaitForRecovery` is `false`, replay runs in the background:

- journal mutations wait on the startup gate (unchanged).
- Cache reads wait on the same gate until replay completes.
- `/health/ready` stays **Unhealthy** until the gate opens (`journal_recovery` check).
- `/health/ready` also reports **Unhealthy** for fatal durability maintenance failures (`journal_maintenance`),
  including a journal pipeline latched as failed (for example a failed fsync; the node cannot commit until restart
  and the check description names the failure), failed journal compaction state, or fatal snapshot trigger failure.
- `/health/ready` reports **Degraded** (HTTP `200`, body `Degraded`) while one journal write or flush has been in
  progress for at least `JournalStallDegradedThreshold`, and returns to **Healthy** once it completes. See
  [diagnostics.md](diagnostics.md).
- `/health/live` remains available for process liveness.

Use non-blocking recovery only when load balancers honor `/health/ready` and callers tolerate delayed read availability
during startup.

## Node settings file (`Squirix.settings.json`)

Except for `Squirix:Cluster:Backpressure` and `Squirix:Cluster:Journal`, which are properties of `SquirixServerOptions`, the optional sections below are
**not** properties on `SquirixServerOptions`. In v0.1 public hosting, only some of them are merged from the settings file
at startup:

| Section | Loaded from `Squirix.settings.json`? | Notes |
| --- | --- | --- |
| `MemoryPressure` | Yes | Merged when present |
| `Snapshot` | Yes | Merged when present |
| `PrometheusMetrics` | Yes | Merged when present |
| Persistence knobs (`PersistenceOptions`) | No | Host defaults when `--persist` / `UsePersistence()`; not a JSON section today |
| Backpressure | Yes, as `Squirix:Cluster:Backpressure` | A property of `SquirixServerOptions`, see [Backpressure](#backpressure) |
| Journal group commit | Yes, as `Squirix:Cluster:Journal` | A property of `SquirixServerOptions`, see [Journal](#journal) |
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
| `JournalMaxSegmentMb`         | int    | `64`                                                       | `>= 9`: a segment must hold the largest journal frame (an entry or a recorded reply of up to 8 MiB, plus framing)                          |
| `ManifestRetentionCount`      | int    | `3`                                                        | `> 0`                                                                                                                                      |
| `SnapshotRetentionCount`      | int    | `3`                                                        | `> 0`                                                                                                                                      |
| Journal group commit wait     | span   | `0` (disabled)                                             | Set through `SquirixServerOptions.Journal`, see [Journal](#journal)                                                                        |
| Journal group commit batch    | int    | `32`                                                       | Set through `SquirixServerOptions.Journal`, see [Journal](#journal)                                                                        |
| `JournalMaxSegmentCount`      | int    | `32`                                                       | `> 0` (Pipelined journal segment count cap)                                                                                                |
| `JournalMaxTotalBytesMb`      | int    | `2048`                                                     | `>= 9` (Pipelined journal total on-disk size hard cap; must hold one segment with the largest journal frame)                               |
| `ReplicaLogCompactionMb`      | int    | `64`                                                       | `> 0` (RF>1: `group.log` size of each served replica group that triggers its compaction)                                                   |
| `ReplicaLogCompactionEntries` | int    | `100000`                                                   | `> 0` (RF>1: entries in each served replica group log that trigger its compaction)                                                         |

Additional host defaults (also not merged from `Squirix.settings.json`):

- `JournalWriteBatchBytes` — `16777216` (16 MiB); `> 0`; journal write-coalescing buffer (larger frames bypass coalescing)
- `RetentionCleanupDegradedWrites` — `3`; consecutive retention-cleanup write failures before degraded readiness
- `RetentionCleanupDegradedWindowFailures` — `5`; failures inside the sliding window before degraded readiness
- `RetentionCleanupDegradedWindowMinutes` — `15`; sliding window used with `RetentionCleanupDegradedWindowFailures`
- `JournalStallDegradedThreshold` — `5` seconds; `> 0`; how long one journal write or flush may stay in progress before
  `journal_maintenance` reports readiness **Degraded**

`JournalMaxTotalBytesMb` soft high-water for `/health/ready/details` is fixed at 80% of this limit. Durable writes
that would exceed the hard cap are rejected with `JOURNAL_DISK_QUOTA` (gRPC `ResourceExhausted`); readiness
stays healthy. See [Journal disk quota](operational-runbook.md#journal-disk-quota) for operator guidance.

Journal group commit is configured through the [Journal](#journal) section. See [journal group commit](journal-group-commit.md) for how it works.

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

### Journal

Journal group commit is configured through `SquirixServerOptions.Journal` (`SquirixServerJournalOptions`) or the
`Squirix:Cluster:Journal` settings object; every key is optional and keeps the default listed below. Group commit lets
concurrent durable mutations share one journal flush at the cost of up to `GroupCommitMaxWait` of extra commit latency.
It is off by default and requires persistence (`PersistenceEnabled`, `UsePersistence()` or `--persist`). Changes apply on
the next host start.

```json
{
    "Squirix": {
        "Cluster": {
            "PersistenceEnabled": true,
            "Journal": {
                "GroupCommitMaxWait": "00:00:00.002",
                "GroupCommitMaxBatch": 64
            }
        }
    }
}
```

| Field                 | Type     | Default    | Validation                                                                                         |
| --------------------- | -------- | ---------- | -------------------------------------------------------------------------------------------------- |
| `GroupCommitMaxWait`  | TimeSpan | `00:00:00` | `0` (group commit off) or `1..100` whole milliseconds; a positive value needs `PersistenceEnabled` |
| `GroupCommitMaxBatch` | int      | `32`       | `1..4096`; used only when `GroupCommitMaxWait` is greater than zero                                |

`TimeSpan` values are strings in `[d.]hh:mm:ss[.fffffff]` form; a number is rejected and the error names the field.
A bare `"5"` is read as five days and rejected by the range check. With RF>1 group commit affects the node journal only, and a
replicated commit stretches by up to `GroupCommitMaxWait`. Unknown keys in the `Journal` object fail loading, and `Journal`
must not be `null`. Invalid values fail host startup, `validate-config`, and settings loading, for example with `Journal GroupCommitMaxWait must be zero or between 1 and 100
whole milliseconds (for example "00:00:00.005").`

### Backpressure

Backpressure is configured through `SquirixServerOptions.Backpressure` (`SquirixServerBackpressureOptions`) or the
`Squirix:Cluster:Backpressure` settings object; every key is optional and keeps the default listed below. Limits apply
before logical reads and writes under load. gRPC transport adapters still enforce transport-level limits (auth, payload
size, deadlines, cancellation). Memory pressure is a separate policy. Node-wide admission control is always on and
cannot be switched off from public options.

```json
{
    "Squirix": {
        "Cluster": {
            "Backpressure": {
                "PerClientMaxInFlight": 32,
                "PerClientRateLimitPerSecond": 200,
                "PerClientRateLimitBurst": 400,
                "MaxQueueWait": "00:00:00.250"
            }
        }
    }
}
```

Invalid values fail host startup, `validate-config`, and settings loading with the matching message from
[Validation failures](#validation-failures). Unknown keys in the `Backpressure` object (for example a misspelled name)
fail loading too. `TimeSpan` values are strings in `[d.]hh:mm:ss[.fffffff]` form, for example `"00:00:00.250"`.

Per-client limits (`PerClientMaxInFlight`, `PerClientRateLimit*`) key off a **backpressure client
id** resolved for each cache operation:

| Source | Client id | When |
| ------ | --------- | ---- |
| JWT bearer principal | `jwt:{subject}` | Authenticated request with a non-empty `sub` / `NameIdentifier` claim |
| ASP.NET Core connection | `conn:{connectionId}` | Request has an `HttpContext` but no usable principal id (anonymous loopback, authenticated token missing `sub`) |
| Internal owner RPC | `internal` | Trusted owner-routed call from another cluster node (peer mTLS on the internal listener). Admitted only to a free slot: no per-client limits, slowdown or queueing; the node rate limit applies |
| In-process / missing context | `runtime` | No `HttpContext` (host bootstrap, some tests, non-HTTP callers). All such callers share one bucket |

Setting `PerClientMaxInFlight` or `PerClientRateLimitPerSecond` turns on per-caller client ids; without either, all callers
share one bucket and no caller identity is computed per request; internal owner calls forwarded from another node are still
told apart.

v0.1 external auth is JWT-only; there is no API-key principal. A request forwarded to its key owner is admitted on the
entry node under the caller's own client id, including the slowdown delay and queue wait, and holds that admission for
the whole hop. The owner treats it as an internal owner call (mTLS on the internal listener): it skips per-client limits,
the slowdown delay and the queue, so a forwarded request waits in at most one queue and traffic is not pooled into one
bucket per peer connection. The owner admits it only to a free slot and refuses it at once with `forwarded_no_slot` when
none is free; the entry node does not retry that refusal, so the client retries it with backoff. The node rate limit
still applies to it, and it never overtakes requests already queued on the owner.

The entry node also limits forwarded calls per owner to half of `MaxInFlight` (at least one) concurrent calls. A call
that finds all of them taken is refused at once with `peer_busy` and does not wait while holding its admission slot, so a
slow or hung owner cannot pin more than that many slots. The entry node sends each forwarded call once and does not
retry it against the owner; the Squirix client retries transient failures (`Unavailable`, `Internal`, `ResourceExhausted`
and deadline expiry) with the same operation id. A raw gRPC caller that does not use the Squirix client must retry them
itself.

| Field                         | Type            | Default        | Validation                                       |
| ----------------------------- | --------------- | -------------- | ------------------------------------------------ |
| `MaxInFlight`                 | int             | `256`          | `> 0`                                            |
| `PerClientMaxInFlight`        | int?            | `null`         | unset or `1..MaxInFlight`                        |
| `MaxQueue`                    | int             | `128`          | `>= 0`                                           |
| `SlowdownThreshold`           | int             | `192`          | `1..MaxInFlight`                                 |
| `NodeRateLimitPerSecond`      | int?            | `null`         | unset or `> 0` with `NodeRateLimitBurst`         |
| `NodeRateLimitBurst`          | int?            | `null`         | needs `NodeRateLimitPerSecond`; `>=` that rate   |
| `PerClientRateLimitPerSecond` | int?            | `null`         | unset or `> 0` with `PerClientRateLimitBurst`    |
| `PerClientRateLimitBurst`     | int?            | `null`         | needs the rate; `>= PerClientRateLimitPerSecond` |
| `MaxSlowdownDelay`            | TimeSpan string | `00:00:00.025` | `0` to `00:00:05`                                |
| `MaxQueueWait`                | TimeSpan string | `00:00:00.250` | `> 0` and at most `00:01:00`                     |

A request is admitted while a slot is free. Once all `MaxInFlight` slots are taken, requests wait in a first-in,
first-out queue of up to `MaxQueue` requests, each for at most `MaxQueueWait`; a request that finds the queue full is rejected
with `queue_full`, and one that waits too long with `queue_wait_timeout`. From `SlowdownThreshold` in-flight requests on,
each new request is first delayed by up to `MaxSlowdownDelay`, growing linearly to the full delay when `MaxInFlight` slots are
taken, so a queued request can wait up to `MaxSlowdownDelay` plus `MaxQueueWait` (275 ms with the defaults).
`PerClientMaxInFlight` counts a caller's admitted and queued requests together; a request over that limit is rejected,
never queued. Forwarded owner calls are never queued on the owner (see above). A burst is meaningless without a rate, so setting a burst alone is rejected.

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
`MapSquirixServerAsync()`.

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
In-process test hosts also accept an optional **per-node security override**
so parallel tests do not share process-wide environment state.

Use `TestNodeSecurityOptions` from `Squirix.Server.TestKit.Hosting` when starting a node in tests. When provided, the override
replaces environment-variable lookup for that startup only; leave the `Security` property of `IntegrationStartOptions` unset on `NodeIntegrationTestBase.StartClusterAsync` to keep
env-based behavior, or rely on the smoke-test default (empty override, unauthenticated node).

```csharp
// E2E / integration auth (JWT)
var credentials = TestJwtHelper.CreateRandomCredentials();
await StartNodeAsync(url, peers, security: TestNodeSecurityOptions.FromJwtCredentials(credentials));

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
without external network access. E2E tests run with TUnit parallelization enabled; auth scenarios must use explicit
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
| `SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT`                 | Dedicated cluster/internal HTTPS listener port for internode gRPC mTLS. Required when remote cluster peers are configured and must differ from the primary `Cluster.Uri` port.                                     |
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
  internode mTLS is required at startup. Internode gRPC is served on the dedicated internal HTTPS listener
  (`SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT`) with required peer client certificates. Each node certificate CN must match
  its `Cluster.NodeId`; peer certificates are accepted only when they chain to the cluster CA and their CN matches the
  expected peer `NodeId`. Outbound `ClientPool` calls attach the local node certificate and apply the same trust and
  identity checks to peer server certificates. Standalone nodes without remote peers do not require cluster mTLS
  material. The primary listener keeps external client behavior unchanged.
- Deployment, rotation, and dev certificate generation for **internode mTLS** are documented in
  [security/internode-mtls.md](security/internode-mtls.md). Squirix consumes externally managed cluster certificates;
  it does not act as a production CA. Internode trust requires the PEM cluster CA at
  `SQUIRIX_CLUSTER_MTLS_CA_PATH` and certificate CN equal to the expected cluster `NodeId`.
- **External JWT** signing, blast radius, and rotation (symmetric vs OIDC) are documented in
  [security/jwt-signing-keys.md](security/jwt-signing-keys.md). Internode forwarding does not use JWT when mTLS is
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

Typical examples from options validators (host composition / `validate-config`). Backpressure messages are also
returned by `SquirixServerOptions.TryValidate` and settings loading, as are the `Journal` messages. Persistence messages apply when that option object
is constructed or overridden in a custom host — they are **not** produced by merging a JSON section that v0.1 public
hosting ignores:

- `Backpressure SlowdownThreshold must be in the range [1, MaxInFlight].`
- `Backpressure PerClientMaxInFlight cannot exceed MaxInFlight.`
- `Backpressure NodeRateLimitBurst must be greater than zero when configured.`
- `Backpressure NodeRateLimitBurst requires NodeRateLimitPerSecond.`
- `Backpressure MaxQueueWait cannot exceed 00:01:00.`
- `Journal GroupCommitMaxBatch must be between 1 and 4096.`
- `Journal GroupCommitMaxWait must be zero or between 1 and 100 whole milliseconds (for example "00:00:00.005").`
- `Journal GroupCommitMaxWait greater than zero requires persistence. Set PersistenceEnabled.`
- `Persistence DataDir is required.`
- `ReplicaCount greater than 1 requires the replication opt-in. Enable Squirix:Cluster:ReplicationEnabled (or pass --enable-replication).`
- `AutomaticFailoverEnabled requires ReplicaCount of at least 3: groups of one or two replicas never elect a leader.`
- `AutomaticFailoverEnabled requires QuorumReadsEnabled: a cluster that elects leaders fences its reads. Set both
  Squirix:Cluster:AutomaticFailoverEnabled and Squirix:Cluster:QuorumReadsEnabled, or neither.`
- `QuorumReadsEnabled requires AutomaticFailoverEnabled: quorum reads are served by elected leaders only. Set both
  Squirix:Cluster:AutomaticFailoverEnabled and Squirix:Cluster:QuorumReadsEnabled, or neither.`
- `Persistence JournalMaxSegmentMb must be at least 9: a journal segment must hold the largest journal frame.`
- `MemoryPressure HighPressureThresholdPercent must be less than CriticalPressureThresholdPercent.`
- `MemoryPressure MaxEstimatedCacheBytes must be positive when set.`
- `MemoryPressure MaxEstimatedCacheBytes ({configured}) exceeds the 80% RAM cap ({cap}).`
- `MemoryPressure cannot resolve RAM budget: available process memory is zero.`
