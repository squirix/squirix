# Operational Runbook

This runbook covers diagnostics, upgrades, backups, restores, and recovery workflows for squirix nodes.

Related documents:

- [diagnostics.md](diagnostics.md)
- [containerization.md](containerization.md)
- [configuration.md#memory-pressure-squirixsettingsjson](configuration.md#memory-pressure-squirixsettingsjson)
- [storage-maintenance.md](storage-maintenance.md)

squirix **0.x** releases (from **0.1.0** until **1.0.0**) are preview releases. Treat every upgrade as potentially
breaking unless the target release explicitly documents otherwise.

## First response

When a node behaves unexpectedly:

1. Stop writes from non-critical clients if data integrity is in doubt.
2. Capture logs, readiness details, and the current configuration before restarting the node.
3. Record the squirix version, serializer package/version, node id, peer set, and data directory path.
4. Check whether the issue affects one node, one owner range, or the whole cluster.
5. Back up the data directory before running repair, compaction, restore, or upgrade steps.

Before changing cluster topology in containers, validate settings:

```bash
squirix-server validate-config --settings ./Squirix.settings.json --strict
```

See [containerization.md](containerization.md) for Docker Compose examples (`Cluster.Uri` must match the local peer
entry).

## Diagnostics

Use these surfaces first:

- `/health/live`
- `/health/ready`
- `/health/ready/details`

Collect:

- health/readiness detail
- configured peer set and static ring shape
- journal and snapshot errors from logs
- Backpressure and request failures from logs
- Serializer and journal JSON codec failures
- Correlation or trace ids for failing requests
- `memoryPressure` on `/health/ready/details` (state, resolved byte limit, estimated usage, entry count, rejections;
  write rejection active). See [configuration.md](configuration.md#memory-pressure-squirixsettingsjson).
- `journalDisk` on `/health/ready/details` (state, max/used/high-water bytes, write rejection active). See
  [Journal disk quota](#journal-disk-quota).

Trace ownership during triage:

- Logical cache operation spans are emitted by `TracingCacheDecorator<T>` through the `Squirix` `ActivitySource`.
- Use logical spans for operation/result triage (`cache.operation`, `cache.result`, `squirix.node_id`).
- Use gRPC interceptor spans and correlation for transport-level failures.
- Use journal, snapshot, and compaction spans for storage failures.
- Do not expect logical operation spans to include raw keys, values, payloads, cache names, or exception messages.

Security checks during triage:

- Confirm whether the primary listener is loopback-only or exposed on a non-loopback interface. Loopback binds
  (`localhost`, `127.0.0.1`) may run without JWT by design; that trusts all local processes — not a production posture.
  See [server-mode.md](server-mode.md#loopback-development-default-not-production-posture).
- Non-loopback listen URLs refuse startup without JWT settings.
- Confirm auth is enabled where required for exposed interfaces.
- For symmetric JWT deployments, treat `SQUIRIX_JWT_SIGNING_KEY` compromise as full external API forgery risk; prefer
  OIDC in production. See [security/jwt-signing-keys.md](security/jwt-signing-keys.md).
- Verify that gRPC cache, remote `/metrics` scrapes, and remote `/health/ready/details` scrapes are challenged
  consistently for missing/invalid credentials (`/health`, `/health/live`, and `/health/ready` remain anonymous).
- On shared or multi-tenant hosts, remember that loopback `/metrics` scrapes stay anonymous even when JWT is enabled;
  see [diagnostics — Loopback trust](diagnostics.md#metrics-loopback-trust).
- Operational routes (`/health`, `/metrics`) are served only on the primary HTTPS listener (HTTPS HTTP/1.1 and HTTP/2).

If failures are isolated to owner-routing paths, compare owner lookup results with the configured peer set and the
node's local ring view.

Ownership mismatch signals:

- Normal runtime local physical mutations are protected after owner routing by `OwnershipGuardCacheDecorator<T>`.
- A mismatch means a mutation reached the owner-local physical path on a node that is not the current owner for that
  cache/key route. Treat this as stale routing, membership divergence, or an internal composition bug.
- The mutation fails before journal append, local memory mutation, memory accounting, and idempotency outcome updates.
- Recovery replay bypasses this guard intentionally because it rebuilds trusted node-local persisted state.

Ring mismatch signals:

- A ring mismatch means two nodes were started with different `ClusterId`, `VirtualNodes`, or peer `NodeId` lists, so they disagree on key owners.
- The first forwarded call between them is refused before it touches any idempotency store. Clients see `Unavailable` and the trailer `squirix-error-code`
  with `ring-mismatch`; later operations on the fenced nodes see `ring-fenced`.
- A missing fingerprint means the peer runs a version without ring agreement: a rolling upgrade to this version fences the upgraded nodes, so upgrade RF=1 clusters
  with a full stop and start.
- `/health/ready` answers `503` with the body `Unhealthy` on every fenced node. The peer name is in the `ring_agreement` health-check log entry (repeated at `Error` on each probe)
  and in the node `Error` log with both ring fingerprints, not in the HTTP body.
- Align the configuration on all nodes and restart the misconfigured nodes and every node that reports the mismatch. A fenced node stays refused until restart.
  See [clustering.md](clustering.md#ring-agreement).

## Memory pressure

Use `/health/ready/details` (`memoryPressure`) and the `Squirix` meter instruments documented in
[configuration.md#memory-pressure-squirixsettingsjson](configuration.md#memory-pressure-squirixsettingsjson).
Growing writes rejected under critical memory pressure fail before journal append.

Alerting guidance:

- **High** (`memoryPressure.state == "high"` or metric `squirix_memory_pressure_state` with `state="high"`): plan
  capacity — trending estimated bytes toward the configured limit. No automatic host readiness failure.
- **Critical** (`state == "critical"`): treat as imminent admission pressure. Expect growing writes to fail with
  documented `MEMORY_PRESSURE` / `ResourceExhausted` signals; monitor
  `rejectedWriteCount` and `squirix_memory_rejections_total`. Journal and snapshots remain **durability** tools — not an
  overflow tier for RAM pressure.
- **Cardinality:** do not add raw cache names, keys, value previews, serialized payloads, or exception messages as
  metric labels or trace tags. Generic logical cache operation metrics are owned by `MetricsCacheDecorator<T>` and use
  bounded `operation` / `result` labels on the public HTTP `/metrics` export (`cache` is recorded on the meter but
  stripped before HTTP scrape). Logical operation spans are owned by `TracingCacheDecorator<T>` and use
  bounded `cache.operation` / `cache.result` / `squirix.node_id` tags only. Memory-pressure metrics remain owned by the
  memory-pressure subsystem, and journal/snapshot/compaction metrics and spans remain storage-owned.

Kubernetes / containers:

- The default **`/health/ready`** probe does **not** fail solely because memory pressure is high or critical
  (compatibility with existing deployments). Use **`/health/ready/details`** or operator checks when you
  need memory pressure visibility, or scrape **`/metrics`** / OpenTelemetry / `MeterListener` exporters for gauges and
  counters.
- Size pod memory limits and `MaxEstimatedCacheBytes` together; critical pressure is a **policy** signal, not a
  substitute for correct RAM limits.

## Journal disk quota

Persistent nodes (`--persist` / `UsePersistence()`) enforce an on-disk journal total size cap via
`JournalMaxTotalBytesMb` (default 2048 MiB). Segment count and per-segment size caps also apply; see
[configuration.md](configuration.md).

Use `/health/ready/details` (`journalDisk`):

| Field | Meaning |
| --- | --- |
| `state` | `normal`, `high` (≥ 80% of max), or `critical` (at hard limit) |
| `maxBytes` | Configured hard cap |
| `usedBytes` | Current on-disk journal total |
| `highWaterBytes` | Soft mark at 80% of `maxBytes` |
| `writeRejectionActive` | `true` when `usedBytes >= maxBytes` |

Behavior:

- Soft high-water (`high`) is observability only for pressure state — writes that still fit under `maxBytes` continue.
- Durable writes are rejected when `usedBytes + appendBytes > maxBytes`, including while `state` remains `high`.
- Hard limit (`critical`) is the observed state at the configured cap (`usedBytes >= maxBytes`). Rejected durable writes
  fail with stable `JOURNAL_DISK_QUOTA` (gRPC `ResourceExhausted`). The process does not crash; `/health/ready`
  stays healthy so operators can still scrape details and run cleanup.
- This quota covers **journal segments only**, not snapshots or manifest files under the data directory.

Operator actions when approaching or at quota:

1. Confirm `journalDisk` and journal backlog on `/health/ready/details`.
2. Trigger or wait for snapshot + journal compaction/retention so obsolete segments can be removed (see
   [storage-maintenance.md](storage-maintenance.md)).
3. If legitimate working set needs more journal headroom, raise `JournalMaxTotalBytesMb` on the host's
   `PersistenceOptions` (v0.1 public hosting uses the 2048 MiB default and does not read this from
   `Squirix.settings.json`) and related segment caps, then restart after confirming disk capacity.
4. Clients should treat `JOURNAL_DISK_QUOTA` like other capacity rejections — back off and retry after operators reclaim
   space or raise the limit.

## Runtime backpressure

Backpressure is distinct from memory pressure. It protects hosted runtime cache operations from overload through
concurrency limits, bounded queues, slowdown, and optional rate limits. A backpressure rejection happens before logical
cache operations enter memory admission or clustered/local paths, so rejected writes do not append journal records,
mutate local memory, update memory accounting, or record idempotency outcomes.

Runtime placement: gRPC adapters keep transport-level protections such as auth, request size limits, serialization
limits, deadlines, cancellation, and server/connection protection. Logical cache-operation backpressure applies after
validation and before memory admission. Reads and writes share this policy across gRPC and in-process calls.
Treat runtime backpressure as overload protection; memory pressure remains capacity admission based on estimated cache
working-set size.

Tune limits through `Squirix:Cluster:Backpressure` or `SquirixServerOptions.Backpressure`; a restart applies the change.
Per-client and rate limits are off by default. Every rejection increments `squirix_backpressure_reject_total` with a
`reason` label (rate-limit rejections also increment `squirix_backpressure_rate_limit_reject_total`):

- `queue_full`: all `MaxInFlight` slots are taken and the node queue holds `MaxQueue` requests.
- `queue_wait_timeout`: the request waited `MaxQueueWait` without getting a slot.
- `node_rate_limit`: the node rate limit is spent.
- `client_rate_limit`: the caller's rate limit is spent.
- `client_concurrency_limit`: the caller has `PerClientMaxInFlight` requests admitted or queued.
- `client_queue_full`: the caller is at `PerClientMaxInFlight` and its queue allowance is used up; usually when `MaxQueue` is 0,
  but concurrent over-limit requests from one caller can also hit it.
- `forwarded_no_slot`: a request forwarded from another node found no free slot on this node (the owner); it is refused
  at once, never queued, and the client retries it. A rising count can also mean requests are queued locally on the owner,
  since a forwarded request never overtakes them.
- `gate_disposed`: the node was shutting down while the request was being admitted or waited.

Raise a limit only after checking that the node, not one noisy caller, is saturated. A request forwarded between nodes is
counted against the caller on the entry node only, where it also waits in the queue. The owner treats it as a trusted
internal owner RPC (client id `internal`, peer mTLS on the internal listener): it applies the node rate limit and admits it
only to a free slot, with no per-client limits, slowdown or queue. A rising `forwarded_no_slot` count on a node means it
owns more load than its `MaxInFlight` serves; queue metrics (`squirix_backpressure_queue_wait_seconds`, `squirix_backpressure_queue_timeouts_total`) on that node no
longer include forwarded requests.

Per-client concurrency and rate limits isolate callers by backpressure client id: JWT `sub` / `NameIdentifier` when the
request is authenticated (`jwt:{subject}`), otherwise the ASP.NET Core connection id (`conn:{id}`). Callers without an
`HttpContext` (in-process paths) share the `runtime` bucket — see [configuration.md](configuration.md#backpressure).
Do not expect anonymous loopback clients on distinct TCP connections to share one JWT principal bucket; they are
isolated by connection unless they present the same subject.

## Backup

Back up the full persistence set for a node:

- journal segments
- snapshot files
- manifest files
- Node configuration
- Serializer configuration and package versions

Recommended flow:

1. Drain or stop client traffic to the node.
2. Wait for in-flight writes to complete or fail.
3. Stop the node process.
4. Copy the full persistence directory to a separate location.
5. Verify the backup contains journal, snapshots, and manifest files from the same point in time.
6. Start the node only after the backup copy is complete.

Do not copy only snapshots without the corresponding journal.

## Snapshot artifacts and memory pressure

- **Background snapshots** are skipped while memory pressure is **critical**. Operational snapshot requests are not
  gated the same way.
- **Partial writes:** snapshot creation uses a `.tmp` file that is deleted if the write or rename fails. Orphan `.tmp`
  files are not referenced by the manifest and are safe to delete during maintenance if a process crashed mid-write.
- **Manifests** are updated only after a snapshot file is successfully written and moved into place.

## Restore

Restore only from a backup produced from the same node identity and compatible serializer configuration unless release
notes document a migration path.

1. Stop the node process.
2. Move the current data directory aside.
3. Copy the backup data directory into place.
4. Confirm file permissions allow read/write access.
5. Start the node with the same serializer and persistence settings used by the backup.
6. Check readiness and recovery logs.
7. Run a small read validation for known keys.

If recovery fails, stop the node and preserve both the failed restore directory and logs for analysis.

## Recovery

Recovery should be deterministic. Do not manually delete journal or manifest files unless a documented repair workflow
says it is safe.

Snapshot recovery is staged. A node publishes snapshot cache entries and retained idempotency records only after the
whole snapshot validates. If validation fails, the node discards the snapshot for that startup and replays journal from
clean recovery state without applying the snapshot watermark.

Recovery triage:

1. Capture the error, recovery logs, manifest contents, and journal segment list.
2. Check whether the active serializer can read persisted payloads.
3. Check whether the node was upgraded or downgraded before the failure.
4. Validate the copied data directory with offline maintenance tooling before changing production files.
5. Apply repair output to production data only after the copied-directory result is understood.

For corruption suspicion, prefer restoring from a known-good backup over manual file edits.

## Activated topology stamp

The first start of an RF>1 node with persistence enabled writes `topology.stamp` into the data directory. It records
the configuration generation, the replica count, and the topology fingerprint (cluster id, replica count, virtual
nodes, configuration generation, peer set, minimum cluster package version, and replication policy constants). Every
later RF>1 start compares the configured topology with the stamp and refuses startup on any difference. RF=1 nodes
never write it, and an RF=1 start with persistence refuses a data directory that carries one. See
[configuration.md](configuration.md#activated-topology-stamp-topologystamp) for the fields and the exact startup
errors.

Changing the activated topology of an existing data directory is not supported in this release, and neither is moving
existing RF=1 data to RF>1 or starting an RF>1 data directory as RF=1. When startup refuses:

1. Read which field the error names: generation, replica count, or topology fingerprint. An RF=1 start on a stamped
   directory names the replica count and generation the directory was activated with.
2. If the change was not intended, restore the settings and package version the directory was activated with.
3. If the change is intended, start the node on an empty data directory. For RF=1 data, either keep the node at RF=1
   or start RF>1 on an empty data directory and migrate the data at the application level. For an RF>1 directory,
   either start the node with the activated replica count or start RF=1 on an empty data directory.
4. Do not edit or delete `topology.stamp` to force a start: the replica group state in the directory still belongs to
   the activated topology.

`squirix-server doctor` reads the stamp and the replica group metadata without starting the node. With persistence
enabled, it inspects the data directory the node would start on: `--data-dir` / `DataDirectory` when set, otherwise the
default `{LocalApplicationData}/squirix/{ClusterId}/{NodeId}` (`{SQUIRIX_TEST_ROOT}/{ClusterId}/{NodeId}` when that
test-only variable is set), reported as `Persistence: enabled (data dir: {path}, default)`. It prints:

- `topology fingerprint: {hex}` and `configuration generation: {n}` for the configured topology;
- `topology stamp: not activated` when the directory has no stamp (an RF=1 or never-started node);
- `topology stamp: MISSING while the data directory holds durable cache journal state (last used by an RF=1 node;
  moving RF=1 data to RF>1 is not supported, so RF>1 startup is refused)` instead when RF>1 is configured and the
  directory holds cache journal segments but no stamp, the same condition under which startup refuses it;
- `topology stamp: UNREADABLE ({reason})` when the stamp is corrupt or has an unsupported format;
- `topology stamp: MISMATCH (activated for replica count {n}, generation {g}; starting it as RF=1 is not supported, so
  RF=1 startup is refused)` when RF=1 is configured and the directory carries a stamp, the same condition under which
  RF=1 startup refuses it;
- otherwise, under an RF>1 configuration, one line each for `generation`, `replica count`, and `fingerprint`, either
  `match` or `MISMATCH (stamped {value}, configured {value})`;
- one line per configured peer group: `group '{id}': no durable state`,
  `group '{id}': metadata UNREADABLE (checksum or format mismatch)`, or
  `group '{id}': term {t} commit {c} applied {a} apply-lag {n} fingerprint match|MISMATCH generation match|MISMATCH`.

When any line reports `MISMATCH`, `UNREADABLE`, or `MISSING`, doctor prints the whole report, writes
`[Squirix.Server] Error: Durable replica state in the data directory does not match the configured topology; see the
MISMATCH, UNREADABLE, or MISSING lines above.` to standard error, and exits with code 1, the same code as a failed
`validate-config`. Otherwise, including `topology stamp: not activated`, it exits with code 0. Run it with the settings
the node will start with, including the same internode mTLS environment, so the configured fingerprint matches what
startup computes. When no data directory is set and the default one cannot be resolved because local application data
is not available, doctor prints `Persistence: enabled (data dir: unavailable; {reason})` and exits with code 1, because
startup fails the same way; set `--data-dir` or `DataDirectory`.

## Upgrade

Before upgrade:

1. Confirm whether rolling upgrade is supported for the source and target versions.
2. Back up each node data directory.
3. Validate recovery from a backup copy with the target version.
4. Run `tools/internal/sqr-release-validate.cs` locally for the target commit before tagging.
5. Validate security posture after startup:
    - Non-loopback cache gRPC rejects missing/invalid JWT when auth is enabled.
    - Remote `/metrics` and `/health/ready/details` require the same JWT bearer credentials.
    - Operational routes are HTTPS-only on the primary listener.
6. For RF>1 data directories, run `squirix-server doctor` from the target version against a backup copy of the data directory. A
   `topology stamp: fingerprint MISMATCH` line (doctor exits with code 1) means the target version refuses to start on
   that directory; see [Activated topology stamp](#activated-topology-stamp).

Compatible rolling upgrade:

1. Upgrade one non-critical node.
2. Confirm readiness, journal backlog, snapshot age, compaction state, and client pool state.
3. Watch logs for serializer, journal, gRPC, and backpressure errors.
4. Continue one node at a time.
5. Keep old binaries and backups until all nodes are validated.

Unsupported or incompatible upgrade:

1. Stop the cluster.
2. Back up all nodes.
3. Run the documented migration or validation workflow.
4. Start all nodes on the target version.
5. Validate recovery and representative reads/writes before reopening traffic.

## Escalation checklist

Keep this information with any incident or bug report:

- Squirix version and commit
- Operating system and .NET SDK/runtime version
- Node id, peer list, and data directory layout
- Serializer configuration
- Relevant config values from [configuration.md](configuration.md)
- Health/readiness payloads
- journal/snapshot/manifest file list
- Logs with correlation ids for failed operations
- Steps already attempted
