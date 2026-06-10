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

```powershell
squirix-server validate-config --settings ./Squirix.settings.json --strict
```

See [containerization.md](containerization.md) for Docker Compose examples (`Cluster.Url` must match the local peer
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
- Backpressure pressure and request failures from logs
- Serializer and journal JSON codec failures
- Correlation or trace ids for failing requests
- `memoryPressure` on `/health/ready/details` (state, limits, estimated usage, entry count, rejections, whether write
  rejection is active)

Trace ownership during triage:

- Logical cache operation spans are emitted by `TracingCacheDecorator<T>` through the `Squirix` `ActivitySource`.
- Use logical spans for operation/result triage (`cache.operation`, `cache.result`, `squirix.node_id`).
- Use gRPC interceptor spans and correlation for transport-level failures.
- Use journal, snapshot, and compaction spans for storage failures.
- Do not expect logical operation spans to include raw keys, values, payloads, cache names, or exception messages.

Security checks during triage:

- Non-loopback listen URLs refuse startup without `SQUIRIX_API_KEYS` and/or JWT settings.
- Confirm auth is enabled where required for exposed interfaces.
- Verify that REST cache, gRPC cache, and remote `/metrics` scrapes are challenged consistently for missing/invalid
  credentials (`/health` remains anonymous).
- Operational routes (`/health`, `/metrics`) are served only on the primary HTTPS listener (HTTPS HTTP/1.1 and HTTP/2).

If failures are isolated to owner-routing paths, compare owner lookup results with the configured peer set and the
node's local ring view.

Ownership mismatch signals:

- Normal runtime local physical mutations are protected after owner routing by `OwnershipGuardCacheDecorator<T>`.
- A mismatch means a mutation reached the owner-local physical path on a node that is not the current owner for that
  cache/key route. Treat this as stale routing, membership divergence, or an internal composition bug.
- The mutation fails before journal append, local memory mutation, memory accounting, and idempotency outcome updates.
- Recovery replay bypasses this guard intentionally because it rebuilds trusted node-local persisted state.

## Memory pressure

Use `/health/ready/details` (`memoryPressure`) and the `Squirix` meter instruments documented in
[configuration.md#memory-pressure-squirixsettingsjson](configuration.md#memory-pressure-squirixsettingsjson).
Growing writes rejected under critical memory pressure fail before journal append.

Alerting guidance:

- **High** (`memoryPressure.state == "high"` or metric `squirix_memory_pressure_state` with `state="high"`): plan
  capacity — trending estimated bytes toward the configured limit. No automatic host readiness failure.
- **Critical** (`state == "critical"`): treat as imminent admission pressure. When `writeRejectionActive` is true,
  expect growing writes to fail with documented `MEMORY_PRESSURE` / `ResourceExhausted` signals; monitor
  `rejectedWriteCount` and `squirix_memory_rejections_total`. journal and snapshots remain **durability** tools — not an
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

## Runtime backpressure

Backpressure is distinct from memory pressure. It protects hosted runtime cache operations from overload through
concurrency limits, bounded queues, slowdown, and optional rate limits. A backpressure rejection happens before logical
cache operations enter memory admission or clustered/local paths, so rejected writes do not append journal records,
mutate local memory, update memory accounting, or record idempotency outcomes.

Runtime placement: REST/gRPC adapters keep transport-level protections such as auth, request size limits, serialization
limits, deadlines, cancellation, and server/connection protection. Logical cache-operation backpressure applies after
validation and before memory admission. Reads and writes share this policy across REST, gRPC, and in-process calls.
Treat runtime backpressure as overload protection; memory pressure remains capacity admission based on estimated cache
working-set size.

## Backup

Back up the full persistence set for a node:

- journal segments
- Snapshot files
- Manifest files
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

- **Background snapshots** are skipped while memory pressure is **critical** (when a positive `MaxEstimatedCacheBytes`
  is configured and pressure evaluation is enabled). Operational snapshot requests are not gated the same way.
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

## Upgrade

Before upgrade:

1. Confirm whether rolling upgrade is supported for the source and target versions.
2. Back up each node data directory.
3. Validate recovery from a backup copy with the target version.
4. Run `tools/internal/sqr-release-validate.cs` locally for the target commit before tagging.
5. Validate security posture after startup:
    - REST, admin, and gRPC reject missing/invalid credentials.
    - gRPC accepts the same credential types (API key/JWT) as REST.
    - Operational routes are HTTPS-only on the primary listener.

Compatible rolling upgrade:

1. Upgrade one non-critical node.
2. Confirm readiness, journal backlog, snapshot age, compaction state, and client pool state.
3. Watch logs for serializer, journal, gRPC, REST, and backpressure errors.
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
