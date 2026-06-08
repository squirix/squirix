# Persistence

Each squirix server node persists cache mutations locally. Durability is **per node** — there is no replication or
automatic failover in v0.1.

## Write-ahead journal

Mutations append to a per-node write-ahead log (WAL) before they are considered durable. On startup, the node replays
the journal (and latest snapshot watermark) to rebuild in-memory state.

Readiness stays unhealthy until journal recovery completes (`journal_recovery` gate). Fatal maintenance failures also
affect readiness — see [observability](observability.md).

## Snapshots

Periodic snapshots capture cache state and advance the journal watermark. Snapshot triggers run in the background while
the node serves traffic.

Readiness reports snapshot age and in-flight state on `/health/ready/details`.

## Compaction

Background journal compaction rewrites tail segments covered by the latest snapshot watermark. Tune thresholds in
`Squirix.settings.json` — see [configuration](configuration.md).

Compaction state is visible on `/health/ready/details` (`compaction.*`).

## On-disk layout

A node data directory typically contains:

- journal segment files
- snapshot files
- manifest files and a `CURRENT` pointer

Backups must include journal, snapshots, and manifest from the same point in time. Copying snapshots without matching
journal/manifest metadata can break recovery.

## Operator workflows

- Online maintenance: compaction runs automatically; monitor readiness and logs.
- Offline maintenance: stop the node, copy the data directory, then inspect/compact/repair on a copy.

Detailed procedures: [storage-maintenance.md](storage-maintenance.md), [operational-runbook.md](operational-runbook.md).

## Group commit

Journal append batching semantics for throughput tuning: [journal-group-commit.md](journal-group-commit.md).

## Limitations (v0.1)

- No cross-node replication or automatic data failover
- On-disk layouts may change during 0.x preview releases
- Offline compact/repair tooling is outside the exported product surface
