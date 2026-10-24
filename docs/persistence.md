# Persistence

By default, squirix server nodes run as an in-memory cache without writing journal, manifest, or snapshot files. Enable
persistence explicitly when a node should survive restarts with local durability.

With RF=1, durability is **per node**: no replication and no automatic failover. For RF>1 replication and the RF>=3
[automatic failover and quorum reads](configuration.md#automatic-failover-and-quorum-reads), see [configuration.md](configuration.md).

## Enable persistence

ASP.NET Core hosting:

```csharp
await builder.AddSquirixServerAsync(options =>
{
    options.NodeId = "node-a";
    options.Uri = new Uri("https://localhost:5001");
    options.UsePersistence("./data");
});
```

Standalone CLI:

```bash
squirix-server run --persist --data-dir ./data
```

`DataDirectory` / `--data-dir` only applies when persistence is enabled (`UsePersistence()` or `--persist`).

## Write-ahead journal

When persistence is enabled, mutations append to a per-node journal before they are considered durable. On
startup, the node replays the journal (and latest snapshot watermark) to rebuild in-memory state.

Readiness stays unhealthy until journal recovery completes (`journal_recovery` gate). Fatal maintenance failures also
affect readiness — see [observability](observability.md).

Journal on-disk growth is bounded by `JournalMaxTotalBytesMb` (default 2048 MiB on host `PersistenceOptions`; not a
`Squirix.settings.json` section in v0.1 public hosting). Oversize durable appends fail with `JOURNAL_DISK_QUOTA`
while `/health/ready` stays healthy and `/health/ready/details` exposes `journalDisk` pressure. See
[operational-runbook.md — Journal disk quota](operational-runbook.md#journal-disk-quota).

## Snapshots

Periodic snapshots capture cache state and advance the journal watermark. Snapshot triggers run in the background while
the node serves traffic.

Readiness reports snapshot age and in-flight state on `/health/ready/details`.

## Compaction

Background journal compaction rewrites tail segments covered by the latest snapshot watermark. Compaction thresholds are
host defaults in v0.1 public hosting (not a `Squirix.settings.json` section) — see [configuration](configuration.md).

Compaction stages the rewritten journal as a temp file, then commits by writing the manifest that names it, then publishes the
file under its final name and deletes the old segments. If the node stops between the manifest write and the publish, startup
finishes the publish. Startup refuses a data directory where a journal segment above the manifest's current segment holds frames.

Compaction state is visible on `/health/ready/details` (`compaction.*`).

## On-disk layout

When persistence is enabled, a node data directory typically contains:

- journal segment files ([binary journal format](journal-binary-format.md))
- snapshot files ([binary snapshot format](snapshot-format.md), `.bsqx`)
- the `man-current` manifest ([manifest format](manifest-format.md))
- `node.lock`, the empty lock file described below

Backups must include journal, snapshots, and manifest from the same point in time. Copying snapshots without matching
journal/manifest metadata can break recovery.

## Data directory lock

A running node holds an exclusive lock on `node.lock` in its data directory for its whole lifetime. A second process, or a
second host in the same process, started on the same directory refuses to start with an error naming the directory.
The operating system releases the lock when the process exits, so a crash needs no manual cleanup; leave `node.lock` in place
and do not delete it from a running node. Network file systems that do not support file locking, and the
`DOTNET_SYSTEM_IO_DISABLEFILELOCKING` environment switch, disable this protection. On Linux NFS, `flock` is emulated with per-process `fcntl` locks, so only other processes are detected,
not a second host in the same process.

## Operator workflows

See [operational runbook](operational-runbook.md) and [storage maintenance](storage-maintenance.md) for backup, restore,
and offline repair procedures.
