# Persistence

By default, squirix server nodes run as an in-memory cache without writing journal, manifest, or snapshot files. Enable
persistence explicitly when a node should survive restarts with local durability.

Durability is **per node** — there is no replication or automatic failover in v0.1.

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

Background journal compaction rewrites tail segments covered by the latest snapshot watermark. Tune thresholds in
`Squirix.settings.json` — see [configuration](configuration.md).

Compaction state is visible on `/health/ready/details` (`compaction.*`).

## On-disk layout

When persistence is enabled, a node data directory typically contains:

- journal segment files ([binary journal format](journal-binary-format.md))
- snapshot files ([binary snapshot format](snapshot-format.md), `.bsqx`)
- manifest files and a `man-current` pointer ([manifest format](manifest-format.md))

Backups must include journal, snapshots, and manifest from the same point in time. Copying snapshots without matching
journal/manifest metadata can break recovery.

## Operator workflows

See [operational runbook](operational-runbook.md) and [storage maintenance](storage-maintenance.md) for backup, restore,
and offline repair procedures.
