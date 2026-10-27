# Storage maintenance

For a persistence overview, see [persistence.md](persistence.md). This document covers journal maintenance built into
`Squirix.Server` and offline operator workflows referenced from the runbook.

## Journal retention

The node takes snapshots while online. After each snapshot is published, retention deletes the journal segments and older
snapshot files it covers, so the journal quota counts only the tail after the latest snapshot. See [persistence](persistence.md).

Readiness reports retention failures on `/health/ready/details` (`retentionCleanup`).

## Offline maintenance

When a node cannot start or operators need to inspect on-disk layout:

1. Stop the node process.
2. Copy the full data directory to a safe location.
3. Work on the copy first. See [operational runbook](operational-runbook.md).

Do not delete journal segments, snapshots, or `man-current` manually unless a documented repair workflow
confirms it is safe.

Typical offline workflows (semantics only):

- **Inspect** — summarize whether the `man-current` manifest is readable, list journal segment and snapshot indices,
  and flag obvious layout issues.
- **Compact** — rewrite journal history using the latest manifest snapshot watermark (node must stay stopped).
- **Repair** — rebuild manifest metadata conservatively when `man-current` is inconsistent so recovery
  can proceed.

These workflows require dedicated offline tooling and are outside the v0.1 exported `Squirix` / `Squirix.Server` product
surface.

## Safety notes

- Invoke mutating offline maintenance only when the node is fully stopped.
- `repair` is intentionally conservative. If manifest metadata is lost and journal still exists, a repaired manifest
  prefers journal-only recovery rather than guessing snapshot watermarks.
- Take a backup before compact or repair on important datasets.

## On-disk layout (operator reference)

A node data directory typically contains:

- journal segment files
- snapshot files
- the `man-current` manifest

Backups must include journal, snapshots, and manifest from the same point in time. Copying snapshots without the
matching journal is unsafe.
