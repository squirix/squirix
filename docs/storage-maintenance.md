# Storage maintenance

Squirix performs snapshots and journal retention online. Operators can validate configuration and inspect startup
diagnostics with the public CLI; the CLI does not provide manual journal compaction or storage repair.

## Automatic journal retention

After a snapshot is published, retention deletes the journal segments and older snapshots it covers. The journal quota
then counts the retained tail, not all writes since the node first started. See [Persistence](persistence.md).

Retention failures appear in `/health/ready/details` under `retentionCleanup`. Start with the reported error and the
node logs rather than deleting files to free space. See [Journal disk quota](operational-runbook.md#journal-disk-quota).

## Inspect a startup failure

1. Capture the startup error, logs, package version, and effective node settings.
2. Stop the node and copy its complete data directory to a separate location. Keep the original unchanged.
3. Use the matching node configuration when inspecting that copy. Do not start a copied RF>1 directory as a duplicate
   node or restore it into a live cluster.

For example, if the stopped node's copied data is in `data-copy` and its settings are in `Squirix.settings.json` in the
current directory, run:

```bash
squirix-server validate-config --settings Squirix.settings.json --strict
squirix-server doctor --settings Squirix.settings.json --persist --data-dir data-copy
```

Check that `data-copy` is the intended existing copy before running the second command. `doctor` can create a missing
directory and writes, then deletes, a small access-probe file; it is not a read-only inspection command. Inspect the
reported data path and access results, not just the exit code. An access-probe failure can be printed without making
the command fail.

`doctor` reports configuration, port availability, data-directory access, and replica-state diagnostics. It does not
validate every journal frame or snapshot payload, repair corruption, or prove that a restore is safe. A successful
configuration check likewise does not prove the data directory can recover.

## Recovery boundaries

There are no public `inspect`, `compact`, or `repair` CLI commands. Do not reconstruct `man-current`, edit persisted
terms or votes, or delete journal segments and snapshots manually. Preserve the failed directory and follow the
[recovery runbook](operational-runbook.md#recovery) and the [RF=1 restore procedure](operational-runbook.md#restore).

For RF>1, an older backup must not be used to roll a node back into a live cluster. See
[replicated-node recovery constraints](clustering.md#starting-a-replica-group).

## Data directory contents

See [On-disk layout](persistence.md#on-disk-layout) for the file inventory and
[Data directory lock](persistence.md#data-directory-lock) for `node.lock`.
Follow the [backup procedure](operational-runbook.md#backup) to stop the node and copy its complete data directory
with its matching configuration and package version.

Format references: [journal](journal-binary-format.md), [snapshot](snapshot-format.md), and
[manifest](manifest-format.md).
