# Operations

Build, test, limitations, and roadmap for squirix 0.1.0 preview.

## Build and test

Prerequisite: .NET SDK as pinned in [`global.json`](../global.json) (minimum **10.0.203**).

```bash
dotnet restore squirix.slnx
dotnet build squirix.slnx --configuration Release
dotnet dev-certs https --trust
dotnet test squirix.slnx --configuration Release --no-build
```

## Current limitations

- **Preview stability** — API, wire format, and on-disk layouts may change during 0.x
- **Performance** — characteristics are not final; do not benchmark against mature cache products yet
- **Automatic failover is opt-in** — RF=1 durability is per node; RF>=3 elects a new leader only with
  [automatic failover and quorum reads](configuration.md#automatic-failover-and-quorum-reads) on
- **Static topology** — peers are configured explicitly; dynamic membership is future work
- **Replica group start and lost disks (RF>1)** — a replica group of a new cluster starts once every member of it is up. A
  member that starts on an empty data directory while its group runs holds an unseeded copy that grants no vote and takes
  no entries until the leader of the group repairs it by a state transfer, with no operator action, once the other
  members confirm no higher term. Two wiped members of three, or three of five, are not repaired, and the cluster is
  recreated. Do not restore the data directory of a replicated node from an older backup. See
  [starting a replica group](clustering.md#starting-a-replica-group)
- **Single-key operations** — cross-key or multi-node atomicity is out of scope for v0.1
- **Narrow client API** — basic KV + expiration; no batch, scan, watch, counters, or tag invalidation yet
- **Replica group log compaction (RF>1 opt-in)** — each node compacts the group logs it leads and its copies of the
  groups it follows. A follower that is down, or that missed an entry, blocks compaction of the leader's group log
  until it is verified again, so that log keeps growing meanwhile, up to `ReplicaLogRetentionMaxMb` (default 1 GiB).
  Past it the leader compacts without that follower. A follower that needs entries the leader already compacted is
  repaired by a state transfer: the leader sends it the data of the group and the follower catches up from the log, with
  no operator action. A node that lost its disk is repaired the same way (see the lost disks item above). All
  nodes of a replica group must run the same replica log codec version. See
  [replication consensus](architecture/replication-consensus.md#group-log-retention-and-compaction)
- **Journal disk quota is per node** — `JournalMaxTotalBytesMb` (host default 2048 MiB; not a settings JSON section
  in v0.1 public hosting) covers journal segments on that node only
- **0.x compatibility** — no promise of upgrade or on-disk persistence compatibility until 1.0.0
- **.NET 10 only** — older TFMs are intentionally out of scope for the preview line

See [consistency](consistency.md) and [operational runbook](operational-runbook.md) before any production-like
evaluation.

## Roadmap (directional)

These are **not commitments** — they reflect likely next areas based on current gaps:

- Harden durability, recovery, and compaction semantics across preview releases
- Expand operational tooling and observability defaults
- Evaluate additional client operations (batch, scan, invalidation) based on feedback
- Explore dynamic cluster membership and replication models after the 0.1 foundation stabilizes
- Performance tuning and benchmark baselines once API and durability contracts settle

## Upgrades and recovery

squirix **0.x** releases are preview releases. Treat every upgrade as potentially breaking unless the target release
explicitly documents otherwise.

Upgrade, backup, restore, and recovery workflows: [operational-runbook.md](operational-runbook.md).

## Contributing and feedback

Early feedback is especially valuable:

- [Open an issue](https://github.com/squirix/squirix/issues) for bugs, API ideas, or durability questions
- [contributing.md](../contributing.md) for pull request guidelines
- Email: [admin@squirix.io](mailto:admin@squirix.io)
- Slack: [Squirix workspace](https://squirix.slack.com)

## Naming conventions

Package ids, CLI commands, and documentation naming: [naming-conventions.md](naming-conventions.md).

## Benchmarks

Internal benchmark notes (not user-facing product docs):

- [benchmarks/e2e-benchmarks.md](benchmarks/e2e-benchmarks.md)
- [benchmarks/read-path-optimization-notes.md](benchmarks/read-path-optimization-notes.md)
