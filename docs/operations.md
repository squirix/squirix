# Operations

Build, test, limitations, and roadmap for squirix 0.1.0 preview.

## Build and test

Install the .NET 11 SDK selected by [`global.json`](../global.json) and the .NET and ASP.NET Core runtimes for the target you
want to run. Run the commands below from the repository root. The solution builds for `net10.0` by default.

```bash
dotnet restore squirix.slnx
dotnet build squirix.slnx --configuration Release
dotnet dev-certs https --trust
dotnet test --solution squirix.slnx --configuration Release --no-build
```

To build and test on .NET 11:

```bash
dotnet build squirix.slnx --configuration Release -p:SquirixTargetFramework=net11.0
dotnet test --solution squirix.slnx --configuration Release --framework net11.0 --no-build -p:SquirixTargetFramework=net11.0
```

## Release packages

The release workflow packs the client, server, caching adapter, and CLI with `net10.0` and `net11.0` in each NuGet package.
Framework-specific Microsoft packages use the release line for the selected target. Shared dependencies such as gRPC and Protobuf use the same versions on both.
It tests consumption from a local feed and installs and runs the CLI for each framework before publication.
Use a new package version for the first release that includes both targets.
Install both sets of runtimes to run the same validation locally:

```bash
dotnet run --file tools/internal/sqr-release-validate.cs --
```

The standalone ZIP and Docker images continue to use .NET 10. For the CLI package, use `dotnet tool install --framework net11.0`
with the normal package ID and version arguments to select the .NET 11 build explicitly.

## Current limitations

- **Preview stability** — API, wire format, and on-disk layouts may change during 0.x
- **Performance** — characteristics are not final; do not benchmark against mature cache products yet
- **Automatic failover is opt-in** — RF=1 durability is per node; RF>=3 elects a new leader only with
  [automatic failover and quorum reads](configuration.md#automatic-failover-and-quorum-reads) on
- **Static topology** — peers are configured explicitly; dynamic membership is future work
- **Replica group start and lost disks (RF>1)** — every member must start before a new group is ready.
  An empty copy can rejoin a running group only after repair. Never restore an older data directory into a live
  replicated cluster. See [starting a replica group](clustering.md#starting-a-replica-group)
- **Single-key operations** — cross-key or multi-node atomicity is out of scope for v0.1
- **Narrow `ICache<T>` API** — basic KV and expiration. The SDK has no batch, scan, watch, counters, or
  tag-invalidation methods. Microsoft HybridCache provides its own [tag invalidation](distributed-cache.md#hybridcache-across-processes)
- **Replica group log compaction (RF>1 opt-in)** — a lagging or unavailable follower can delay compaction.
  See [retention limits and compaction](architecture/replication-consensus.md#group-log-retention-and-compaction)
  and the [state transfer runbook](operational-runbook.md#state-transfer)
- **Journal disk quota is per node** — `JournalMaxTotalBytesMb` (host default 2048 MiB; not a settings JSON section
  in v0.1 public hosting) covers journal segments on that node only
- **0.x compatibility** — no promise of upgrade or on-disk persistence compatibility until 1.0.0
- **Runtime support** — `net10.0` and `net11.0`

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
