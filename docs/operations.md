# Operations

Build, test, limitations, and roadmap for squirix 0.1.0 preview.

## Build and test

Prerequisite: .NET SDK as pinned in [`global.json`](../global.json) (minimum **10.0.203**).

```powershell
dotnet restore squirix.slnx
dotnet build squirix.slnx --configuration Release
dotnet dev-certs https --trust
dotnet test squirix.slnx --configuration Release --no-build
```

## Current limitations

- **Preview stability** — API, wire format, and on-disk layouts may change during 0.x
- **Performance** — characteristics are not final; do not benchmark against mature cache products yet
- **No replication or automatic failover** — durability is per node
- **Static topology** — peers are configured explicitly; dynamic membership is future work
- **Single-key operations** — cross-key or multi-node atomicity is out of scope for v0.1
- **Narrow client API** — basic KV + expiration; no batch, scan, watch, counters, or tag invalidation yet
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
