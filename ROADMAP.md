# Roadmap

Squirix is an experimental distributed cache and key-value runtime for .NET, with a typed client SDK and a server that
owns routing, replication, and persistence. The goal is to make shared state across .NET services practical to integrate,
understand, and operate, with explicit consistency and durability guarantees.

This roadmap covers the public `squirix`, `squirix.server`, and `squirix.server.tool` NuGet packages.
It describes priorities rather than release dates or delivery commitments. Scope and ordering may change with evidence
from testing, contributor feedback, and application requirements.

## Existing foundation

- Typed single-key cache operations over HTTPS gRPC, with pluggable serialization and explicit expiration semantics.
- Standalone server tooling and hosting in an ASP.NET Core application.
- Static key routing and replicated groups, with opt-in automatic failover and quorum reads for replica counts of at least three.
- Persistence through journals and snapshots, plus health endpoints and metrics.

See the [API reference](docs/api.md), [consistency guarantees](docs/consistency.md), and
[persistence documentation](docs/persistence.md) for behavior and limitations. The preview is not production-ready.

## Near-term priorities

### Stabilize the client and server contract

- Refine API behavior, expiration semantics, cancellation, retries, and error reporting using feedback from real applications.
- Keep supported configurations, compatibility boundaries, and breaking changes explicit in documentation and release notes.
- Make installation, server configuration, and the first client connection reproducible for external users.

### Strengthen correctness and recovery evidence

- Expand deterministic verification of replication, ordering, and behavior during partial failures.
- Extend recovery coverage across process crashes, restarts, interrupted writes, snapshots, and retained journal data.
- Keep documented guarantees aligned with executable checks and distinguish verified behavior from unsupported scenarios.

### Improve operations and troubleshooting

- Make health, readiness, diagnostics, and metrics explain the state of the server and replicated groups.
- Improve guidance for authentication, transport security, resource limits, storage management, and recovery.
- Document supported deployment and upgrade procedures together with their restrictions.

### Measure performance in representative workloads

- Extend repeatable client/server benchmarks for throughput, latency, allocations, and persistence overhead.
- Evaluate payload sizes, expiration patterns, concurrency, and replication settings rather than relying on one headline number.
- Optimize measured bottlenecks while preserving consistency and recovery guarantees.

## Longer-term direction

Candidate directions include changing cluster topology and moving data between nodes beyond today's static configuration.
These require use-case feedback and a design that preserves consistency and recoverability before they can become
scheduled work. Automatic rebalancing and transactions across owners are not currently supported.

Further API additions will be considered against concrete application requirements. The [API reference](docs/api.md)
defines the current public surface; [related projects](docs/related-projects.md) explains its differences from other systems.

## Contributing to the direction

Propose a change through [GitHub Issues](https://github.com/squirix/squirix/issues).
Describe the application problem, the current workaround, the guarantees needed, and the deployment constraints.
Concrete use cases and reproducible reports help prioritize work; see [contributing.md](contributing.md) for participation.
