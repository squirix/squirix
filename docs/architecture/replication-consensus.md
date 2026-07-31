# ADR: Replication Consensus Protocol

## Status

Accepted for M8-01. Product election and local promotion remain forbidden until this ADR is
merged and the protocol-model explorer reports no unexpected counterexamples within documented bounds.

## Context

Squirix introduces replica sets (RF 1–5) with majority durability and leader authority. A prior informal idea
of “log shipping plus a local term bump, not Raft” is unsafe: a lone node can inflate a term and create dual leaders or
lose committed entries.

M8-01 therefore freezes a Raft-safety-equivalent custom protocol with static membership, an executable C# state model,
and an NDepend namespace DAG gate before any product election code lands.

## Decision

### Protocol shape

- Custom protocol with **static membership** per replica group.
- Safety equivalent to Raft for **leader election**, **log commit**, and **leadership change**.
- `MaxReplicaCount = 5` (hard upper bound for RF).
- No time-based leader lease. Authority is always majority-confirmed in the **current term**.

### Persistent election state

Each group stores durable `current_term` and at most one `voted_for` per term. A positive vote response is persisted
before it is sent. A candidate wins only with a majority of the configured static membership; votes may come only from
ready members. Readiness never changes the quorum denominator.

### Current-term commit before serving

After election, a leader appends and majority-commits a **current-term** entry (noop or real) before treating old-term
majority replication as a new commit that may be served. This preserves Raft’s “leader completeness” / current-term
commit rule.

### Quorum reads (ReadIndex equivalent)

For each linearizable/current read under RF>1:

1. Confirm leadership with majority replies in the current term.
2. Take `read_index >= commit_index`.
3. Wait until local `applied_index >= read_index`.
4. Only then return the value.

Minority partitions and former leaders without majority must return `Unavailable` / `stale-term`, never a stale value
as current.

### Executable model (isolation)

- Model project: `src/squirix.protocol-model` (`Squirix.ProtocolModel`), `net10.0` only; not a shipped product package.
- Tests: `tests/squirix.protocol-model/squirix.protocol-model.tests` — reference the model only (not product assemblies).
- No `ProjectReference` from model → product or product → model.
- Layers: immutable canonical state → pure transitions → deterministic BFS explorer → safety invariants + minimal
  counterexample traces.

Absence of a counterexample means only that the **finite** profile bounds were clean — not a mathematical proof for
unbounded systems.

### Search bounds (full profile)

Documented explorer bounds for M8-01 full profile:

- Elections RF=2/3/4/5 up to three terms.
- Commit RF=2/3/4/5 up to three log entries and four in-flight messages.
- Quorum read RF=2/3/4/5 up to two log entries and one pending read.
- Crash/restart points before and after durable writes of term, vote, log, and `commit_index`.
- Network: loss, duplicate, reorder; one-way partition; reconnect.
- Per sub-profile BFS cap: **`MaxStates = 50_000`** (symmetric reduction on). Hitting the cap without a safety
  violation is within these documented bounds: `summary.json` reports `fixedPointReached=false` and the CLI exits 0.
  It is residual risk inside the finite envelope, not a counterexample.

Residual risk: larger RF, deeper logs, richer failure interleavings, or states beyond `MaxStates` are not explored here.

### Negative explorer modes (M8-01)

Required broken-rule fixtures (must produce expected counterexamples):

- `vote` — grant votes without up-to-date log checks.
- `current-term-commit` — commit old-term entries without a current-term commit.
- `read-index` — serve reads without majority confirm and/or before `applied_index >= read_index`.

Additional negative modes (local term inflation, commit-across-gap) are optional follow-ups and do not block M8-01.

### Mapping model → future product components

| Model concept | Future product home |
| --- | --- |
| Term / vote persistence | `Squirix.Server.Cluster.Replication` + durable group state via `Squirix.Server.Storage.Replication` |
| AppendEntries / vote / ReadIndex RPCs | Server-only protobuf under `Squirix.Server.Adapters.Grpc` (not shared `SquirixCache.proto`) |
| Log matching / catch-up | `Cluster.Replication` orchestration over `Storage.Replication` journal/snapshot ports |
| Majority commit pipeline | Durable replication pipeline (M8-07+) |
| ReadIndex wait on apply | Leader read authority path (M8-11/M8-12) |
| Topology fingerprint / generation | Placement + config (M8-02/M8-03) |

Conformance traces (`ProtocolModelConformanceTests`) compare production projections to this model in later milestones;
version fingerprints must stay aligned.

### Namespace DAG

Forbidden dependency edges (product architecture):

- Client must not depend on Server.
- Cluster → Storage only via the allowed `Cluster.Replication` → `Storage.Replication` edge (no reverse edge).
- `Cluster.Replication` must not depend on adapters, hosting, `Node.App`, cluster transport, `LocalCache`, `Errors`,
  `Utils`, or `Runtime.Invocation`.
- `Adapters.Endpoint` must not own `Cluster.Replication`.
- `Node.App` must not bypass into `Storage.Replication`.
- Product must not reference `Squirix.ProtocolModel`.

New edges require an ADR/DAG update before merge. Enforcement lives outside this document (compile-time namespace
policy and architecture tests).

### MaxReplicaCount = 5 budget

RF=5 is the product maximum. Fan-out (vote / append / ReadIndex), per-group in-memory state, and metrics cardinality
must be sized for five peers on the established internal channels. Profiles RF=2..5 in the explorer confirm safety
machinery scales across the allowed RF set; operational budgets are validated by later placement/perf milestones.

## Consequences

- Product election code and local promotion stay off until this ADR merges and model evidence is green.
- M8-09 may activate RF>1 mutations only after dependent storage/protocol milestones; quorum reads stay gated until
  M8-12 after ReadIndex traces match the model.
- RF=1 keeps single-owner behavior without elections.
- RF=2 cannot elect a replacement after losing one member (majority is two).
- RF≥3 may automatic-failover only after M8-12 proof matrix.

## Alternatives considered

| Option | Rejected because |
| --- | --- |
| Local term bump + log shipping | Dual leaders / lost commits under partitions |
| Time-based leader lease | Clock skew / false authority without quorum |
| Embed model inside `Squirix.Server` | Shared-bug risk; ambiguous isolation |
| Unbounded model checking as merge gate | Non-terminating; residual risk must stay explicit |
