# ADR: Replication Consensus Protocol

## Status

Accepted for M8-01. Product election and local promotion remain forbidden until this ADR is merged and the
protocol-model explorer reports no unexpected counterexamples within documented bounds.

## Context

Squirix introduces replica sets (RF 1–5) with majority durability and leader authority. A prior informal idea of
“log shipping plus a local term bump, not Raft” is unsafe: a lone node can inflate a term and create dual leaders or
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

### Elected leadership behind the automatic failover switch

Automatic failover is the `AutomaticFailoverEnabled` server option
([configuration](../configuration.md#automatic-failover-and-quorum-reads)). It is off by default; with it off, the
owner of each group leads it statically in term one, exactly as before. With it on, the groups of three or more
replicas elect their leader; validation requires `QuorumReadsEnabled` with it. Groups of one or two replicas never
elect: validation refuses the switch for them, so they keep no election driver and no heartbeats, and RF=2 never
elects a replacement.

- **Election.** Each node runs one election driver per served group. A follower campaigns once neither a leader contact
  nor a granted vote happened for the election timeout plus a seeded jitter. It first runs a pre-vote for the next term,
  which changes no term anywhere; with a majority of the static membership it persists the term and its own vote through
  the same vote path a peer request takes, then asks the other members. A higher term seen in any reply is made durable
  before the node follows it.
- **Term one.** Term one belongs to the owner of a group: a node whose own group log never moved past term one leads it in
  term one at start, without votes, and no vote is ever granted for term one. The first election is for term two.
- **Leader contact.** A member refuses a pre-vote with `leader-contact` while it heard from a live leader within one
  election timeout, so a node cut off from the leader alone keeps campaigning in vain instead of deposing it.
- **Authority.** A winner appends a `leader-noop` record in its term, under the operation scope `squirix:leader-term`,
  before it probes any follower. The start stops probing once enough followers answered from their logs to form a
  majority with the leader, so a follower that never answers does not hold it for the probe timeout. It gains authority
  only once a majority committed that entry for this promotion; until then, and after any step-down, a write is refused
  before anything is appended: `Unavailable` with "Replica group has no leader with authority on this node; nothing was
  read or written.", or the stale-owner refusal naming the leader this node knows.
- **Heartbeats and step-down.** A leader sends an empty append to every idle follower each heartbeat interval. It steps
  down once fewer than a majority, itself included, answered within one election timeout, and at once on a higher term
  in any reply. A new leader gets one timeout of grace. A follower that answered the probes of the start counts as
  answered, and a leader without authority whose promotion held its driver for a heartbeat interval or more, such as a
  start that waited for followers that gave no answer, gets the grace again once the promotion returns. Authority is
  revoked before the leader retires its pipeline.
- **Repair.** A follower out of the write quorum that answers a heartbeat or an append from its log is queued for repair
  at once, at most once per election timeout, instead of waiting for the readiness retry.
- **Status.** While a driver runs, the replica status reports a node as leader only with authority, its majority contact
  from the quorum check (leader) or a recent leader contact (follower), and the highest term it saw.

Default timing: election timeout 1 s, jitter up to 1 s, heartbeat interval 100 ms, vote RPC timeout 250 ms. On
three-node starts with nodes about 0.4 s apart, a 500 ms timeout deposed a provisional owner and elected a needless second
term in half of the runs, and 1 s in none. A failover takes the election timeout plus the jitter to detect the stopped
leader, then one election round; a follower that is gone adds no probe timeout to the promotion of the new leader.

An entry node routes each single-key call to the leader of the key's group and reroutes at most once when the target
answers `stale-owner` or `stale-term`; a refusal of either kind is only given before anything was appended.

### Quorum reads (ReadIndex equivalent)

Quorum reads are the `QuorumReadsEnabled` server option, which validation requires together with automatic failover;
they apply only to elected leaders. For each read of an
elected group with the switch on:

1. Check that this node holds authority in the term of its running pipeline.
2. Take `read_index = commit_index`.
3. Confirm leadership with majority replies in that term to requests sent after the index was taken; a reply to an
   earlier request never counts, and a reply in a higher term fails the read with `stale-term`.
4. Wait until local `applied_index >= read_index`.
5. Only then return the value.

A refused read returns no value: the leader refusals above (`stale-term`, `stale-owner`, or no leader with authority),
`read_quorum_unconfirmed` when no majority confirmed the index within one election timeout, or `read_index_unapplied`
when the index was confirmed but memory did not apply it in time. Minority partitions and former leaders never return a
stale value as current. With the switch off, or for a statically led group, reads are local as before; there are no
lease reads.

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
- Network: loss, duplicate, reorder; **one-way single-node isolation** (one replica partitioned from the majority
  component) and reconnect/heal. Multi-node split topologies are out of M8-01 model bounds.
- Per sub-profile BFS cap: **`MaxStates = 50_000`** (symmetric reduction on). Hitting the cap without a safety violation
  is within these documented bounds: `summary.json` reports `fixedPointReached=false` and the CLI exits **4** (distinct
  from exit **0** = fixed point with no violation). It is residual risk inside the finite envelope, not a
  counterexample.
- `modelVersionHash` in `summary.json` is a **manual** semantics fingerprint in `ExploreRunner` (Assembly/MVID hashing
  is banned by RS0030). Bump the constant when transitions or invariants change.

Residual risk: larger RF, deeper logs, richer failure interleavings, or states beyond `MaxStates` are not explored here.

### Negative explorer modes (M8-01)

Required broken-rule fixtures (must produce expected counterexamples):

- `vote` — grant votes without up-to-date log checks.
- `current-term-commit` — commit old-term entries without a current-term commit.
- `read-index` — serve reads without majority confirm and/or before `applied_index >= read_index`.

Additional negative modes (local term inflation, commit-across-gap) are optional follow-ups and do not block M8-01.

### Mapping model → future product components

| Model concept                         | Future product home                                                                                 |
|---------------------------------------|-----------------------------------------------------------------------------------------------------|
| Term / vote persistence               | `Squirix.Server.Cluster.Replication` + durable group state via `Squirix.Server.Storage.Replication` |
| AppendEntries / vote / ReadIndex RPCs | Server-only protobuf under `Squirix.Server.Adapters.Grpc` (not shared `SquirixCache.proto`)         |
| Log matching / catch-up               | `Cluster.Replication` orchestration over `Storage.Replication` journal/snapshot ports               |
| Majority commit pipeline              | Durable replication pipeline (M8-07+)                                                               |
| ReadIndex wait on apply               | Leader read authority path (M8-11/M8-12)                                                            |
| Topology fingerprint / generation     | Placement + config (M8-02/M8-03)                                                                    |

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

New edges require an ADR/DAG update before merge. Enforcement lives outside this document (compile-time namespace policy
and architecture tests).

### MaxReplicaCount = 5 budget

RF=5 is the product maximum. Fan-out (vote / append / ReadIndex), per-group in-memory state, and metrics cardinality
must be sized for five peers on the established internal channels. Profiles RF=2..5 in the explorer confirm safety
machinery scales across the allowed RF set; operational budgets are validated by later placement/perf milestones.

### Replica-group snapshots

The storage layer publishes an atomic, CRC32C-validated `group.snapshot` containing the committed baseline and resolved
idempotency outcomes. Installing a snapshot is accepted only for the same group and compatible topology generation;
the committed prefix is replaced while the uncommitted suffix is retained. Compaction removes only the journal prefix
covered by the published snapshot and preserves the log header and installable snapshot.

Transport streaming and catch-up orchestration are intentionally deferred to M8-08. The snapshot storage contract is
therefore internal to `Storage.Replication` and does not add a transport dependency.

### Group log retention and compaction

Every node maintains each group log it serves: the log of the group it owns (leads) and its copy of every group it
follows.

- **Applied index.** The owner applies every committed entry to memory in log order and tracks the index it reached.
  A follower does the same for each group it follows: one apply loop per group applies the entries the leader marked
  committed, in log order, and records their idempotency outcomes. A maintenance pass every 10 seconds waits until the
  node cache journal holds every applied entry durably, then persists that index in each group log, which releases the
  applied payloads from memory. After a restart the owner applies the committed entries above the persisted index again,
  in log order, before it serves writes; a follower's apply loop first rebuilds the outcomes of the committed entries,
  then does the same.
- **Trigger.** The same pass compacts a group log once `group.log` reaches `ReplicaLogCompactionMb` (default 64 MiB) or
  holds `ReplicaLogCompactionEntries` entries (default 100 000); see
  [configuration](../configuration.md#persistence-host-defaults).
- **Follower compaction.** A follower compacts its copy of a group through the applied index it persisted, while the
  commit index may be higher. It publishes `group.snapshot` through that index and rewrites `group.log` to its header
  plus every entry above it, committed or not, so the apply loop and the leader's next append find them. The step waits
  until the apply loop has rebuilt the outcomes of the applied entries, and while nothing was applied past the last
  snapshot. A crash after the snapshot is published, or after `group.log` is rewritten, recovers the same state: the
  snapshot, the commit index, and the entries above the snapshot.
- **Gated owner step.** The owner's compaction runs as one step under the commit gate, so writes wait for it and
  continue after it.
  It requires no uncommitted tail, every committed entry applied, every idempotency outcome resolved, and every
  follower slot verified ready with a durable match index at the commit index. It then publishes `group.snapshot`
  through the commit index and rewrites `group.log` to its header.
- **Every follower must have caught up.** The leader keeps every entry a follower may still need: a follower that is
  down, or that missed an entry, blocks compaction of the owner's group log until it is back. A follower whose append
  failed is taken out of the write quorum; once it answers again, the owner sends it the entries it lacks from the
  group log through its sender, admits it, and compaction resumes. There is no snapshot catch-up: a follower that needs
  entries already compacted cannot be caught up and stays out of the quorum.
- **Snapshot size.** A snapshot carries the resolved idempotency outcomes of the covered entries. When they exceed the
  64 MiB snapshot limit, compaction stalls with `snapshot_too_large` until the outcomes age out of idempotency
  retention (one hour).

Each pass reports its outcome through `squirix_replication_log_compactions_total` and
`squirix_replication_log_compaction_skipped_total{reason}`, and the retained size through
`squirix_replication_log_bytes`, `squirix_replication_log_retained_entries`, `squirix_replication_snapshot_index`,
and the `replicaGroups` section of [readiness details](../diagnostics.md#readiness-details).

### Replicated mutation effects

The leader decides every replicated mutation once, at prepare time, from one read of the key and one reading of its
clock. The record carries the decision: the outcome (the applied flag and, for a remove, the removed entry), the
effect, and the pinned absolute expiration deadline.

| Kind             | Applied when                 | Effect when applied                   | Effect otherwise                     |
|------------------|------------------------------|---------------------------------------|--------------------------------------|
| Set              | always                       | write the entry                       | not possible                         |
| TryAdd           | the key is absent or expired | write the entry                       | nothing                              |
| Update           | the key is live              | write the entry with the new value    | nothing; delete the key when expired |
| Touch            | the key is live              | write the entry with the new deadline | nothing; delete the key when expired |
| RemoveExpiration | the live entry has one       | write the entry without a deadline    | nothing; delete the key when expired |
| Remove           | the key is live              | delete the key                        | delete the key                       |
| Expire           | never                        | not possible                          | delete the key                       |

A key is expired when its stored deadline is at or before the prepare time on the leader clock. An Update, Touch or
RemoveExpiration that finds the key expired folds the expiry into its record: the record reports nothing applied, carries
no entry, carries the passed deadline, and deletes the key. An Expire record has the same shape and is committed only to
remove an expired key.

An upserted entry is written exactly as decided: value, absolute deadline, version and tags. The deadline of Set and
TryAdd is the earlier of the entry's absolute expiration and its relative expiration measured from prepare time; the
deadline of Touch is prepare time plus the requested expiration.

Applying a record never reads a clock and never re-checks liveness or preconditions, so the apply of the same
committed prefix, at any time and on any node clock, yields identical entries. A restart re-applies the committed
entries above the durable applied index with the same result, and a recovered uncommitted tail reports the outcome its
record carries. The client outcome, the durable record outcome and the group idempotency outcome are always the same
bytes. The operation fingerprint is computed from the request, not from the decision, so a retry keeps its identity.

A retry replays its outcome across a restart of the owner too. The group snapshot carries the outcomes of the entries it
covers; for the committed entries above it, applied or not, the restarted owner reads the records back from the group
log and rebuilds each outcome they carry before it serves writes. The idempotency store keeps the outcomes with the
newest log indexes: the pinned uncommitted tail is never displaced, a rebuilt outcome takes a free place or the place of
the retained outcome with the oldest log index, and replaces an older outcome of the same identity, so an older snapshot
outcome gives way. A rebuilt outcome counts its retention from the leader time of its decision, which precedes the commit by at most the commit budget, so an
outcome already past its window is not rebuilt.

A record whose effect contradicts its outcome is never applied. The entry stays pending: the client of its own commit
gets `COMMIT_OUTCOME_UNKNOWN`, its idempotency outcome stays unresolved, later writes are refused with
`replica_apply_pending`, and a restart refuses to start the committer. Each refusal is logged at error level and counted
by `squirix_replication_inconsistent_records_total`.

Update, Touch and RemoveExpiration apply by writing the whole decided entry, so under memory pressure the write is admitted
like an insert: if the admission refuses it after the majority, the entry stays pending and later writes are refused with
`replica_apply_pending` until the pressure clears.
Pinned deadlines are rounded up to whole milliseconds, the precision of the cache journal, so a journal recovery and a log re-apply write
the same entry.

The canonical record encoding is version 4 and nodes refuse records of any other version, version 3 included: every node
of a replica group must run the same replica log codec version.

### Expiry

Only the leader decides that a key expired, on its own clock, and a key becomes absent for readers only through a
committed record. On activated hosts the cache, its snapshots, journal recovery and journal compaction keep an entry past
its deadline until a committed record removes it; RF=1 and foundation-only hosts keep expiring on the local clock.

- A read on the leader that finds its entry at or past the deadline commits an Expire record and applies it before it
  reports the miss. Concurrent reads of one key share one commit. A read whose Expire record cannot commit (no write
  majority, a pending apply, the commit budget, or an unknown outcome) is refused with gRPC `Unavailable` and the detail
  `replica_expiration_pending`; nothing was read, and a retry may succeed. It is never reported as `COMMIT_OUTCOME_UNKNOWN`.
- A background sweep on the leader expires, every 10 seconds, up to 1024 owned keys that no read touched; a pass stops
  at its first failure, logs it once, and the next pass retries. Keys of groups the node only follows are left to their
  leader.
- A follower never expires an entry on its own: it keeps the entry until it applies the Expire record or a conditional
  write that folded the expiry.
- Leader clocks can disagree. An entry expires when the leader that serves the key finds its deadline passed; a leader
  whose clock is ahead removes it that much earlier, one that is behind that much later, bounded by the clock skew
  between members. Once committed, the removal holds on every replica whatever its clock.
- After a failover the new leader starts from the committed log: a key the old leader expired stays absent, and a key it
  still read live is expired by the new leader on its own clock at the first read or sweep.
- An Expire record takes its identity from the expired entry (cache, key, version and deadline) under an operation scope
  no cache name can use, so no client operation shares it. It answers no client retry: it is pinned only while in flight,
  never counts against the group idempotency capacity or retention, and leaves no outcome.

## Consequences

- Product election code and local promotion stay off until this ADR merges and model evidence is green.
- RF>1 mutations replicate through the group log; quorum reads apply only to elected leaders and are enabled only
  together with automatic failover.
- RF=1 keeps single-owner behavior without elections.
- RF=2 cannot elect a replacement after losing one member (majority is two).
- RF≥3 elects a replacement leader only with the `AutomaticFailoverEnabled` and `QuorumReadsEnabled` options on.

## Alternatives considered

| Option                                 | Rejected because                                  |
|----------------------------------------|---------------------------------------------------|
| Local term bump + log shipping         | Dual leaders / lost commits under partitions      |
| Time-based leader lease                | Clock skew / false authority without quorum       |
| Embed model inside `Squirix.Server`    | Shared-bug risk; ambiguous isolation              |
| Unbounded model checking as merge gate | Non-terminating; residual risk must stay explicit |
