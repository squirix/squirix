# ADR: Replication Consensus Protocol

## Status

Accepted for M8-01. Product election runs only behind the opt-in `AutomaticFailoverEnabled` switch, which was gated on
this ADR and on the protocol-model explorer reporting no unexpected counterexamples within documented bounds.

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
ready members whose copy is complete, or incomplete with its term, vote and log intact (see below). Readiness never
changes the quorum denominator.

### Member state and the first start of a group

The group metadata file records, next to the term and the vote, the member state of the copy on that node and whether
the owner offered it a place in the first start. The file holds two record slots, each written whole. Only a record
written to both slots survives the loss of one: vote grants, every record that raises the term, and the member state
transitions (the fresh unseeded record, the offer, completion, and the marking of a copy incomplete and its
completion after a state transfer) are written to both; other records go to one slot at a time, so damage to the
newest slot rolls the file back to the previous record. A file written by an earlier build (the single-record layout)
is refused at open and left unchanged.

- **Unseeded.** A copy on a fresh disk opens unseeded, the fail-safe value. It refuses pre-votes, votes, and appends
  with the refusal `unseeded` and adopts no term, so it can never grant a vote
  that a lost disk would let it grant twice. Its node neither leads it in term one nor campaigns for it
  (`FailoverDenial.Unseeded`), and a static committer does not start over it: reads and writes of the group fail with
  `Unavailable`. A leader repairs it by a state transfer after the term ceiling (see State transfer).
- **Complete and incomplete.** A complete copy holds the data of the group and takes part as before; an incomplete copy
  is in the middle of receiving it and takes no entries. It refuses appends (probes included) with the refusal
  `incomplete`, which tells it apart from `unseeded`, adopts no term from a leader, and never applies log entries. Its
  term, vote and log are intact, so it grants pre-votes and votes like a complete copy; it never stands for election.
  A state transfer marks a copy incomplete first and completes it last: it publishes the group snapshot, replaces the
  log by the snapshot baseline, and then writes the complete record with the commit and applied index at the snapshot
  index. Recovery never changes the member state, so a crash anywhere before the complete record leaves the copy
  incomplete. A leader whose probe meets `incomplete` counts the member as one that answered and
  marks it as catching up, so it stays out of the write quorum; the entry catch-up session ends with the outcome
  `incomplete`, having sent no entries when its first request is the one refused. The bootstrap paths keep refusing such a copy. The offer answers `not-ready`, and the
  completion on a call of a member answers `incomplete`.
- **First start.** Only the owner of a group starts it. It asks every other member for its status; when all of them
  answer with a ready log, the same topology, and an unseeded copy, it offers each a place, and it completes its own
  copy once all of them accepted. An offered copy completes on the first call of a member of the group. If any
  member holds a complete copy, or does not answer, the owner offers nothing and retries: a member that returns on an
  empty disk into a running group is not offered a place and stays unseeded until a leader repairs it (#1126). All members
  of a group losing their disks at once cannot be told from a new cluster, and the group starts empty.

### Current-term commit before serving

After election, a leader appends and majority-commits a **current-term** entry (noop or real) before treating old-term
majority replication as a new commit that may be served. This preserves Raft’s “leader completeness” / current-term
commit rule.

### Deposed leader

A newer leader can reach a node while that node still leads in the old term. It rewrites the uncommitted tail of the
node's log before the node steps down. The old leader still commits nothing over the rewritten entries. Every follower
that voted for the newer leader lacked them and has moved past the old term, so it refuses the old leader's appends.
Without those followers there is no majority. Two term checks back this up locally (#1158):

- A follower that an entry catch-up session caught up is admitted only while the leader's log is still in the led term
  and the follower holds no entry of a later term. The session may have read entries a newer leader wrote.
- A copy refuses with `log-mismatch` any batch carrying an entry whose term is above the request term. A leader never
  sends one, so the copy's last log term never passes its current term.

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
  before the node follows it. A pre-vote or vote round asks every other member at once and ends as soon as its outcome is
  decided: a majority granted, a majority can no longer be reached, or a reply reported a higher term. Calls still
  unanswered are canceled, so a member that does not answer adds no vote timeout to a decided round; only a round that
  depends on that member waits for the vote RPC timeout.
- **Term one.** Term one belongs to the owner of a group: a node whose own group log never moved past term one and holds a
  complete copy leads it in term one once the first start completed it, without votes, and no vote is ever granted for
  term one. The first election is for term two.
- **Leader contact.** A member refuses a pre-vote with `leader-contact` while it heard from a live leader within one
  election timeout, so a node cut off from the leader alone keeps campaigning in vain instead of deposing it. The same
  rule decides whether a follower names its leader: one it has not heard from for an election timeout is no longer named.
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
  revoked before the leader retires its pipeline. A leader that got no authority within 30 seconds of its first promotion
  attempt steps down the same way, and its node does not campaign for the election timeout plus the largest jitter plus
  two vote timeouts after the retirement completed, so another member can take the group. While the same node keeps
  being elected without authority, the 30 seconds double, up to 240 seconds; getting authority restarts them. A state
  transfer that keeps failing at its last step holds that deadline only while its steps are accepted, so the leader still
  gives up within about two attempts. A leader with authority has a second, separate window: it runs while the applied
  index of the group is below the highest index a majority backs (the commit index, or the index the written majority
  covers that the current-term rule lets it commit) and has not moved since the window started. The driver reads both
  indexes without the commit gate, which a blocked apply may hold, because it is the only source of heartbeats. Any move of
  the applied index restarts the 30 seconds, catching up clears them, and they never double; entries without a majority
  do not count, as that is a lost quorum. The leader then revokes its authority and reports the timeout at once, and its retirement runs in the
  next step, because it waits for the commit gate that a hung apply may hold; the node abstains for the same time once its
  retirement completed. The retirement itself still waits until every committed entry is applied. A leader with authority
  has a third, separate window for the appends to its own group log. The driver reads the counters of the local appends
  (appended, failed, started, whether an append is in flight, and whether the log refused the last one as not ready)
  without the commit gate or the log gate. A failed append or a refusing log starts a window at the first failure after
  the last success. A successful append clears it, and the first observation after authority only records the counters.
  After 30 seconds the leader gives the group up as for the apply window, but only while a failure is still visible in
  that tick (a new failure since the previous tick, or a log that refused as not ready or latched its failure under an
  append or before one, which stays refused until an append succeeds). An append in flight counts separately: it must be the same append, by the started-appends counter,
  on two ticks in a row, and then stand still for the whole 30 seconds itself, so a healthy append caught mid-flush
  after an old failure is no stall. The leader also gives up only while the followers alone, without the leader, form a
  majority that answered from a ready log within the election timeout (an accepted append or a log mismatch; a
  follower whose log is not ready or incomplete answers but refuses every ballot, and does not count; a new leader
  gets no grace). Without that majority nobody could win an election, so the leader keeps the group and its reads, and
  gives up at the first later tick where the contact is back and the stall is still visible. A failure, a quiet period,
  and a failure give the group up at the second failure once 30 seconds passed without a success; this is accepted. A
  failed append of a leader is logged once per fault type until an append succeeds. A static leader, led without an
  election, has no such deadline and nobody can take its group.
- **Blocked apply.** The apply loop of a follower group marks the group blocked when a pass fails with a storage
  fault, a full node journal, or memory that refuses the write while committed entries are pending, or while the
  outcomes of the log are rebuilt (a leader start would fail on that too), and when a committed record it cannot apply
  stopped the group. A pass that ends with nothing left to apply clears the mark, also after a fault with nothing
  pending; so does a pass the loop skips because a committer leads the group, unless the group is stopped. A stopped
  group runs no pass until a state transfer resets its applied index; the next pass then clears the mark. A pass that
  finds the log not ready or the copy not complete, or that cannot read the log status, leaves the mark as it is. The
  driver reads the mark without any gate and, once the gate that checks for a complete copy, a ready log, and a
  current term admits the node, does not campaign while it is set: it reports the denial once per block, re-arms the
  election timeout, and checks again. The mark starts unassessed and turns assessed on the first decision of the loop
  for a complete copy with a ready log (a pass that blocks or clears, one that finds nothing to apply, or one it skips
  because a lease is held), and for a group that has no loop because this node leads it from the start. A copy that is
  not complete or not ready stays unassessed until it is, so the pass after it becomes complete decides before the
  node can campaign. Until then the driver does not campaign, and the owner does not take its own group at the
  provisional term, with a distinct debug-level denial, so a backlog the node cannot apply is never won first at
  start-up. It checks again after one heartbeat interval, not a full election timeout, so the node campaigns soon
  after the loop decided. A group whose log status cannot be read stays unassessed. The driver reads the mark again
  before it makes its own vote durable and before it promotes a won term, and drops the candidacy if the apply blocked
  meanwhile. A blocked node still grants votes and pre-votes, whose rules depend on the log, and accepts appends. When
  every member is blocked nobody campaigns, which is no worse than a blocked winner that fails its start and steps
  down. A hung apply is not a fault and is not marked; a leader is judged by the apply window above.
- **Repair.** A follower out of the write quorum that answers a heartbeat or an append from its log is queued for repair
  at once, at most once per election timeout, instead of waiting for the readiness retry.
- **Status.** While a driver runs, the replica status reports a node as leader only with authority, its majority contact
  from the quorum check (leader) or a recent leader contact (follower), and the highest term it saw.

Default timing: election timeout 1 s, jitter up to 1 s, heartbeat interval 100 ms, vote RPC timeout 250 ms. On
three-node starts with nodes about 0.4 s apart, a 500 ms timeout deposed a provisional owner and elected a needless second
term in half of the runs, and 1 s in none. A failover takes the election timeout plus the jitter to detect the stopped
leader, then one election round, which a follower that is gone does not prolong once the others decided it; nor does
such a follower add a probe timeout to the promotion of the new leader.

An entry node routes each single-key call to the leader of the key's group and reroutes at most once when the target
answers `stale-owner` or `stale-term`; a refusal of either kind is only given before anything was appended. A member
stops routing to a leader it has not heard from for one election timeout and waits for the next one; a follower without
a live leader answers `Unavailable` (no leader) instead of a stale-owner hint that names the silent leader. Sometimes
an entry node outside the group can reach no member, or the member it reached answers that it has no leader. This
happens, for example, to a ring owner cut off from the other members while they elect a new leader. The entry node
then spends the same single reroute on the next member in slot order. It sends the same request, and it learns the
leader that member names (#1104). A follower forwards to a new leader as soon as it accepts the leader-term entry,
usually before that entry commits. A forwarded call that reaches the new leader then waits for its authority, within
the leader wait and the call deadline, as a call entering there does. It is refused only if the authority does not
come, and at once when the node steps down or sees a higher term. Unless configured, the leader wait is the election
timeout plus the jitter, capped at the per-attempt timeout of a call less one second, so a node without a leader refuses
with `Unavailable` before the caller's attempt times out.

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
- Disk loss and repair (#1126), RF=3, two terms, one log entry, two messages in flight, crashes, no partitions, explored to
  its fixed point within `MaxStates = 300_000`: one member may lose its disk once (its term, vote, log and commit index
  are gone and its copy is unseeded: it refuses votes and appends, adopts no term and never campaigns). A leader repairs
  it in one step: it reads the current terms of members that, with itself and without the wiped one, form a majority;
  with a higher term among them it steps down, otherwise the member gets the leader's committed entries, its term and a
  vote for it. Messages the member sent before the loss stay in flight. RF=5 needs four messages in flight for one
  election and exceeds any affordable state budget before the repair without the ceiling shows, so it is outside the
  bounds; the ceiling rule for it rests on the quorum argument in #1126. The model counts an acknowledgement only from
  a member that stores the entry, so the residual risk of an acknowledgement sent before the loss and counted after it
  (see State transfer) is outside the model.
- Log rules (#1158, #1170), RF=3, four terms, two log entries, one message in flight, crashes, no partitions, explored to
  its fixed point within `MaxStates = 1_500_000` (about one million states). An election is one step: a candidate takes
  the votes of a majority that would grant them under the vote rules, or loses and keeps the term alone. A leader resends
  the entry after the last one a member confirmed, with the term of the entry before it; a member accepts an entry only
  when it stores that previous entry with that term, and keeps an entry it already stores with the same term together
  with what follows it. The applied index stays where it is (reads are explored by the other profiles). Four terms are
  what an entry of an earlier term needs to be lost to a later leader: written alone in term 1, overwritten on another
  member in term 2, carried to a majority in term 3, and missing on the leader of term 4. Resends and one-step elections
  are outside the other profiles, whose leaders send each entry once, when it is appended.
- Per sub-profile BFS cap: **`MaxStates = 50_000`** (symmetric reduction on). Hitting the cap without a safety violation
  is within these documented bounds: `summary.json` reports `fixedPointReached=false` and the CLI exits **4** (distinct
  from exit **0** = fixed point with no violation). It is residual risk inside the finite envelope, not a
  counterexample.
- `modelVersionHash` in `summary.json` is a **manual** semantics fingerprint in `ExploreRunner` (Assembly/MVID hashing
  is banned by RS0030). Bump the constant when transitions or invariants change.

Residual risk: larger RF, deeper logs, richer failure interleavings, or states beyond `MaxStates` are not explored here.

### Negative explorer modes (M8-01)

Required broken-rule fixtures (must produce expected counterexamples):

- `vote` — grant a second vote in a term already voted in (the up-to-date log check still applies).
- `current-term-commit` — commit an entry of an earlier term once a majority stores it, without an entry of the leader's
  term. The checker sees it from the state: the leader of a later term lacks an entry a majority holds as committed
  (`LeaderCompleteness`).
- `read-index` — serve reads without majority confirm and/or before `applied_index >= read_index`.
- `repair-ceiling` — repair a member that lost its disk without the term ceiling (#1126); it yields two leaders in one
  term: the member voted in a term above that of the stale leader repairing it, and votes again in that term.
- `log-order` — compare the length of the logs before the term of their last entries when granting a vote (#1170); a
  longer log of an older term elects a leader without a committed entry (`LeaderCompleteness`).

A higher term clears the vote has no negative mode: kept across the term change, the vote only makes the member refuse other
candidates, which can stall an election but never adds a leader or loses a committed entry. The full profile with the vote kept
found no violation in about two million states (#1170); the rule is pinned by product tests instead.

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

The storage layer publishes a CRC32C-validated snapshot, held in two in-place slot files (`group.snapshot.0` and
`group.snapshot.1`; see [snapshot format](../snapshot-format.md#replica-group-snapshots)), containing the committed
baseline and resolved idempotency outcomes, and carries no cache entries. Compaction removes only the journal prefix covered by the
published snapshot and rewrites the log into a new generation file. A snapshot reaches a follower only inside a
[state transfer](#state-transfer), which also carries the cache entries of the group; there is no request that installs
a snapshot alone, because it would move the applied index of a follower past data that follower does not hold.

### Group log retention and compaction

Every node maintains each group log it serves: the log of the group it owns (leads) and its copy of every group it
follows. A group log lives in two files, `group.log.0` and `group.log.1`; each starts with a header that holds a
generation number. A rewrite empties the inactive file, writes and flushes the frames, and only then writes and flushes
the header with the next generation, so a crash leaves the previous file or the new one complete. On open the node uses
the file whose header is valid and has the higher generation, and empties the other. A `group.log` left by an earlier
build is refused and left unchanged.

- **Applied index.** The owner applies every committed entry to memory in log order and tracks the index it reached.
  A follower does the same for each group it follows: one apply loop per group applies the entries the leader marked
  committed, in log order, and records their idempotency outcomes. A maintenance pass every 10 seconds waits until the
  node cache journal holds every applied entry durably, then persists that index in each group log, which releases the
  applied payloads from memory. After a restart the owner applies the committed entries above the persisted index again,
  in log order, before it serves writes; a follower's apply loop first rebuilds the outcomes of the committed entries,
  then does the same.
- **Trigger.** The same pass compacts a group log once the group log reaches `ReplicaLogCompactionMb` (default 64 MiB) or
  holds `ReplicaLogCompactionEntries` entries (default 100 000); see
  [configuration](../configuration.md#persistence-host-defaults).
- **Follower compaction.** A follower compacts its copy of a group through the applied index it persisted, while the
  commit index may be higher. It publishes the group snapshot through that index and rewrites the group log to its header
  plus every entry above it, committed or not, so the apply loop and the leader's next append find them. The step waits
  until the apply loop has rebuilt the outcomes of the applied entries, and while nothing was applied past the last
  snapshot. A crash after the snapshot is published, or after the group log is rewritten, recovers the same state: the
  snapshot, the commit index, and the entries above the snapshot.
- **Gated owner step.** The owner's compaction runs as one step under the commit gate, so writes wait for it and
  continue after it.
  It requires no uncommitted tail, every committed entry applied, every idempotency outcome resolved, and every
  follower slot verified ready with a durable match index at the commit index. It then publishes the group snapshot
  through the commit index and rewrites the group log to its header.
- **Every follower must have caught up, up to a limit.** The leader keeps every entry a follower may still need: a
  follower that is down, or that missed an entry, blocks compaction of the owner's group log until it is back. A
  follower whose append failed is taken out of the write quorum; once it answers again, the owner sends it the entries
  it lacks from the group log through its sender, admits it, and compaction resumes. Once the group log reaches
  `ReplicaLogRetentionMaxMb` (default 1 GiB), a follower slot that is not verified ready no longer holds the log: the
  owner compacts without it and reports `compacted_past_follower`; a pass whose outcome changes to it logs `4071`. A
  ready follower behind the commit index still holds it. A follower that needs entries already compacted is repaired by
  a [state transfer](#state-transfer). The leader does not compact its log while a transfer holds it, past the limit
  too.
- **Snapshot size.** A snapshot carries the resolved idempotency outcomes of the covered entries. When they exceed the
  64 MiB snapshot limit, compaction stalls with `snapshot_too_large` until the outcomes age out of idempotency
  retention (15 minutes) or are replaced by new writes. A table that stays full does not empty by aging: each new write
  takes the place of the oldest outcome once that outcome is older than one minute.

Each pass reports its outcome through `squirix_replication_log_compactions_total{outcome}` and
`squirix_replication_log_compaction_skipped_total{reason}`, and the retained size through
`squirix_replication_log_bytes`, `squirix_replication_log_retained_entries`, `squirix_replication_snapshot_index`,
and the `replicaGroups` section of [readiness details](../diagnostics.md#readiness-details).

### State transfer

A follower that cannot be caught up from the log of the leader is repaired by a state transfer: the leader sends the
snapshot of the group and every cache entry of the group at its applied index, and the follower then catches up from
the log above that index.

- **When it starts.** In the catch-up pass of the leader, under the lease of the follower's sender, when the entry
  session ends `compacted` (the leader no longer retains the entries the follower needs, which includes a complete copy
  with an empty log), `incomplete` (the copy was marked incomplete by a transfer that did not end), or `unseeded` (the
  copy lost its disk; see the term ceiling below). A ready follower that answers a heartbeat with `unseeded` or
  `incomplete` is taken out of the write quorum at once and queued for this pass. The passes run one follower after
  another, so the other followers of the group wait while one transfer runs.
- **Term ceiling (#1126).** Before a transfer to an `unseeded` copy the leader asks the other members, without the wiped
  one, for their durable current term. Answers count only from copies that kept their term (complete or incomplete);
  with the leader they must form a majority: the other member of three, three of the four others of five. A term above
  the leader's among them is observed, which steps the leader down, and nothing is sent; too few answers send nothing
  either (`ceiling_unavailable`). Otherwise the begin carries `ceiling_checked`, without which the copy refuses it as
  `unseeded`. Why it is enough: a vote in a term was asked by a candidate that holds the term durably, and a candidate
  that won the term is held at it by a majority; the answers and the leader form a majority as well, so they meet
  every majority that elected someone with the lost vote. The protocol model explores it (`repair-ceiling`).
- **Capture.** Under the commit gate the leader checks that it leads the term of the follower's pipeline, that no
  committed entry waits to be applied and that every idempotency outcome is known, makes the applied index durable,
  captures the group snapshot at that index and pins its log there. It then leaves the gate and streams outside it, so
  writes go on; what they append above the index reaches the follower through the entry session that follows.
- **Authority.** The leader does not ask for authority to start a transfer: a leader whose only other live member is
  the follower under repair cannot get authority without it. This is safe because everything captured is committed
  data, the applied index never passes the commit index, and the transfer changes only the copy of the follower,
  which counts toward no quorum until the transfer ended and the entry session verified it.
- **Pin.** While a transfer holds the pin, the compaction of the leader's log is skipped (`follower_not_ready`), so the
  entries above the index and the term at it stay. The pin is released when the follower's pass ends, whatever way,
  and lives in memory only: a leader that restarts loses the transfer.
- **Contact.** Every accepted reply of the follower is a contact of the leader in its term and keeps the authority
  deadline of the group; a reply of a higher term is observed like any other. Neither counts toward a commit or a read
  index confirmation.
- **Order on the follower.** The begin checks membership, the topology, the term and the declared size, waits for the
  local journal recovery, and takes the apply driver of the group; it is refused while this node leads the group or
  while another transfer holds the group (`transfer-busy`). The snapshot image arrives next and writes nothing durable.
  The first wipe step then publishes the incomplete record in both metadata slots, and only after that removes the keys
  of the group in bounded steps. The entries are written through the local cache chain, so the journal and the memory
  admission of the node apply. The end compares the count and the CRC32C of the entries, awaits the journal
  durability barrier, publishes the group snapshot, replaces the log by the snapshot baseline and writes the complete
  record with the commit and the applied index at the transferred index.
- **Incomplete copy.** It refuses appends, probes included, with `incomplete`, never applies entries and never stands
  for election; its term, vote and log are intact, so it still answers pre-votes and votes. Recovery never changes
  the state, so a crash anywhere before the complete record leaves the copy incomplete, and the next pass of the
  leader starts a new transfer.
- **Unseeded copy.** A transfer with `ceiling_checked` into it writes nothing at the begin and the wipe: the copy stays
  unseeded, votes for nobody and adopts no term until the end, whose complete record also carries the leader's term and
  a vote for the leader, in both metadata slots. A transfer cut short, or a crash before the complete record, leaves the
  copy unseeded, and the next pass starts over.
- **Residual risk.** A node that acknowledged a write and then lost its disk can still be counted for that write
  for a short time, until the leader meets its refusal and takes it out of the quorum.

Each attempt is logged when it starts and ends on the leader and on the follower, and counted by
`squirix_replication_state_transfers_total`; see [diagnostics](../diagnostics.md#state-transfer-metrics-and-events).

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

A replica group keeps up to 65,536 idempotency outcomes. An outcome is always replayable for one minute, and for up to
15 minutes while the table has room. When the table is full, a new write takes the place of the oldest outcome that is
older than one minute; the write is refused for capacity only when every outcome is younger.
Unresolved operations are never evicted. Ages are counted on each node's own clock, and on a follower from the leader's
decision time, so clock skew between nodes shortens the minute on a node whose clock runs ahead. Outcomes are evicted by
age and the eviction is not replicated, so replicas can differ on outcomes older than one minute. Each node counts the
outcomes it gave up to make room, apart from the ones that expired, in `squirix_replication_idempotency_capacity_evictions_total`
(labels `node`, `group`); a group whose count keeps growing runs at the one-minute minimum instead of the full retention.

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

A committed entry whose apply fails for another reason, such as a full node journal or a storage error, stays pending too,
and the committer retries it before a write, on every readiness pass, and when the leadership retires. The warning for the
failed apply is logged once while the fault type and the applied index stay the same, and again after an apply that
completed. A leader with authority that makes no apply progress for 30 seconds gives the group up, as described under
heartbeats and step-down above; so does one whose own log append keeps failing, which leaves no committed entry unapplied.

A group log append that fails after its frame was handed to the storage, in the frame write, the flush, or the
commit-index write behind the frames, is unconfirmed: the frame may be on disk, and a restart recovers it as an
uncommitted tail that a majority can commit. The log latches failed at once, for a leader and a follower alike, and
refuses every later append as not ready until the node restarts, because writing behind a failed flush is unsafe. The
latch is logged once at error level (event 4077), and the log reports readiness `failed` in the status. The client of the
write gets `COMMIT_OUTCOME_UNKNOWN`, its idempotency outcome stays unresolved, and a retry with the same operation
identifier gets `COMMIT_OUTCOME_UNKNOWN` again without a second execution. After a restart the retry is answered from the
committed entry, or runs as a new write when the frame did not reach the disk.

A follower with a latched log does not vote and does not accept appends, so the group loses that member until the node
restarts. An electable leader with a latched log gives the group up through the append window above. When the failed
append is the leader-term entry of a new leadership, the promotion is not authorized and the group moves to another node
through the election timeout, not through the append window. A leader without an election stays without writes until the
node restarts.

Update, Touch and RemoveExpiration apply by writing the whole decided entry. Memory admission runs before the write is
proposed: an apply of a committed entry never passes admission, never fails and never waits because of memory, on the leader
and on every follower alike.
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
  leader. The sweep reads the due keys from a deadline index of the local cache. It does not copy the stored keys, so a pass
  costs in proportion to the expired keys, not to all keys.
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

- Product election and promotion are active only with the `AutomaticFailoverEnabled` option on; they are off by default.
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
